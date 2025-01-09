#include "postgres.h"
#include "fmgr.h"

#include "miscadmin.h"
#include "postmaster/bgworker.h"

#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

#include "utils/guc.h"
#include "pgstat.h"
#include "storage/spin.h"
#include "lib/ilist.h"
#include <signal.h>

#include <jni.h>
#include "moonshot_worker.h"
#include "moonshot_jvm.h"
#include "moonshot_spi.h"

#include <dlfcn.h>
#include "utils/snapmgr.h"
#include "math.h"

bool got_signal = false;
int worker_id;

static worker_data_head *worker_head = NULL;

#ifndef PGXC
/* hook */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static void ms_shmem_request(void);

/*
 * Module load callback
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
			return;

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = ms_shmem_request;
}

/* Reserve shared memory */
static void
ms_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(mul_size(MAX_USERS,sizeof(worker_data_head)));
	RequestNamedLWLockTranche("ms_background_workers", 1);
}
#endif


worker_data_head*
launch_dynamic_workers(int32 n_workers, bool needSPI, bool globalWorker)
{
	Oid			roleid = GetUserId();
	Oid			dbid = MyDatabaseId;
	
    char buf[BGW_MAXLEN];
	if(!globalWorker) {
		snprintf(buf, BGW_MAXLEN, "MW_%d_%d", roleid, dbid); 
	} else {
		snprintf(buf, BGW_MAXLEN, "MW_global");
	}

	/* initialize worker data header */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    bool found = false;
    worker_head = (worker_data_head*) ShmemInitStruct(buf,
								   sizeof(worker_data_head),
								   &found);
	LWLockRelease(AddinShmemInitLock);
	
	if (found && worker_head->n_workers > 0) {
    	return worker_head;
    }

	SpinLockAcquire(&worker_head->lock);

	/* initialize worker data header */
	memset(worker_head, 0, sizeof(worker_data_head));
    dlist_init(&worker_head->exec_list);
    dlist_init(&worker_head->free_list);
	dlist_init(&worker_head->return_list);
		
	// Init free list
	for(int i = 0; i < MAX_QUEUE_LENGTH; i++) {
		worker_head->list_data[i].taskid = i;
		dlist_push_tail(&worker_head->free_list,&worker_head->list_data[i].node);
	}

	for(int n = 0; n < fmin(n_workers,MAX_WORKERS); n++) {
		BackgroundWorker worker;
		BackgroundWorkerHandle *handle;
		BgwHandleStatus status;
		pid_t		pid;
		
		memset(&worker, 0, sizeof(worker));
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
		worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		worker.bgw_restart_time = BGW_NEVER_RESTART; // Time in s to restart if crash. Use BGW_NEVER_RESTART for no restart;
		
		//char* WORKER_LIB = GetConfigOption("ms.lib",true,true);
		char* WORKER_LIB = "$libdir/moonshot.so";

		sprintf(worker.bgw_library_name, WORKER_LIB);
		sprintf(worker.bgw_function_name, "moonshot_worker_main");
		
		snprintf(worker.bgw_name, BGW_MAXLEN, "%s",buf);
		//snprintf(worker.bgw_name, BGW_MAXLEN, "MW_%s_%d",buf,(n+1));
			
		worker.bgw_main_arg = Int32GetDatum(n);
		worker.bgw_notify_pid = MyProcPid;

		memcpy(&worker.bgw_extra[0],&roleid,4);
		memcpy(&worker.bgw_extra[4],&dbid,4);
		if(!needSPI || globalWorker) {
			worker.bgw_extra[9] = 0;
		} else {
			worker.bgw_extra[9] = 1;
		}

		if (!RegisterDynamicBackgroundWorker(&worker, &handle))
			continue;

		status = WaitForBackgroundWorkerStartup(handle, &pid);

		if (status == BGWH_STOPPED)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					errmsg("could not start background process"),
					errhint("More details may be available in the server log.")));
		if (status == BGWH_POSTMASTER_DIED)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					errmsg("cannot start background processes without postmaster"),
					errhint("Kill all remaining database processes and restart the database.")));
		
		Assert(status == BGWH_STARTED);
		
		elog(WARNING,"Moonshot worker %d of %d initialized with pid %d (from %d)",(n+1),(int) fmin(n_workers,MAX_WORKERS),pid,MyProcPid);
	}

	worker_head->n_workers = fmin(n_workers,MAX_WORKERS);

    SpinLockRelease(&worker_head->lock);
	
	return worker_head;
}


/*
	Build Java args from serialized datums
*/
int 
argDeSerializer(jvalue* args, short* argprim, worker_exec_entry* entry) {
	char* pos = entry->data;
	for(int i = 0; i < entry->n_args; i++) {
		
		char* T = pos;
		pos += strlen(T)+1;
		//elog(WARNING,"%d. T: %s, pos: %d",i,T,(int) pos);

		bool isnull;
		
		if(T[0] == '[') {
			ArrayType* v;
			Datum arg;
			argprim[i] = 1;
			switch(T[1]) {
				// 1D arrays
				case 'B':
					arg = datumDeSerialize(&pos, &isnull);
					bytea* bytes  = DatumGetByteaP( arg );
					jsize  nElems = VARSIZE(bytes) - sizeof(int32);
					jbyteArray byteArray  =(*jenv)->NewByteArray(jenv,nElems);
					(*jenv)->SetByteArrayRegion(jenv, byteArray, 0, nElems, (jbyte*)VARDATA(bytes));
					args[i].l = byteArray;
					break;
				case 'I':
					arg = datumDeSerialize(&pos, &isnull);
					v = DatumGetArrayTypeP(arg);
					if(!ARR_HASNULL(v)) {
						jsize      nElems = (jsize)ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));
						jintArray intArray = (*jenv)->NewIntArray(jenv,nElems);
						(*jenv)->SetIntArrayRegion(jenv,intArray, 0, nElems, (jint*)ARR_DATA_PTR(v));
						args[i].l = intArray;
					} else {
						strcpy(entry->data,"Array with null not supported yet");
						return -1;
					}
					break;
				case 'J':
					arg = datumDeSerialize(&pos, &isnull);
					v = DatumGetArrayTypeP(arg);
					if(!ARR_HASNULL(v)) {
						jsize      nElems = (jsize)ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));
						jlongArray longArray = (*jenv)->NewLongArray(jenv,nElems);
						(*jenv)->SetLongArrayRegion(jenv,longArray, 0, nElems, (jlong*)ARR_DATA_PTR(v));
						args[i].l = longArray;
					} else {
						strcpy(entry->data,"Array with null not supported yet");
						return -1;
					}
					break;
				case 'D':
					arg = datumDeSerialize(&pos, &isnull);
					v = DatumGetArrayTypeP(arg);
					if(!ARR_HASNULL(v)) {
						jsize      nElems = (jsize)ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));
						jdoubleArray doubleArray = (*jenv)->NewDoubleArray(jenv,nElems);
						(*jenv)->SetDoubleArrayRegion(jenv, doubleArray, 0, nElems, (jdouble *)ARR_DATA_PTR(v));
						args[i].l = doubleArray;
					} else {
						strcpy(entry->data,"Array with null not supported yet");
						return -1;
					}
					break;
				case 'F':
					arg = datumDeSerialize(&pos, &isnull);
					v = DatumGetArrayTypeP(arg);
					if(!ARR_HASNULL(v)) {
						jsize      nElems = (jsize)ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));
						jfloatArray floatArray = (*jenv)->NewFloatArray(jenv,nElems);
						(*jenv)->SetFloatArrayRegion(jenv, floatArray, 0, nElems, (jfloat *)ARR_DATA_PTR(v));
						args[i].l = floatArray;
					} else {
						strcpy(entry->data,"Array with null not supported yet");
						return -1;
					}
					break;
				case '[':
					arg = datumDeSerialize(&pos, &isnull);
					int nc;
					// 2D arrays;
					switch(T[2]) {
						case 'I':
							v = DatumGetArrayTypeP(arg);
							nc = 0;
							if(!ARR_HASNULL(v)) {
								jclass cls = (*jenv)->FindClass(jenv,"[I");
								jobjectArray objectArray = (*jenv)->NewObjectArray(jenv, ARR_DIMS(v)[0],cls,0);
								
								for (int idx = 0; idx < ARR_DIMS(v)[0]; ++idx) {
									// Create inner
									jintArray innerArray = (*jenv)->NewIntArray(jenv,ARR_DIMS(v)[1]);
									(*jenv)->SetIntArrayRegion(jenv, innerArray, 0, ARR_DIMS(v)[1], (jint *) (ARR_DATA_PTR(v) + nc*sizeof(int) ));
									nc += ARR_DIMS(v)[1];
									(*jenv)->SetObjectArrayElement(jenv, objectArray, idx, innerArray);
									(*jenv)->DeleteLocalRef(jenv,innerArray);
								}
								
								args[i].l = objectArray;
								break;
							}	
						case 'D':
							v = DatumGetArrayTypeP(arg);
							nc = 0;
							if(!ARR_HASNULL(v)) {
								jclass cls = (*jenv)->FindClass(jenv,"[D");
								jobjectArray objectArray = (*jenv)->NewObjectArray(jenv, ARR_DIMS(v)[0],cls,0);
								
								for (int idx = 0; idx < ARR_DIMS(v)[0]; ++idx) {
									// Create inner
									jdoubleArray innerArray = (*jenv)->NewDoubleArray(jenv,ARR_DIMS(v)[1]);
									(*jenv)->SetDoubleArrayRegion(jenv, innerArray, 0, ARR_DIMS(v)[1], (jdouble *) (ARR_DATA_PTR(v) + nc*sizeof(double) ));
									nc += ARR_DIMS(v)[1];
									(*jenv)->SetObjectArrayElement(jenv, objectArray, idx, innerArray);
									(*jenv)->DeleteLocalRef(jenv,innerArray);
								}
								
								args[i].l = objectArray;
								break;
							}	
						default:
							strcpy(entry->data,"Could not deserialize java function argument (unknown 2d array type)");
							return -1;
					}
					break;
				case 'L':
					// Object array
					if(strcmp(T, "[Ljava/lang/String;") == 0) {
						// String array
						strcpy(entry->data,"Could not deserialize java function argument (string arrays not supported yet)");
						return -1;
					} else {
						// Composite type
					
						// Read array length
						int N = ((int*)(pos))[0];
						pos += 4;

						// Map composite type
						jclass cls = (*jenv)->FindClass(jenv, T+1);
						if(cls == NULL) {
							strcpy(entry->data,"Could not deserialize java function argument (unknown class for composite type)");
							return -1;
						}
						// Get field info
						jmethodID getFields = (*jenv)->GetMethodID(jenv, (*jenv)->GetObjectClass(jenv,cls), "getFields", "()[Ljava/lang/reflect/Field;");
						jobjectArray fieldsList = (jobjectArray)  (*jenv)->CallObjectMethod(jenv, cls, getFields);    
						jsize len =  (*jenv)->GetArrayLength(jenv,fieldsList);
						
						if( len == 0 ) {
							strcpy(entry->data,"Could not deserialize java function argument (composite type has zero elements)");
							return -1;		
						} else {
							// Create object array
							jobjectArray objectArray = (*jenv)->NewObjectArray(jenv, N, cls, 0);
								
							// Infer fields
							char* sig[len];
							char* fieldname[len];
							char* typename[len];
							jstring jstr[len];
							jstring jstr2[len];
							jfieldID fid[len];

							// Loop over class fields to prepare                             
							for(int j = 0; j < len; j++) {
								
								// Detect field
								jobject field = (*jenv)->GetObjectArrayElement(jenv, fieldsList, j);
								jclass fieldClass = (*jenv)->GetObjectClass(jenv, field);
									
								// Obtain signature
								jmethodID m =  (*jenv)->GetMethodID(jenv, fieldClass, "getName", "()Ljava/lang/String;");   
								jstr[j] = (jstring)(*jenv)->CallObjectMethod(jenv, field, m);
							
								fieldname[j] =  (*jenv)->GetStringUTFChars(jenv, jstr[j], false);
							
								m =  (*jenv)->GetMethodID(jenv, fieldClass, "getType", "()Ljava/lang/Class;");   
								jobject value = (*jenv)->CallObjectMethod(jenv, field, m);
								jclass  valueClass = (*jenv)->GetObjectClass(jenv, value);

								m =  (*jenv)->GetMethodID(jenv, valueClass, "getName", "()Ljava/lang/String;");   
								jstr2[j] = (jstring)(*jenv)->CallObjectMethod(jenv, value, m);
								typename[j] =  (*jenv)->GetStringUTFChars(jenv, jstr2[j], false);

								char error_msg[128];
								sig[j] = convert_name_to_JNI_signature(typename[j], error_msg);
								
								if(sig[j] == NULL) {
									sprintf(entry->data,"%s",error_msg);   
									return -1;
								}
							
								fid[j] = (*jenv)->GetFieldID(jenv, cls, fieldname[j], sig[j]);
							}

							// Loop over elements
							for(int n = 0; n < N; n++) {
						
								// Construct new instance
								jmethodID constructor = (*jenv)->GetMethodID(jenv, cls, "<init>", "()V");
								jobject cobj = (*jenv)->NewObject(jenv, cls, constructor);

								// Loop over class fields                            
								for(int j = 0; j < len; j++) {
									
									// Read element
									arg = datumDeSerialize(&pos, &isnull);
									
									if(isnull) {
										strcpy(entry->data,"Could not deserialize java function argument (no corresponding attribute found)");
										return -1;
									}
									
									int res = set_jobject_field_from_datum(&cobj, &fid[j], &arg, sig[j]); 
									
									if(res != 0) {                                
										strcpy(entry->data,"Could not deserialize java function argument (unknown error in composite type)");
										return -1;
									}
								}
								
								// Set element
								(*jenv)->SetObjectArrayElement(jenv, objectArray, n, cobj);
							}
							
							// Cleanup
							for(int j = 0; j < len; j++) {
								(*jenv)->ReleaseStringUTFChars(jenv, jstr2[j], typename[j]);
								(*jenv)->ReleaseStringUTFChars(jenv, jstr[j], fieldname[j]);
							}

							args[i].l = objectArray;
						}			
						break;
					}
					
				default:
					strcpy(entry->data,"Could not deserialize java function argument (unknown array type)");
					return -1;
			}
		}
		else if(T[0] == 'L') {
			Datum arg = datumDeSerialize(&pos, &isnull);

			// Objects
			if(strcmp(T, "Ljava/lang/String;") == 0) {
				text* txt = DatumGetTextP(arg);
				int len = VARSIZE_ANY_EXHDR(txt)+1;
				char t[len];
				text_to_cstring_buffer(txt, &t, len);
				args[i].l = (*jenv)->NewStringUTF(jenv, t);
				argprim[i] = 2;

			} else {
				argprim[i] = 1;
	
				// Map composite type
				jclass cls = (*jenv)->FindClass(jenv, T);
				if(cls == NULL) {
                	strcpy(entry->data,"Could not deserialize java function argument (unknown class for composite type)");
					return -1;
                }
				// Get field info
				jmethodID getFields = (*jenv)->GetMethodID(jenv, (*jenv)->GetObjectClass(jenv,cls), "getFields", "()[Ljava/lang/reflect/Field;");
				jobjectArray fieldsList = (jobjectArray)  (*jenv)->CallObjectMethod(jenv, cls, getFields);    
				jsize len =  (*jenv)->GetArrayLength(jenv,fieldsList);
				
				if(len == 0) {
					strcpy(entry->data,"Could not deserialize java function argument (empty class for composite type)");
					return -1;
				} else {
					// Construct new instance
					jmethodID constructor = (*jenv)->GetMethodID(jenv, cls, "<init>", "()V");
					jobject cobj = (*jenv)->NewObject(jenv, cls, constructor);
							
					// Loop over class fields                            
					for(int j = 0; j < len; j++) {
						
						// ToDo: Move to helper function ?

						// Detect field
						jobject field = (*jenv)->GetObjectArrayElement(jenv, fieldsList, j);
						jclass fieldClass = (*jenv)->GetObjectClass(jenv, field);
							
						// Obtain signature
						jmethodID m =  (*jenv)->GetMethodID(jenv, fieldClass, "getName", "()Ljava/lang/String;");   
						jstring jstr = (jstring)(*jenv)->CallObjectMethod(jenv, field, m);
					
						char* fieldname =  (*jenv)->GetStringUTFChars(jenv, jstr, false);
					
						m =  (*jenv)->GetMethodID(jenv, fieldClass, "getType", "()Ljava/lang/Class;");   
						jobject value = (*jenv)->CallObjectMethod(jenv, field, m);
						jclass  valueClass = (*jenv)->GetObjectClass(jenv, value);

						m =  (*jenv)->GetMethodID(jenv, valueClass, "getName", "()Ljava/lang/String;");   
						jstring jstr2 = (jstring)(*jenv)->CallObjectMethod(jenv, value, m);
						char* typename =  (*jenv)->GetStringUTFChars(jenv, jstr2, false);

						char* sig = convert_name_to_JNI_signature(typename, entry->data);
						
						if(sig == NULL) {
							return -1;  
						}
						
						jfieldID fid = (*jenv)->GetFieldID(jenv, cls, fieldname, sig);
						
						Datum attr;
						if(j > 0) {
							attr = datumDeSerialize(&pos, &isnull);
						} else {
							attr = arg;
						}
					
						if(isnull) {
							strcpy(entry->data,"Could not deserialize java function argument (no corresponding attribute found)");
							return -1;
						}
						
						int res = set_jobject_field_from_datum(&cobj, &fid, &attr, sig); 
						
						(*jenv)->ReleaseStringUTFChars(jenv, jstr2, typename);
						(*jenv)->ReleaseStringUTFChars(jenv, jstr, fieldname);

						if(res != 0) {                                
							strcpy(entry->data,"Could not deserialize java function argument (unknown error in composite type)");
							return -1;
						}
                        
					}
					args[i].l = cobj;
				}
				
				//strcpy(entry->data,"HALDE");
				//return -1;
			}
		} 
		else {
			Datum arg = datumDeSerialize(&pos, &isnull);

			// Natives
			switch(T[0]) {
				case 'S':
					args[i].s = (jshort) DatumGetInt16(arg);
					break;
				case 'I':
					args[i].i = (jint) DatumGetInt32(arg);
					break;
				case 'J':
					args[i].j = (jlong) DatumGetInt64(arg);
					break;
				case 'Z':
					args[i].z = (jboolean) DatumGetBool(arg);
					break;
				case 'F':
					args[i].f = (jfloat) DatumGetFloat4(arg);
					break;
				case 'D':
					args[i].d = (jdouble) DatumGetFloat8(arg);	
					break;	

				default:
					strcpy(entry->data,"Could not deserialize java function argument (unknown native type)");
					return -1;	
			}
		}
	}		

	return 0;
}


void
sigTermHandler(SIGNAL_ARGS)
{
    elog(WARNING,"Moonshot worker %d received sigterm",worker_id);
	got_signal = true;
	SetLatch(MyLatch);
}

void
moonshot_worker_main(Datum main_arg)
{
	int			workerid = DatumGetInt32(main_arg);

	//StringInfoData buf;
	Oid			roleoid;
	Oid			dboid;
	bits32		flags = 0;
 	memcpy(&roleoid,&MyBgworkerEntry->bgw_extra[0],4);
	memcpy(&dboid,&MyBgworkerEntry->bgw_extra[4],4);
	memcpy(&flags,&MyBgworkerEntry->bgw_flags,4);

	activeSPI = MyBgworkerEntry->bgw_extra[9];

	char buf[BGW_MAXLEN];
	snprintf(buf, BGW_MAXLEN, "%s_%d", MyBgworkerEntry->bgw_name, worker_id); 
	//snprintf(buf, BGW_MAXLEN, "%s", MyBgworkerEntry->bgw_name); 

	// Attach to shared memory
	bool found;

	worker_head = (worker_data_head*) ShmemInitStruct(MyBgworkerEntry->bgw_name,
								   sizeof(worker_data_head),
								   &found);

	if(!found) {
		elog(ERROR,"Shared memory for background worker has not been initialized (%s)",buf);
		/*
		// initialize worker data header 
		memset(worker_head, 0, sizeof(worker_data_head));
		dlist_init(&worker_head->exec_list);
		dlist_init(&worker_head->free_list);
		dlist_init(&worker_head->return_list);

		// Init free list
		for(int i = 0; i < MAX_QUEUE_LENGTH; i++) {
			worker_head->list_data[i].taskid = i;
			dlist_push_tail(&worker_head->free_list,&worker_head->list_data[i].node);
		}
		worker_head->n_workers = 0;
		*/
	}
	
	SpinLockAcquire(&worker_head->lock); 
	worker_head->latch[workerid] = MyLatch;
	// Set pid (due to potential restart)
	worker_head->pid[workerid] = MyProcPid;
	
	elog(WARNING,"[DEBUG]: BG worker %s init shared memory found: %d | pid: %d, total: %d | SPI: %d",buf,(int) found,worker_head->pid[workerid],worker_head->n_workers,activeSPI);

	SpinLockRelease(&worker_head->lock);
		
	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGTERM, sigTermHandler);
	
	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();
	
	if(activeSPI) {
		/* Connect to our database */
#ifdef PGXC
		BackgroundWorkerInitializeConnectionByOid(dboid, roleoid);
#else
		BackgroundWorkerInitializeConnectionByOid(dboid, roleoid, 0);
#endif
	}

    // Start JVM
	char error_msg[128];
    int jc = startJVM(error_msg);
   	if(jc < 0) {
		elog(ERROR,"%s",error_msg);
	}

	elog(LOG, "%s initialized",buf);
		
	/*
	 * Main loop: do this until SIGTERM is received and processed by
	 * ProcessInterrupts.
	 */
	while(!got_signal)
	{
		int			ret;

        SpinLockAcquire(&worker_head->lock);
       
        if (dlist_is_empty(&worker_head->exec_list))
        {
            SpinLockRelease(&worker_head->lock);
		    int ev = WaitLatch(MyLatch,
                            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                            10 * 1000L,
                            PG_WAIT_EXTENSION);
            ResetLatch(MyLatch);
		    if (ev & WL_POSTMASTER_DEATH)
                elog(FATAL, "unexpected postmaster dead");
            
            CHECK_FOR_INTERRUPTS();
            continue;
        }
        
        /*
            Exec task
        */       
        dlist_node* dnode = dlist_pop_head_node(&worker_head->exec_list);
        worker_exec_entry* entry = dlist_container(worker_exec_entry, node, dnode);

        // Run function and return data
        //elog(WARNING,"BG worker taskid: %d",entry->taskid);
		
		SpinLockRelease(&worker_head->lock);

		if(activeSPI) {
			/* 
				Connect SPI
			*/	 
			SetCurrentStatementStartTimestamp();
			StartTransactionCommand();
			connect_SPI();
			PushActiveSnapshot(GetTransactionSnapshot());
		}
		/*
			Call JAVA
		*/
		// ToDo: Move to switch function

		/*
		char* pos = entry->data;
		for(int i = 0; i < entry->n_args; i++) {

			char* T = pos;
		 	pos += strlen(T)+1;
			//elog(WARNING,"%d. T: %s, pos: %d",i,T,(int) pos);

			bool isnull;
			Datum arg = datumDeSerialize(&pos, &isnull);

			jvalue val;
			// Natives
			if(strcmp(T, "I") == 0) {
				val.i = (jint) DatumGetInt32(arg);
			} else if(strcmp(T, "F") == 0) {
				val.f = (jfloat) DatumGetFloat4(arg);
			} else if(strcmp(T, "D") == 0) {
				val.d = (jdouble) DatumGetFloat8(arg);
			} else if(strcmp(T, "Z") == 0) {
				val.z = (jboolean) DatumGetBool(arg);
			} else if(strcmp(T, "J") == 0) {
				val.j = (jlong) DatumGetInt64(arg);
			}

			// Arrays
			else if(strcmp(T, "[J") == 0) {
				ArrayType* v = DatumGetArrayTypeP(arg);
				if(!ARR_HASNULL(v)) {
					jsize      nElems = (jsize)ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));
					jlongArray longArray = (*jenv)->NewLongArray(jenv,nElems);
					(*jenv)->SetLongArrayRegion(jenv,longArray, 0, nElems, (jlong*)ARR_DATA_PTR(v));
					val.l = longArray;
				} else {
					// Copy element by element 
					//...
				}
			}
			else if(strcmp(T, "[F") == 0) {
				ArrayType* v = DatumGetArrayTypeP(arg);
				if(!ARR_HASNULL(v)) {
					jsize      nElems = (jsize)ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));
					jfloatArray floatArray = (*jenv)->NewFloatArray(jenv,nElems);
					(*jenv)->SetFloatArrayRegion(jenv,floatArray, 0, nElems, (jfloat *)ARR_DATA_PTR(v));
					val.l = floatArray;
				} else {
					// Copy element by element 
					//...

				}
			}
			else if(strcmp(T, "[[F") == 0) {
				// ToDo 
			}
			else if(strcmp(T, "[D") == 0) {
				ArrayType* v = DatumGetArrayTypeP(arg);
				if(!ARR_HASNULL(v)) {
					jsize      nElems = (jsize)ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));
					jdoubleArray doubleArray = (*jenv)->NewDoubleArray(jenv,nElems);
					(*jenv)->SetDoubleArrayRegion(jenv,doubleArray, 0, nElems, (jdouble *)ARR_DATA_PTR(v));
					val.l = doubleArray;
				} else {
					// Copy element by element 
					//...
				}
			}
			else if(strcmp(T, "[[D") == 0) {
				ArrayType* v = DatumGetArrayTypeP(arg);
				int nc = 0;
				if(!ARR_HASNULL(v)) {
					jclass cls = (*jenv)->FindClass(jenv,"[D");
					jobjectArray objectArray = (*jenv)->NewObjectArray(jenv, ARR_DIMS(v)[0],cls,0);
					
					for (int idx = 0; idx < ARR_DIMS(v)[0]; ++idx) {
						// Create inner
						jfloatArray innerArray = (*jenv)->NewDoubleArray(jenv,ARR_DIMS(v)[1]);
						(*jenv)->SetDoubleArrayRegion(jenv, innerArray, 0, ARR_DIMS(v)[1], (jdouble *) (ARR_DATA_PTR(v) + nc*sizeof(double) ));
						nc += ARR_DIMS(v)[1];
						(*jenv)->SetObjectArrayElement(jenv, objectArray, idx, innerArray);
						(*jenv)->DeleteLocalRef(jenv,innerArray);
					}
					
					val.l = objectArray;

				} else {
					// Copy element by element 
					//...
				}
				
			}
			// Other objects
			else if(strcmp(T, "Ljava/lang/String;") == 0) {
				text* txt = DatumGetTextP(arg);
				int len = VARSIZE_ANY_EXHDR(txt)+1;
				char t[len];
				text_to_cstring_buffer(txt, &t, len);
				val.l = (*jenv)->NewStringUTF(jenv, t);
			}

			args[i] = val;
		}
		*/
	
		Datum values[entry->n_return];
		bool primitive[entry->n_return];
		memset(primitive, 0, sizeof(primitive));
		
		// Prepare args
		jvalue args[entry->n_args];
		short argprim[entry->n_args];
		memset(argprim, 0, sizeof(argprim));
		
		int jfr = argDeSerializer(args, argprim, entry);

		
		//elog(WARNING,"[DEBUG]: Calling java function %s->%s",entry->class_name,entry->method_name);
		if(jfr == 0) {			
			jfr = call_java_function(values, primitive, entry->class_name, entry->method_name, entry->signature, entry->return_type, &args, entry->data);
		} 

		// Release args
		freejvalues(args, argprim, entry->n_args);
		
		// Check for exception
		if( jfr > 0 ) {
			elog(WARNING,"Java exception occured. Code: %d",jfr);
			// Set error msg to true;
			entry->error = true;
			jthrowable exh = (*jenv)->ExceptionOccurred(jenv);
			
			if(exh !=0) {
				prepareErrorMsg(exh, &entry->data, MAX_DATA); 
			} else {
				strcpy(entry->data,"Unknown error occured during java function call");
			}	

			// Clear exception
			(*jenv)->ExceptionClear(jenv);	
		} else if(jfr < 0) {
			// Activate error messaging (supplied via data)
			entry->error = true;
		} else {
			// Set error msg to false;
			entry->error = false;
		
			// Prepare return
			char* data = entry->data;
			for(int i = 0; i < entry->n_return; i++) {
				if(!primitive[i]) 
					datumSerialize( PG_DETOAST_DATUM( values[i] ) , false, primitive[i],-1, &data);
				else
					datumSerialize( values[i], false, primitive[i],-1, &data);
			}		
		}

	    SpinLockAcquire(&worker_head->lock);
		dlist_push_tail(&worker_head->return_list,&entry->node);
		SpinLockRelease(&worker_head->lock);
		
		/*
			Cleanup
		*/
		if(activeSPI) {
			/*
				SPI cleanup
			*/
			disconnect_SPI();
			PopActiveSnapshot();
			CommitTransactionCommand();
		}
		SetLatch( entry->notify_latch );

		//elog(WARNING,"BG worker: DONE");	
	}

    elog(WARNING, "SIG RECEIVED");	
}

void prepareErrorMsg(jthrowable exh, char* target, int cutoff) {
	jclass Throwable_class = (*jenv)->FindClass(jenv, "java/lang/Throwable");
	jmethodID Throwable_getMessage =  (*jenv)->GetMethodID(jenv,Throwable_class, "getMessage", "()Ljava/lang/String;");

	// Get error msg			
	jstring jmsg = (jstring)(*jenv)->CallObjectMethod(jenv, exh, Throwable_getMessage);

	const char* msg = (*jenv)->GetStringUTFChars(jenv, jmsg, false);

	strcpy(target,msg);
	target += strlen(msg);

	(*jenv)->ReleaseStringUTFChars(jenv, jmsg, msg);
	
	// Get stack trace
	jmethodID stacktrace_method = (*jenv)->GetMethodID(jenv, Throwable_class, "getStackTrace", "()[Ljava/lang/StackTraceElement;");		
	jclass ste_class = (*jenv)->FindClass(jenv, "java/lang/StackTraceElement");
	jmethodID ste_tostring =  (*jenv)->GetMethodID(jenv,ste_class, "toString", "()Ljava/lang/String;");
	
	jobjectArray stacktraces = (*jenv)->CallObjectMethod(jenv, exh, stacktrace_method);

	jsize len = (*jenv)->GetArrayLength(jenv, stacktraces);
	int c = strlen(msg);	
	for(int i = 0; i < len; i++) {
		jobject tmp =  (*jenv) -> GetObjectArrayElement(jenv,stacktraces, i);
		// Get trace			
		jstring tmsg = (jstring)(*jenv)->CallObjectMethod(jenv, tmp, ste_tostring);
		const char* cmsg = (*jenv)->GetStringUTFChars(jenv, tmsg, false);

		// Copy out
		if( (c+1+strlen(cmsg)) < cutoff) {
			*target = '\n';
			target++;
			strcpy(target,cmsg);
			target += strlen(cmsg);
			c += strlen(cmsg);

			(*jenv)->ReleaseStringUTFChars(jenv, tmsg, cmsg);
		}
	}
}

// ToDo: Solve why external call of the two Postgres functions below crash, and then remove here.
Datum
datumDeSerialize(char **address, bool *isnull)
{
    int        header;
    void       *d;

    /* Read header word. */
    memcpy(&header, *address, sizeof(int));
    *address += sizeof(int);

    if (header == -2)
    {
        *isnull = true;
        return (Datum) 0;
    }

    *isnull = false;

    /* If this datum is pass-by-value, sizeof(Datum) bytes follow. */
    if (header == -1)
    {
        Datum        val;

        memcpy(&val, *address, sizeof(Datum));
        *address += sizeof(Datum);
        return val;
    }

    /* Pass-by-reference case; copy indicated number of bytes. */
    Assert(header > 0);
	//elog(WARNING,"[XZDEBUG](datumDeSerialize): %d",header);

    d = palloc(header);
    memcpy(d, *address, header);
    *address += header;
    
    return PointerGetDatum(d);
}

