//#include "pg_config.h"
//#include "pg_config_manual.h"

#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
#include "executor/spi.h" 
#include "funcapi.h"
#include "access/htup_details.h"
#include <jni.h>
#include <dlfcn.h>
#include <time.h>
#include "catalog/pg_type.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "moonshot.h"
#include "moonshot_jvm.h"
#include "moonshot_spi.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "ctype.h"

#include <utils/typcache.h>
#include "postmaster/bgworker.h"

#include "commands/event_trigger.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/event_trigger.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/hsearch.h"

#include "storage/proc.h"

PG_MODULE_MAGIC;

worker_data_head *worker_head_user = NULL;
worker_data_head *worker_head_global = NULL;

HTAB *function_hash = NULL;

enum { NS_PER_SECOND = 1000000000 };

void GetNAttributes(HeapTupleHeader tuple,
                int16 N, 
                Datum* datum, bool *isNull, bool *passbyval);


void sub_timespec(struct timespec t1, struct timespec t2, struct timespec *td)
{
    td->tv_nsec = t2.tv_nsec - t1.tv_nsec;
    td->tv_sec  = t2.tv_sec - t1.tv_sec;
    if (td->tv_sec > 0 && td->tv_nsec < 0)
    {
        td->tv_nsec += NS_PER_SECOND;
        td->tv_sec--;
    }
    else if (td->tv_sec < 0 && td->tv_nsec > 0)
    {
        td->tv_nsec -= NS_PER_SECOND;
        td->tv_sec++;
    }
}

static char* pgtype_to_java(Oid type) {
    switch(type) {
        case BYTEAOID:
            return "[B";
        case BOOLOID:
            return "Z";
        case INT2OID:
            return "S";
        case INT4OID:
            return "I";
        case INT8OID:
            return "J";
        case FLOAT4OID:
            return "F";
        case FLOAT8OID:
            return "D";
        case TEXTOID:
            return "Ljava/lang/String;";
        default:
            return "O";
    }
}

static Datum java_func_handler(PG_FUNCTION_ARGS)
{
    bool isnull;
    Datum ret;
    char *source;
    bool found;
    MemoryContext oldctx;
    Oid fid = fcinfo->flinfo->fn_oid;

    //elog(WARNING,"[DEBUG]: Entry java function handler");

    if(function_hash == NULL) {
        //elog(WARNING,"[DEBUG]: Init cache");

        // Init hash cache
        HASHCTL        ctl;

        /* Create the hash table. */
        MemSet(&ctl, 0, sizeof(ctl));
        ctl.keysize = sizeof(Oid);
        ctl.entrysize = sizeof(control_entry);
        ctl.hcxt = TopMemoryContext;

        oldctx = MemoryContextSwitchTo(TopMemoryContext);
        function_hash = hash_create("function control cache", 128, &ctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
        MemoryContextSwitchTo(oldctx);
    }

    // Lookup in cache
    oldctx = MemoryContextSwitchTo(TopMemoryContext);
    control_entry* centry = (control_entry *) hash_search(function_hash, (void *) &fid, HASH_ENTER, &found);
    
    if (!found) {
        //elog(WARNING,"CENTRY NOT FOUND: %d",fid);
     
        /* Fetch pg_proc entry. */
        HeapTuple tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(fid));
        
        if (!HeapTupleIsValid(tuple))
                elog(ERROR, "cache lookup failed for function %u",
                            fid);

        Form_pg_proc fstruct = (Form_pg_proc) GETSTRUCT(tuple);
        ret = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_prosrc, &isnull);
        if (isnull)
                elog(ERROR, "could not find call info for function \"%u\"",
                            fid);
        
        source = DatumGetCString(DirectFunctionCall1(textout, ret));
        
        //ereport(WARNING, (errmsg("source text of function : %s", source)));

        // Infer return type
        centry->return_type = pgtype_to_java(fstruct->prorettype);

        char* token = strtok(source, "|");
        
        if(token != NULL) {
            centry->mode = (char*) strdup(token);
            
            // ToDo: Re-order
            token = strtok(0,"|");
            if(token != NULL) {
                centry->class_name = strdup( token );

                token = strtok(0,"|");
                if(token != NULL) {
                    centry->method_name = strdup( token );
                    
                    token = strtok(0,"|");
                    if(token != NULL) {
                        centry->signature = strdup(token);
                    } else {
                    
                            
                        if(centry->return_type[0]=='O') {
                            elog(ERROR,"Return type requires manual specification of Java signature");
                        }
                        
                        // Try to infer signature   
                        
                        // Args                     
                        Oid        *argtypes;
                        char      **argnames;
                        char       *argmodes;
                        char       *proname;

                        int numargs = get_func_arg_info(tuple, &argtypes, &argnames, &argmodes);
                        
                        centry->signature = palloc(512);
                        centry->signature[0] = '(';
                        int pos = 1;
                        for (int i = 0; i < numargs; i++)
                        {
                            Oid argtype = fstruct->proargtypes.values[i];
                            char* type = pgtype_to_java(argtype);
                            //elog(WARNING,"arg: %s", type);
                            
                            if(type[0] == 'O') {
                                elog(ERROR,"Argument type requires manual specification of Java signature");
                            }
                            
                            // Build signature
                            int len = strlen(type);
                            memcpy(&centry->signature[pos],type,strlen(centry->return_type));
                            pos += len;
                        }
                        
                        centry->signature[pos] = ')';
                        pos++;
                        // Return type
                        memcpy(&centry->signature[pos],centry->return_type,strlen(centry->return_type)+1);
                    }
                
                } else {
                    elog(ERROR,"No method name supplied");
                }

            } else {
                elog(ERROR,"No class name supplied");
            }

        } else {
            elog(ERROR,"No Java function information supplied");
        }
        
        ReleaseSysCache(tuple);
    }

    //elog(WARNING,"mode: %s",centry->mode);
    //elog(WARNING,"class: %s",centry->class_name);
    //elog(WARNING,"sig: %s",centry->signature);
    
    Datum retr;
    if(centry->mode[0] == 'F') {
        // Foreground without SPI
        retr = control_fgworker(fcinfo, false, centry->class_name, centry->method_name, centry->signature, centry->return_type);
    } else if(centry->mode[0] == 'S') {
        // Foreground with SPI
        retr = control_fgworker(fcinfo, true, centry->class_name, centry->method_name, centry->signature, centry->return_type);   
    } else if(centry->mode[0] == 'G') {
        // Background global (NO SPI)
        retr = control_bgworkers(fcinfo, MAX_WORKERS, false, true, centry->class_name, centry->method_name, centry->signature, centry->return_type);
    } else if(centry->mode[0] == 'B') {
        // Background with SPI
        retr = control_bgworkers(fcinfo, MAX_WORKERS, true, false, centry->class_name, centry->method_name, centry->signature, centry->return_type);
    } else 
        elog(ERROR,"Not supported worker type: %s",centry->mode);
    
    MemoryContextSwitchTo(oldctx);

    PG_RETURN_DATUM( retr );   
}


PG_FUNCTION_INFO_V1(java_call_handler);

Datum java_call_handler(PG_FUNCTION_ARGS)
{
    Datum           ret = (Datum) 0;

    if (CALLED_AS_TRIGGER(fcinfo))
    {
        elog(ERROR,"Java function called as trigger not supported yet");
    }
    else if (CALLED_AS_EVENT_TRIGGER(fcinfo))
    {
        elog(ERROR,"Java function called as event trigger not supported yet");
    }
    else
    {
        ret = java_func_handler(fcinfo);
    }
    
    return ret;
}



PG_FUNCTION_INFO_V1(kmeans_gradients_cpu_float);
Datum
kmeans_gradients_cpu_float(PG_FUNCTION_ARGS) 
{

    char* class_name = "ai/sedn/unsupervised/Kmeans";
    char* method_name = "kmeans_gradients_cpu_float_ms";
    char* signature = "(Ljava/lang/String;Ljava/lang/String;IF[F)Lai/sedn/unsupervised/GradientReturn;";
    char* return_type = "O";
    
    Datum ret = control_bgworkers(fcinfo, MAX_WORKERS, true, false, class_name, method_name, signature, return_type);

    PG_RETURN_DATUM( ret );   
}

PG_FUNCTION_INFO_V1(kmeans_gradients_tvm_float);
Datum
kmeans_gradients_tvm_float(PG_FUNCTION_ARGS) 
{

    char* class_name = "ai/sedn/unsupervised/Kmeans";
    char* method_name = "kmeans_gradients_tvm_float_ms";
    char* signature = "(Ljava/lang/String;Ljava/lang/String;IFI[F)Lai/sedn/unsupervised/GradientReturn;";
    char* return_type = "O";

    Datum ret = control_bgworkers(fcinfo, MAX_WORKERS, true, false, class_name, method_name, signature, return_type);

    PG_RETURN_DATUM( ret );  
}

/* <- NON BACKGROUND WORKER BASED ! 
      ( UNKNOWN PROBLEM WITH SPI AND BACKGROUND WORKER ON COORDINATOR ! )
*/
PG_FUNCTION_INFO_V1(dbscan_batch);
Datum
dbscan_batch(PG_FUNCTION_ARGS) 
{
    char* class_name = "ai/sedn/unsupervised/DBscan";
    char* method_name = "dbscan_batch_ms";
    char* signature = "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;IFIJI)Lai/sedn/unsupervised/dbscan_batch_ret;";
    char* return_type = "O";

    Datum ret = control_fgworker(fcinfo, true, class_name, method_name, signature, return_type);

    PG_RETURN_DATUM( ret );   
}


/*
    Period search
*/

PG_FUNCTION_INFO_V1(psearch_ms_bg_cpu);
Datum
psearch_ms_bg_cpu(PG_FUNCTION_ARGS) 
{
    char* class_name = "gaia/cu7/algo/character/periodsearch/AEROInterface";
    char* method_name = "DoPeriodSearchCPU";
    
    char* signature = "([Lgaia/cu7/algo/character/periodsearch/PeriodData;)Lgaia/cu7/algo/character/periodsearch/PeriodResult;";
    char* return_type = "O";

    Datum ret = control_bgworkers(fcinfo, MAX_WORKERS, false, true, class_name, method_name, signature, return_type);
    
    PG_RETURN_DATUM( ret );   
}

PG_FUNCTION_INFO_V1(psearch_ms_bg_gpu);
Datum
psearch_ms_bg_gpu(PG_FUNCTION_ARGS) 
{
    char* class_name = "gaia/cu7/algo/character/periodsearch/AEROInterface";
    char* method_name = "DoPeriodSearchGPU";
    
    char* signature = "([Lgaia/cu7/algo/character/periodsearch/PeriodData;)Lgaia/cu7/algo/character/periodsearch/PeriodResult;";
    char* return_type = "O";

    Datum ret = control_bgworkers(fcinfo, MAX_WORKERS, false, true, class_name, method_name, signature, return_type);
    
    PG_RETURN_DATUM( ret );   
}

PG_FUNCTION_INFO_V1(psearch_ms_fg_cpu);
Datum
psearch_ms_fg_cpu(PG_FUNCTION_ARGS) 
{
    char* class_name = "gaia/cu7/algo/character/periodsearch/AEROInterface";
    char* method_name = "DoPeriodSearchCPU";
    
    char* signature = "([Lgaia/cu7/algo/character/periodsearch/PeriodData;)Lgaia/cu7/algo/character/periodsearch/PeriodResult;";
    char* return_type = "O";

    Datum ret = control_fgworker(fcinfo, false, class_name, method_name, signature, return_type);
    
    PG_RETURN_DATUM( ret );   
}

PG_FUNCTION_INFO_V1(psearch_ms_fg_gpu);
Datum
psearch_ms_fg_gpu(PG_FUNCTION_ARGS) 
{
    char* class_name = "gaia/cu7/algo/character/periodsearch/AEROInterface";
    char* method_name = "DoPeriodSearchGPU";
    
    char* signature = "([Lgaia/cu7/algo/character/periodsearch/PeriodData;)Lgaia/cu7/algo/character/periodsearch/PeriodResult;";
    char* return_type = "O";

    Datum ret = control_fgworker(fcinfo, false, class_name, method_name, signature, return_type);
    
    PG_RETURN_DATUM( ret );   
}

/* TESTING AREA */

PG_FUNCTION_INFO_V1(etest_ms);
Datum
etest_ms(PG_FUNCTION_ARGS) 
{
    char* class_name = "ai/sedn/unsupervised/Kmeans";
    char* method_name = "etest";
    
    char* signature = "([B)Lai/sedn/unsupervised/TestReturn2;";
    char* return_type = "O";

    Datum ret = control_fgworker(fcinfo, false, class_name, method_name, signature, return_type);
    
    PG_RETURN_DATUM( ret );   
}

PG_FUNCTION_INFO_V1(etest_bg_ms);
Datum
etest_bg_ms(PG_FUNCTION_ARGS) 
{
    char* class_name = "ai/sedn/unsupervised/Kmeans";
    char* method_name = "etest";
    
    char* signature = "([B)Lai/sedn/unsupervised/TestReturn2;";
    char* return_type = "O";

    Datum ret = control_bgworkers(fcinfo, MAX_WORKERS, false, true, class_name, method_name, signature, return_type);
  
    PG_RETURN_DATUM( ret );   
}



PG_FUNCTION_INFO_V1(astrots_ms);
Datum
astrots_ms(PG_FUNCTION_ARGS) 
{
    char* class_name = "gaia/cu7/mapping/AstroTsSQL_native2d";
    char* method_name = "astroTsReturn_ms";
    
    char* signature = "([B)Lgaia/cu7/mapping/AstroTsType;";
    char* return_type = "O";

    Datum ret = control_fgworker(fcinfo, false, class_name, method_name, signature, return_type);
    
    PG_RETURN_DATUM( ret );   
}

PG_FUNCTION_INFO_V1(astrots_bg_ms);
Datum
astrots_bg_ms(PG_FUNCTION_ARGS) 
{
    char* class_name = "gaia/cu7/mapping/AstroTsSQL_native2d";
    char* method_name = "astroTsReturn_ms";
    
    char* signature = "([B)Lgaia/cu7/mapping/AstroTsType;";
    char* return_type = "O";

    Datum ret = control_bgworkers(fcinfo, MAX_WORKERS, false, true, class_name, method_name, signature, return_type);
    
    PG_RETURN_DATUM( ret );   
}

PG_FUNCTION_INFO_V1(bprpspectra_veri_ms);
Datum
bprpspectra_veri_ms(PG_FUNCTION_ARGS) 
{
    char* class_name = "gaia/cu7/mapping/BpRpSpectraSQL_native2d";
    char* method_name = "bprpspectraVerification_ms";
    
    char* signature = "([B)Z";
    char* return_type = "Z";

    Datum ret = control_fgworker(fcinfo, false, class_name, method_name, signature, return_type);
    
    PG_RETURN_DATUM( ret );   
}


PG_FUNCTION_INFO_V1(bprpspectra_ms);
Datum
bprpspectra_ms(PG_FUNCTION_ARGS) 
{
    char* class_name = "gaia/cu7/mapping/BpRpSpectraSQL_native2d";
    char* method_name = "bprpspectraReturn_ms";
    
    char* signature = "([B)Lgaia/cu7/mapping/BpRpSpectraType;";
    char* return_type = "O";

    Datum ret = control_fgworker(fcinfo, false, class_name, method_name, signature, return_type);
    
    PG_RETURN_DATUM( ret );   
}

PG_FUNCTION_INFO_V1(bprpspectra_bg_ms);
Datum
bprpspectra_bg_ms(PG_FUNCTION_ARGS) 
{
    char* class_name = "gaia/cu7/mapping/BpRpSpectraSQL_native2d";
    char* method_name = "bprpspectraReturn_ms";
    
    char* signature = "([B)Lgaia/cu7/mapping/BpRpSpectraType;";
    char* return_type = "O";

    Datum ret = control_bgworkers(fcinfo, MAX_WORKERS, false, true, class_name, method_name, signature, return_type);
    
    PG_RETURN_DATUM( ret );   
}






/*
    Main function to deliver tasks to bg workers and collect results
*/
Datum control_bgworkers(FunctionCallInfo fcinfo, int n_workers, bool need_SPI, bool globalWorker, char* class_name, char* method_name, char* signature, char* return_type) {

    worker_data_head *worker_head;

    // Start workers if not started yet
    if(globalWorker) {
        if(worker_head_global == NULL || worker_head_global->n_workers == 0) {
            worker_head_global = launch_dynamic_workers(n_workers, need_SPI, globalWorker);
            pg_usleep(5000L);		/* 5msec */
        }
        worker_head = worker_head_global;
    } else {
        if(worker_head_user == NULL || worker_head_user->n_workers == 0) {
            worker_head_user = launch_dynamic_workers(n_workers, need_SPI, globalWorker);
            pg_usleep(5000L);		/* 5msec */
        }
        worker_head = worker_head_user; 
    }

    // Prepare return tuple
    ReturnSetInfo   *rsinfo       = (ReturnSetInfo *) fcinfo->resultinfo;
    
    TupleDesc tupdesc; 
    int rtype = get_call_result_type(fcinfo, NULL, &tupdesc);
    
    if(rsinfo != NULL)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("function returning set called in context "
                        "that cannot accept type set")));
    
    int natts;
    if(rtype == TYPEFUNC_COMPOSITE) {
        tupdesc = BlessTupleDesc(tupdesc);
        natts = tupdesc->natts;
    } else {
        tupdesc = NULL; 
        natts = 1;
    }

    Datum values[natts];
    bool* nulls = palloc0( natts * sizeof( bool ) );
    
    SpinLockAcquire(&worker_head->lock);
    /*
        Lock acquired
    */      
    if(!dlist_is_empty(&worker_head->free_list)) {
        dlist_node* dnode = dlist_pop_head_node(&worker_head->free_list);
        worker_exec_entry* entry = dlist_container(worker_exec_entry, node, dnode);

        strncpy(entry->class_name, class_name, strlen(class_name)+1);
        strncpy(entry->method_name, method_name, strlen(method_name)+1);
        strncpy(entry->signature, signature, strlen(signature)+1);
        strncpy(entry->return_type, return_type, 1);
        
        entry->n_return = natts;
        entry->notify_latch = MyLatch;

#ifdef PGXC        
        entry->n_args = argSerializer(entry->data, signature, &fcinfo->arg );
#else
        entry->n_args = argSerializer(entry->data, signature, &fcinfo->args );
#endif

        // Push
        dlist_push_tail(&worker_head->exec_list,&entry->node);
    
        for(int w = 0; w < worker_head->n_workers; w++) {
            SetLatch( worker_head->latch[w] );
        }

        SpinLockRelease(&worker_head->lock);
        /*
            Lock released
        */    

       // Wait for return
        dlist_iter    iter;
        bool got_signal = false;
        while(!got_signal)
	    {
            SpinLockAcquire(&worker_head->lock);
        
            if (dlist_is_empty(&worker_head->return_list))
            {
                SpinLockRelease(&worker_head->lock);
                int ev = WaitLatch(MyLatch,
                                WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                                1 * 1000L,
                                PG_WAIT_EXTENSION);
                ResetLatch(MyLatch);
                if (ev & WL_POSTMASTER_DEATH)
                    elog(FATAL, "unexpected postmaster dead");
                
                CHECK_FOR_INTERRUPTS();
                continue;
            }
    
            worker_exec_entry* ret;
            dlist_foreach(iter, &worker_head->return_list) {
                ret = dlist_container(worker_exec_entry, node, iter.cur);

                if(ret->taskid == entry->taskid) {
                    got_signal = true;
                    dlist_delete(iter.cur);
                    break;
               }
            }
            SpinLockRelease(&worker_head->lock);           
        
            if(got_signal) {

                // Process error message
                if(entry->error) {
                    pfree(nulls);
                    // Copy message
                    char buf[ strlen(entry->data) ];
                    strcpy(buf, entry->data);
                    
                    // Put to free list 
                    SpinLockAcquire(&worker_head->lock);
                    dlist_push_tail(&worker_head->free_list,entry);           
                    SpinLockRelease(&worker_head->lock);              

                    // Throw
                    elog(ERROR,"%s",buf);
                }

                // Prep return
                char* data = entry->data;
              
                Datum values[ret->n_return];
                for(int i = 0; i < ret->n_return; i++) {
                    bool null;
                    values[i] = datumDeSerialize(&data, &null);
                }
                
                // Cleanup
                SpinLockAcquire(&worker_head->lock);
                dlist_push_tail(&worker_head->free_list,entry);           
                SpinLockRelease(&worker_head->lock);              

                if(tupdesc != NULL) {
                    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);             
                    pfree(nulls);
                    PG_RETURN_DATUM( HeapTupleGetDatum(tuple ));    
                } else {
                    pfree(nulls);
                    PG_RETURN_DATUM( values[0] );
                }
            }
        }

    } else {
        SpinLockRelease(&worker_head->lock);
        pfree(nulls);
        elog(ERROR,"BG worker task queue is full");
    }

    return NULL;
}

/*
    Helper function to serialize function arguments for sending to background worker
*/
int argSerializer(char* target, char* signature, Datum* args) {
    bool openrb = false;
    bool openo = false;
    bool opensb = false;

    int ac = 0;
    char* spos;
    // Loop over signature and detect arguments
    for(int i = 0; i < strlen(signature); i++) {
        if(signature[i] == '(') {
            openrb = true;
            continue;
        }
        
        if(signature[i] == ')') {
            // Done
            break;
        }
    
        if(openrb) {
            // Ready to read arguments
            if( ( (!openo && !opensb) && (signature[i] == '[' || signature[i] == 'L'))   ) {
                // Buffer 
                *target = signature[i];
                target++;

                if(signature[i]=='[') {
                    opensb = true;
                } else {
                    openo = true;
                    spos = target-1;
                }

                continue;
            }     
            
            if ( openo ) {
                
                if(signature[i] != ';') {
                    // Buffer
                    *target = signature[i];
                    target++;
                } else {
                    // Serialize object argument
                    target[0] = signature[i];
                    target[1] = '\0';
                    target+=2;
                    
                    if(strcmp("Ljava/lang/String;",spos) == 0) {
                        // String
                        datumSerialize(args[ac], false, false, -1, &target);
                    } else {
                        // Composite type  
                        HeapTupleHeader t = DatumGetHeapTupleHeader(args[ac]);

                        // Serialize each composite object one-by-one
                        int16 Na = (int16) HeapTupleHeaderGetNatts( t );
                        
                        Datum attr[Na];
                        bool isnull[Na];
                        bool passbyval[Na];
                        GetNAttributes(t, Na, attr, isnull, passbyval) ;

                        for(int a = 0; a < Na; a++) {  
                            if(isnull[a]) {
                                elog(ERROR,"Attribute %d of composite type is null",a); 
                            } else {
                                datumSerialize(attr[a], false, passbyval[a], -1, &target); // getTupleBassBy slow !
                            } 
                        }
        
                    }
                    ac++;
                    openo = false;
                }
            } else if ( opensb ) {
                
                if(signature[i] == '[') {
                    // Buffer 
                    *target = signature[i];
                    target++;
                } else {
                    // ToDo: Check if supported
                    if(signature[i] != 'L') {
                        // Serialize native array
                        target[0] = signature[i];
                        target[1] = '\0';
                        target+=2;
                        datumSerialize( PG_DETOAST_DATUM( args[ac] ), false, false, -1, &target);
                    } else {
                        if(signature[i] == '[') {
                            elog(ERROR,"2d arrays as input to bg worker not supported yet");
                        } else if(signature[i] == 'L') {


                            if(strcmp("Ljava/lang/String;",signature+i) == 0) {
                                // compare is broken: Fix as above
                                elog(ERROR,"Array of strings as input to bg worker not supported yet");
                            } else {
                                // Array of Composite

                                // ToDo: Move up full signature read ...

                                // Read full signature
                                target[0] = signature[i];
                                target++;
                               
                                while(signature[i] != ';' && i < strlen(signature)) {
                                    i++;
                                    target[0] = signature[i];
                                    target++;
                                }  
                                target[0] = '\0';
                                target++;
                                
                                // Get array
                                ArrayType *v = DatumGetArrayTypeP(args[ac]);
                                Oid elemType = ARR_ELEMTYPE(v);
                                Datum  *datums;
                                bool   *nulls;
                                int     N;
                                int16   elemWidth;
                                bool    elemTypeByVal, isNull;
                                char    elemAlignmentCode;
                                                            
                                get_typlenbyvalalign(elemType, &elemWidth, &elemTypeByVal, &elemAlignmentCode);
                                deconstruct_array(v, elemType, elemWidth, elemTypeByVal, elemAlignmentCode, &datums, &nulls, &N);

                                // Serialize array size (4 byte)
                                int* tmp = (int*)(target);
                                tmp[0] = N;
                                target+=4;
    
                                // Serialize all array elements
                                for(int n = 0; n < N; n++) {
                                    // Prepare elements
    
                                    HeapTupleHeader t = DatumGetHeapTupleHeader(datums[n]);
                                  
                                  /*
                                     // TEST
                                    TupleDesc td = lookup_rowtype_tupdesc( HeapTupleHeaderGetTypeId(t), -1);
                                    elog(WARNING, "A");
                                    td = CreateTupleDescCopyConstr(td);
                                    elog(WARNING, "B");
                                    
                                    elog(WARNING, "N: %d",td->natts);

                                    ReleaseTupleDesc(td);                                  
                                    */

                                    // Serialize each composite object one-by-one
                                    int16 Na = (int16) HeapTupleHeaderGetNatts( t );
                                    
                                    Datum attr[Na];
                                    bool isnull[Na];
                                    bool passbyval[Na];
                                    GetNAttributes(t, Na, attr, isnull, passbyval);

                                    for(int a = 0; a < Na; a++) {  
                                        if(isnull[a]) {
                                            elog(ERROR,"Attribute %d of composite type is null",a); 
                                        } else {
                                            datumSerialize(attr[a], false, passbyval[a], -1, &target); // getTupleBassBy slow !
                                        } 
                                    }
                                }
                            }
                        } else {
                            elog(ERROR,"Unknown array signature");
                        }
                    }
                    
                    ac++;
                    opensb = false;
                }
            } else {
                // ToDo: Check if supported
                    
                // Serialize native argument
                target[0] = signature[i];
                target[1] = '\0';
                target+=2;
                datumSerialize(args[ac], false, true, -1, &target);
                ac++;
            }
        }
    }

    // Consistency check
    if(!openrb || openo || opensb) elog(ERROR,"Inconsistent Java function signature");        

    return ac;
}

/*
    Main function to start fg worker and collect results
*/
Datum control_fgworker(FunctionCallInfo fcinfo, bool need_SPI, char* class_name, char* method_name, char* signature, char* return_type) {
    
    ReturnSetInfo   *rsinfo       = (ReturnSetInfo *) fcinfo->resultinfo;
    
    TupleDesc tupdesc; 
    int rtype = get_call_result_type(fcinfo, NULL, &tupdesc);
    
    if(rtype != TYPEFUNC_COMPOSITE && rsinfo != NULL)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("function returning set called in context "
                        "that cannot accept type set")));
      
    if(rtype == TYPEFUNC_COMPOSITE && rsinfo == NULL) {
        tupdesc = BlessTupleDesc(tupdesc);
    } else if(rsinfo != NULL) {
        tupdesc = rsinfo->expectedDesc;
        // ToDo: Check if materialized allowed 
    } else {
       tupdesc = NULL; 
    }

    // Start JVM
    char error_msg[128];  
    if(jenv == NIL) {
        int jc = startJVM(error_msg);
        if(jc < 0 ) {
            elog(ERROR,"%s",error_msg);
        }
    }
    // Prep arguments
    jvalue args[fcinfo->nargs];
    short argprim[fcinfo->nargs];
    memset(argprim, 0, sizeof(argprim));
    argToJava(args, signature, fcinfo, argprim);
    
    // Call java function
    activeSPI = need_SPI;
    if(need_SPI) connect_SPI();
    PushActiveSnapshot(GetTransactionSnapshot());
   
    int jfr;
   
    if(rsinfo == NULL) {
        int natts;
        if(tupdesc != NULL) {
            natts = tupdesc->natts;
        } else {
            natts = 1;
        }
        Datum values[natts];
        bool* nulls = palloc0( natts * sizeof( bool ) );
        bool primitive[natts];
        memset(primitive, 0, sizeof(primitive));
       // elog(WARNING,"[DEBUG] %s",return_type);
        jfr = call_java_function(values, primitive, class_name, method_name, signature, return_type, &args, error_msg);
    
        if(jfr == 0) {     
            if(need_SPI) disconnect_SPI();
            PopActiveSnapshot();
            if(tupdesc != NULL && natts > 1) {
                HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
                pfree(nulls);
                freejvalues(args, argprim, fcinfo->nargs);
                PG_RETURN_DATUM( HeapTupleGetDatum(tuple ));
            } else {
                pfree(nulls);
                freejvalues(args, argprim, fcinfo->nargs);
                PG_RETURN_DATUM( values[0] );
            }
        } else {
            pfree(nulls);
        }
    } else {
     
        MemoryContext   per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
        MemoryContext   oldcontext    = MemoryContextSwitchTo(per_query_ctx);

        Tuplestorestate *tupstore     = tuplestore_begin_heap(false, false, work_mem);
        rsinfo->setResult             = tupstore;
        rsinfo->returnMode            = SFRM_Materialize;

        jfr = call_iter_java_function(tupstore,tupdesc,class_name, method_name, signature, &args, error_msg);

        MemoryContextSwitchTo(oldcontext);
    }
    
    if(need_SPI) disconnect_SPI();
    PopActiveSnapshot();

    if( jfr > 0 ) {
        jthrowable exh = (*jenv)->ExceptionOccurred(jenv);
			
        // Clear exception
        (*jenv)->ExceptionClear(jenv);
			
        if(exh !=0) {
            char msg[2048];
            prepareErrorMsg(exh, msg, 2048);
            elog(ERROR,"Java exception: %s",msg);

        } else {
            elog(ERROR,"Unknown Java exception occured (%d)",jfr);
        }	
    } else if(jfr < 0) {
        elog(ERROR,"%s",error_msg);
    }

    // Final cleanup
    freejvalues(args, argprim, fcinfo->nargs);
   
    PG_RETURN_NULL();
}

/*
    Conversion from PG text to jvalue
    Move to _jvm ?
*/
jvalue PG_text_to_jvalue(text* txt) {
    jvalue val;
   
    int len = VARSIZE_ANY_EXHDR(txt)+1;
    char t[len];
   
    text_to_cstring_buffer(txt, &t, len);
   
    val.l = (*jenv)->NewStringUTF(jenv, t);
   
    return val;
}

/*
    Helper function to convert arguments to jvalues for foreground worker
*/
int argToJava(jvalue* target, char* signature, FunctionCallInfo fcinfo, short* argprim) {
    bool openrb = false;
    bool openo = false;
    bool opensb = false;

    int ac = 0;
    char buf[256];
    int pos = 0;
    
    // Loop over signature and detect arguments
    for(int i = 0; i < strlen(signature); i++) {
        
        if(signature[i] == '(') {
            openrb = true;
            continue;
        }
        
        if(signature[i] == ')') {
            // Done
            break;
        }
    
        if(openrb) {
            // Ready to read arguments
            if( ( !openo && !opensb && (signature[i] == '[' || signature[i] == 'L'))   ) {
                buf[pos] = signature[i];
                pos++;
                if(signature[i]=='[') {
                    opensb = true;
                } else {
                    openo = true;
                }

                continue;
            }     
            
            if ( openo ) {
                
                if(signature[i] != ';') {
                    // Buffer 
                    buf[pos] = signature[i];
                    pos++;
                } else {
                    // Serialize object argument
                    buf[pos] = ';';
                    buf[pos+1] = '\0';

                    if(strcmp("Ljava/lang/String;",buf) == 0) {
                        target[ac] = (jvalue) PG_text_to_jvalue( DatumGetTextP( PG_GETARG_DATUM(ac) ));
                        argprim[ac] = 2;
                    } else {
                        // Map to composite type
                        // ToDo: Move to helper function ?
                        jclass cls = (*jenv)->FindClass(jenv, buf);
                        if(cls == NULL) {
                            elog(ERROR,"Java class %s not known",buf);
                        }
                    
                        // Get field info
                        jmethodID getFields = (*jenv)->GetMethodID(jenv, (*jenv)->GetObjectClass(jenv,cls), "getFields", "()[Ljava/lang/reflect/Field;");
                        jobjectArray fieldsList = (jobjectArray)  (*jenv)->CallObjectMethod(jenv, cls, getFields);    
                        jsize len =  (*jenv)->GetArrayLength(jenv,fieldsList);
                    
                        if(len == 0) {
                            elog(ERROR,"Java class %s has no fields",buf);
                        } else {
                            // Construct new instance
                            jmethodID constructor = (*jenv)->GetMethodID(jenv, cls, "<init>", "()V");
                            jobject cobj = (*jenv)->NewObject(jenv, cls, constructor);
                                 
                            TupleTableSlot  *t = (TupleTableSlot *) PG_GETARG_POINTER(ac);
                            bool isnull;

                            // Loop over class fields                            
                            for(int i = 0; i < len; i++) {
                                
                                // ToDo: Move to helper function ?

                                // Detect field
                                jobject field = (*jenv)->GetObjectArrayElement(jenv, fieldsList, i);
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

                                char error_msg[128];
                                char* sig = convert_name_to_JNI_signature(typename, error_msg);
                                
                                if(sig == NULL) {
                                    elog(ERROR,"%s",error_msg);   
                                }
                               
                                jfieldID fid = (*jenv)->GetFieldID(jenv, cls, fieldname, sig);

                                Datum attr = GetAttributeByName(t, fieldname, &isnull); // Note: By num should be faster
                                if(isnull) {
                                    elog(ERROR,"No attribute %s in supplied composite argument",fieldname);
                                }

                                int res = set_jobject_field_from_datum(&cobj, &fid, &attr, sig); 

                                (*jenv)->ReleaseStringUTFChars(jenv, jstr2, typename);
                                (*jenv)->ReleaseStringUTFChars(jenv, jstr, fieldname);
                                
                                if(res == 0) {                                
                                    target[ac].l = cobj;
                                    argprim[ac] = 1;
                                } else {
                                    elog(ERROR,"Unknown error in setting composite type");
                                }
                                
                            }
                                                       
                        }
                    }        
                    
                    ac++;       
                    openo = false;
                    pos = 0;
                }
            } else if ( opensb ) {
                
                if(signature[i] == '[') {
                    // Buffer 
                    buf[pos] = signature[i];
                    pos++;
                } else {
                    ArrayType* v;
                    switch(signature[i]) {
                        case 'B':
                            switch(pos) {
                                case 1:
                                    bytea* bytes  = DatumGetByteaP( PG_GETARG_DATUM(ac) );
                                    jsize  nElems = VARSIZE(bytes) - sizeof(int32);
                                    jbyteArray byteArray  =(*jenv)->NewByteArray(jenv,nElems);
                                    (*jenv)->SetByteArrayRegion(jenv, byteArray, 0, nElems, (jbyte*)VARDATA(bytes));
                                    target[ac].l = byteArray;
                                    argprim[ac] = 1;
                                    break;
                                elog(ERROR,"Higher dimensional array as argument not implemented yet for foreground Java worker");   
                            }
                            break;
                        case 'I':
                            v = DatumGetArrayTypeP( PG_GETARG_DATUM(ac) ); 
                            switch(pos) {
                                case 1:
                                    if(!ARR_HASNULL(v)) {
                                        jsize      nElems = (jsize)ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));
                                        jintArray intArray = (*jenv)->NewIntArray(jenv,nElems);
                                        (*jenv)->SetIntArrayRegion(jenv, intArray, 0, nElems, (jint*)ARR_DATA_PTR(v));
                                        target[ac].l = intArray;
                                        argprim[ac] = 1;
                                    } else {
                                        elog(ERROR,"Array with NULLs not implemented yet for foreground Java worker");     
                                    }
                                    break;
                                case 2:
                                    int nc = 0;
                                    if(!ARR_HASNULL(v)) {
                                        jclass cls = (*jenv)->FindClass(jenv,"[I");
                                        jobjectArray objectArray = (*jenv)->NewObjectArray(jenv, ARR_DIMS(v)[0], cls, 0);
                                        
                                        for (int idx = 0; idx < ARR_DIMS(v)[0]; ++idx) {
                                            // Create inner
                                            jintArray innerArray = (*jenv)->NewIntArray(jenv,ARR_DIMS(v)[1]);
                                            (*jenv)->SetIntArrayRegion(jenv, innerArray, 0, ARR_DIMS(v)[1], (jint *) (ARR_DATA_PTR(v) + nc*sizeof(int) ));
                                            nc += ARR_DIMS(v)[1];
                                            (*jenv)->SetObjectArrayElement(jenv, objectArray, idx, innerArray);
                                            (*jenv)->DeleteLocalRef(jenv,innerArray);
                                        }    
                                        target[ac].l = objectArray;
                                        argprim[ac] = 1;
                                    } else {
                                        elog(ERROR,"Array with NULLs not implemented yet for foreground Java worker");     
                                    }
                                    break;
                                elog(ERROR,"Higher dimensional array as argument not implemented yet for foreground Java worker");   
                            }
                            break;
                        case 'J':
                            v = DatumGetArrayTypeP( PG_GETARG_DATUM(ac) );          
                            switch(pos) {
                                case 1:
                                    if(!ARR_HASNULL(v)) {
                                        jsize      nElems = (jsize)ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));
                                        jlongArray longArray = (*jenv)->NewLongArray(jenv,nElems);
                                        (*jenv)->SetLongArrayRegion(jenv,longArray, 0, nElems, (jlong*)ARR_DATA_PTR(v));
                                        target[ac].l = longArray;
                                        argprim[ac] = 1;
                                    } else {
                                        elog(ERROR,"Array with NULLs not implemented yet for foreground Java worker");     
                                    }
                                    break;
                                case 2:
                                    int nc = 0;
                                    if(!ARR_HASNULL(v)) {
                                        jclass cls = (*jenv)->FindClass(jenv,"[J");
                                        jobjectArray objectArray = (*jenv)->NewObjectArray(jenv, ARR_DIMS(v)[0], cls, 0);
                                        
                                        for (int idx = 0; idx < ARR_DIMS(v)[0]; ++idx) {
                                            // Create inner
                                            jlongArray innerArray = (*jenv)->NewLongArray(jenv,ARR_DIMS(v)[1]);
                                            (*jenv)->SetLongArrayRegion(jenv, innerArray, 0, ARR_DIMS(v)[1], (jlong *) (ARR_DATA_PTR(v) + nc*sizeof(long) ));
                                            nc += ARR_DIMS(v)[1];
                                            (*jenv)->SetObjectArrayElement(jenv, objectArray, idx, innerArray);
                                            (*jenv)->DeleteLocalRef(jenv,innerArray);
                                        }    
                                        target[ac].l = objectArray;
                                        argprim[ac] = 1;
                                    } else {
                                        elog(ERROR,"Array with NULLs not implemented yet for foreground Java worker");     
                                    }
                                    break;
                                
                                elog(ERROR,"Higher dimensional array as argument not implemented yet for foreground Java worker");   
                            }
                            break;
                        case 'F':
                            v = DatumGetArrayTypeP( PG_GETARG_DATUM(ac) );       
                            switch(pos) {
                                case 1:
                                    if(!ARR_HASNULL(v)) {
                                        jsize      nElems = (jsize)ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));
                                        jfloatArray floatArray = (*jenv)->NewFloatArray(jenv,nElems);
                                        (*jenv)->SetFloatArrayRegion(jenv,floatArray, 0, nElems, (jfloat *)ARR_DATA_PTR(v));
                                        target[ac].l = floatArray;
                                        argprim[ac] = 1;
                                    } else {
                                        elog(ERROR,"Array with NULLs not implemented yet for foreground Java worker");     
                                    }
                                    break;
                                case 2:
                                    int nc = 0;
                                    if(!ARR_HASNULL(v)) {
                                        jclass cls = (*jenv)->FindClass(jenv,"[F");
                                        jobjectArray objectArray = (*jenv)->NewObjectArray(jenv, ARR_DIMS(v)[0], cls, 0);
                                        
                                        for (int idx = 0; idx < ARR_DIMS(v)[0]; ++idx) {
                                            // Create inner
                                            jfloatArray innerArray = (*jenv)->NewFloatArray(jenv,ARR_DIMS(v)[1]);
                                            (*jenv)->SetFloatArrayRegion(jenv, innerArray, 0, ARR_DIMS(v)[1], (jfloat *) (ARR_DATA_PTR(v) + nc*sizeof(float) ));
                                            nc += ARR_DIMS(v)[1];
                                            (*jenv)->SetObjectArrayElement(jenv, objectArray, idx, innerArray);
                                            (*jenv)->DeleteLocalRef(jenv,innerArray);
                                        }    
                                        target[ac].l = objectArray;
                                        argprim[ac] = 1;
                                    } else {
                                        elog(ERROR,"Array with NULLs not implemented yet for foreground Java worker");     
                                    }
                                    break;
                                elog(ERROR,"Higher dimensional array as argument not implemented yet for foreground Java worker");   
                            }
                            break;
                        case 'D':
                            v = DatumGetArrayTypeP( PG_GETARG_DATUM(ac) );       
                            switch(pos) {
                                case 1:
                                    if(!ARR_HASNULL(v)) {
                                        jsize      nElems = (jsize)ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));
                                        jdoubleArray doubleArray = (*jenv)->NewDoubleArray(jenv,nElems);
                                        (*jenv)->SetDoubleArrayRegion(jenv,doubleArray, 0, nElems, (jdouble *)ARR_DATA_PTR(v));
                                        target[ac].l = doubleArray;
                                        argprim[ac] = 1;
                                    } else {
                                        elog(ERROR,"Array with NULLs not implemented yet for foreground Java worker");     
                                    }
                                    break;
                                case 2:
                                    int nc = 0;
                                    if(!ARR_HASNULL(v)) {
                                        jclass cls = (*jenv)->FindClass(jenv,"[D");
                                        jobjectArray objectArray = (*jenv)->NewObjectArray(jenv, ARR_DIMS(v)[0], cls, 0);
                                        
                                        for (int idx = 0; idx < ARR_DIMS(v)[0]; ++idx) {
                                            // Create inner
                                            jdoubleArray innerArray = (*jenv)->NewDoubleArray(jenv,ARR_DIMS(v)[1]);
                                            (*jenv)->SetDoubleArrayRegion(jenv, innerArray, 0, ARR_DIMS(v)[1], (jdouble *) (ARR_DATA_PTR(v) + nc*sizeof(double) ));
                                            nc += ARR_DIMS(v)[1];
                                            (*jenv)->SetObjectArrayElement(jenv, objectArray, idx, innerArray);
                                            (*jenv)->DeleteLocalRef(jenv,innerArray);
                                        }    
                                        target[ac].l = objectArray;
                                        argprim[ac] = 1;
                                    } else {
                                        elog(ERROR,"Array with NULLs not implemented yet for foreground Java worker");     
                                    }
                                    break;
                                elog(ERROR,"Higher dimensional array as argument not implemented yet for foreground Java worker");   
                            }
                            break;
                        case 'L':
                            // Composite
                            i++;
                            char cbuf[128];
                            cbuf[0] = 'L';
                            int c = 1;
                            while(signature[i] != ';' && i < strlen(signature) ) {
                                cbuf[c] = signature[i];
                                i++;
                                c++;
                            }
                            if(signature[i] == ';') {
                                cbuf[c] = ';';
                                cbuf[c+1] = '\0';
                                
                                // Build composite type                              
                                jclass cls = (*jenv)->FindClass(jenv, cbuf);
                                if(cls == NULL) {
                                    elog(ERROR,"Java class %s not known",cbuf);
                                }
                               
                                // Get field info
                                jmethodID getFields = (*jenv)->GetMethodID(jenv, (*jenv)->GetObjectClass(jenv,cls), "getFields", "()[Ljava/lang/reflect/Field;");
                                jobjectArray fieldsList = (jobjectArray)  (*jenv)->CallObjectMethod(jenv, cls, getFields);    
                                jsize len =  (*jenv)->GetArrayLength(jenv,fieldsList);
                            
                                if(len == 0) {
                                    elog(ERROR,"Java class %s has no fields",cbuf);
                                }
                                
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
                                        elog(ERROR,"%s",error_msg);   
                                    }
                                
                                    fid[j] = (*jenv)->GetFieldID(jenv, cls, fieldname[j], sig[j]);

                                    // Convert fieldname to lower case for PG lookup
                                    for(int k = 0; k < strlen(fieldname[k]); k++) {
                                        fieldname[j][k] = tolower(fieldname[j][k]);
                                    } 
                                }

                                // Get array
                                v = PG_GETARG_ARRAYTYPE_P(ac);
                                Oid elemType = ARR_ELEMTYPE(v);
                                Datum  *datums;
                                bool   *nulls;
                                int     N;
                                int16   elemWidth;
                                bool    elemTypeByVal, isNull;
                                char    elemAlignmentCode;
                                                            
                                get_typlenbyvalalign(elemType, &elemWidth, &elemTypeByVal, &elemAlignmentCode);
                                deconstruct_array(v, elemType, elemWidth, elemTypeByVal, elemAlignmentCode, &datums, &nulls, &N);
                                
                                // Create array
                                jobjectArray array = (*jenv)->NewObjectArray(jenv,N,cls,0);
                                
                                jmethodID constructor = (*jenv)->GetMethodID(jenv, cls, "<init>", "()V");
                                       
                                // Loop over array elements
                                for (int n = 0; n < N; n++)
                                {
                                    if (!nulls[n]) {
                                        // Construct new instance
                                        jobject cobj = (*jenv)->NewObject(jenv, cls, constructor);
                        
                                        // Prepare elements
                                        HeapTupleHeader t = DatumGetHeapTupleHeader(datums[n]);
                                        
                                        // Loop over class fields                            
                                        for(int j = 0; j < len; j++) {
                                            bool isnull;
                                            Datum attr = GetAttributeByName(t, fieldname[j], &isnull); // Note: By num should be faster
                                            if(isnull) {
                                                elog(ERROR,"No attribute %s in supplied composite argument",fieldname[j]);
                                            }
                                            int res = set_jobject_field_from_datum(&cobj, &fid[j], &attr, sig[j]); 
                                            
                                            //elog(NOTICE,"-> %s (%s)", fieldname[j], sig[j]);
                                          
                                            if(res != 0) {                                
                                                elog(ERROR,"Unknown error in setting composite type");
                                            }
                                        }
                                    
                                        (*jenv)->SetObjectArrayElement(jenv, array, n, cobj);
                                        (*jenv)->DeleteLocalRef(jenv,cobj);
                                    } 
                                }
                                         
                                target[ac].l = array;
                                argprim[ac] = 1;
                                
                                // Cleanup
                                for(int j = 0; i < len; j++) {
                                    (*jenv)->ReleaseStringUTFChars(jenv, jstr2[j], typename[j]);
                                    (*jenv)->ReleaseStringUTFChars(jenv, jstr[j], fieldname[j]);
                                }

                            } else {
                                elog(ERROR,"Object array signature incomplete: %s",cbuf);
                            }

                            break;
                        
                        default:
                            elog(ERROR,"Array type as argument not implemented yet for foreground Java worker");
                    }
                    
                    ac++;
                    opensb = false;
                    pos = 0;
                }
            } else {   
                // Convert native argument
                switch(signature[i]) {
                    case 'S':
                        target[ac].s = (jshort) PG_GETARG_INT16(ac);  
                        break;
                    case 'I':
                        target[ac].i = (jint) PG_GETARG_INT32(ac);  
                        break;
                    case 'J':
                        target[ac].j = (jlong) PG_GETARG_INT64(ac);     
                        break;
                    case 'F':
                        target[ac].f =  (jfloat) PG_GETARG_FLOAT4(ac);
                        break;
                    case 'D':
                        target[ac].d =  (jdouble) PG_GETARG_FLOAT8(ac);
                        break;
                    default:
                        elog(ERROR,"Argument type not implemented yet for foreground Java worker");
                }
                ac++;
            }
        }
    }

    // Consistency check
    if(!openrb || openo || opensb) elog(ERROR,"Inconsistent Java function signature");        

    return ac;
}


PG_FUNCTION_INFO_V1(ms_clear_user_queue);
Datum
ms_clear_user_queue(PG_FUNCTION_ARGS) {
    if(worker_head_user == NULL) {
        Oid			roleid = GetUserId();
	    Oid			dbid = MyDatabaseId;
	
        char buf[12];
        snprintf(buf, 12, "MW_%d_%d", roleid, dbid); 
        
        LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
        bool found;
        worker_data_head* worker_head_user = (worker_data_head*) ShmemInitStruct(buf,
                                    sizeof(worker_data_head),
                                    &found);
        LWLockRelease(AddinShmemInitLock);
        
        if(!found) {
            worker_head_user->n_workers = 0;
        }
    }

    if(worker_head_user->n_workers == 0) {
        elog(ERROR,"No workers started yet");
    }

    SpinLockAcquire(&worker_head_user->lock);
    int c = 0;
    while(!dlist_is_empty(&worker_head_user->exec_list)) {
        dlist_node* dnode = dlist_pop_head_node(&worker_head_user->exec_list);
        dlist_push_tail(&worker_head_user->free_list,dnode);
        c++;
    }
    
    SpinLockRelease(&worker_head_user->lock);   

    PG_RETURN_INT32(c);
}

PG_FUNCTION_INFO_V1(ms_show_user_queue);
Datum
ms_show_user_queue(PG_FUNCTION_ARGS) {
    
    if(worker_head_user == NULL) {
        Oid			roleid = GetUserId();
	    Oid			dbid = MyDatabaseId;
	
        char buf[12];
        snprintf(buf, 12, "MW_%d_%d", roleid, dbid); 
        
        LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
        bool found;
        worker_data_head* worker_head_user = (worker_data_head*) ShmemInitStruct(buf,
                                    sizeof(worker_data_head),
                                    &found);
        LWLockRelease(AddinShmemInitLock);

        if(!found) {
            worker_head_user->n_workers = 0;
        }
    } 

    if(worker_head_user->n_workers == 0) {
        elog(ERROR,"No workers started yet");
    }

    SpinLockAcquire(&worker_head_user->lock);
    
    int c = 0;
    dlist_iter iter;
    
    dlist_foreach(iter, &worker_head_user->exec_list) {
        c++;
    }
    
    SpinLockRelease(&worker_head_user->lock);   

    PG_RETURN_INT32(c);
}

PG_FUNCTION_INFO_V1(ms_kill_user_workers);
Datum
ms_kill_user_workers(PG_FUNCTION_ARGS) {
   
    Oid			roleid = GetUserId();
    Oid			dbid = MyDatabaseId;

    char buf[BGW_MAXLEN];
    snprintf(buf, BGW_MAXLEN, "MW_%d_%d", roleid, dbid); 
    
    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    bool found;
    worker_data_head* worker_head_user = (worker_data_head*) ShmemInitStruct(buf,
                                sizeof(worker_data_head),
                                &found);
    LWLockRelease(AddinShmemInitLock);

    if(!found) {
        worker_head_user->n_workers = 0;
        elog(ERROR,"Can not kill workers if not started yet");
    }

    SpinLockAcquire(&worker_head_user->lock);
    
    int n_workers = worker_head_user->n_workers;
    
    if(n_workers == 0) {
        SpinLockRelease(&worker_head_user->lock);   
        elog(ERROR,"Can not kill workers if not started yet");
    }
    
    int ret = 0;
    for(int r = 0; r < n_workers; r++) {
        ret += kill( worker_head_user->pid[r], SIGTERM);
    }
  
    SpinLockRelease(&worker_head_user->lock);   
    
    PG_RETURN_INT32(ret);
}

PG_FUNCTION_INFO_V1(ms_kill_global_workers);
Datum
ms_kill_global_workers(PG_FUNCTION_ARGS) {
   
    // Get global data structure
    char buf[BGW_MAXLEN];
    snprintf(buf, BGW_MAXLEN, "MW_global"); 
        
    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    bool found;
    worker_data_head* worker_head_global = (worker_data_head*) ShmemInitStruct(buf,
                                sizeof(worker_data_head),
                                &found);
    LWLockRelease(AddinShmemInitLock);
	
    if(!found) {
        worker_head_global->n_workers = 0;
        elog(ERROR,"Can not kill workers if not started yet (shared mem blank)");
    }
    
    SpinLockAcquire(&worker_head_global->lock);
    
    int n_workers = worker_head_global->n_workers;
    
    if(n_workers == 0) {
        SpinLockRelease(&worker_head_global->lock);   
        elog(ERROR,"Can not kill workers if not started yet (n_workers=0)");
    }
    
    int ret = 0;
    for(int r = 0; r < n_workers; r++) {
        if(worker_head_global->pid[r] != 0) {
            int err = kill( worker_head_global->pid[r], SIGTERM);
            if(err == 0) {
                ret +=1;
                worker_head_global->pid[r] = 0;
            } else 
                elog(WARNING,"Global worker %d with pid %d could not be killed (%d)",r,worker_head_global->pid[r],err);
            
        } else 
            elog(WARNING,"Global worker %d with invalid pid",r);
        
    }
    
    worker_head_global->n_workers = 0;
    
    SpinLockRelease(&worker_head_global->lock);   
    
    PG_RETURN_INT32(ret);
}


/*
    Get N attributes at once
*/
void GetNAttributes(HeapTupleHeader tuple,
                int16 N, 
                Datum* datum, bool *isNull, bool *passbyval) 
{
    Oid          tupType;
    int32        tupTypmod;
    TupleDesc    tupDesc;
    HeapTupleData tmptup;

    if (tuple == NULL)
    {
        /* Kinda bogus but compatible with old behavior... */
        *isNull = true;
        return;
    }

    tupType = HeapTupleHeaderGetTypeId(tuple);
    tupTypmod = HeapTupleHeaderGetTypMod(tuple);
    tupDesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
  
    /*
     * heap_getattr needs a HeapTuple not a bare HeapTupleHeader.  We set all
     * the fields in the struct just in case user tries to inspect system
     * columns.
     */
    tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
    ItemPointerSetInvalid(&(tmptup.t_self));
    tmptup.t_tableOid = InvalidOid;
    tmptup.t_data = tuple;
#ifdef PGXC
	tmptup.t_xc_node_id = InvalidOid;
#endif
    
    for(int16 a = 0; a < N; a++) 
    {

#ifdef PGXC
        passbyval[a] = tupDesc->attrs[a]->attbyval;
#else
        passbyval[a] = TupleDescAttr(tupDesc, a)->attbyval;
#endif
        datum[a] = heap_getattr(&tmptup,
                          a+1,
                          tupDesc,
                          &isNull[a]);
    }
    
    ReleaseTupleDesc(tupDesc);
}


/*

    EXPERIMENTAL

*/
PG_FUNCTION_INFO_V1(atest);
Datum
atest(PG_FUNCTION_ARGS) {

    // Call java function
    char* class_name = "ai/sedn/unsupervised/Kmeans";
    char* method_name = "atest";
    char* signature = "([Lai/sedn/unsupervised/TestReturn;)I";
    elog(NOTICE,"A");
    //Datum ret = control_fgworker(fcinfo, false, class_name, method_name, signature, "I");
    Datum ret = control_bgworkers(fcinfo, 1, false, false, class_name, method_name, signature, "I");
    
    elog(NOTICE,"B");

    PG_RETURN_DATUM( ret );      
}

PG_FUNCTION_INFO_V1(rtest);
Datum
rtest(PG_FUNCTION_ARGS) {
 
    // Call java function
    char* class_name = "ai/sedn/unsupervised/Kmeans";
    char* method_name = "rtest";
    char* signature = "()Ljava/util/Iterator;";

    control_fgworker(fcinfo, true, class_name, method_name, signature, "O");

    PG_RETURN_NULL();
}
