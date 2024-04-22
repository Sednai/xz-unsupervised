#include "pg_config.h"
#include "pg_config_manual.h"

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

#include "moonshot.h"
#include "moonshot_jvm.h"
#include "moonshot_spi.h"

#include "miscadmin.h"
#include "pgstat.h"

worker_data_head *worker_head = NULL;

enum { NS_PER_SECOND = 1000000000 };


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

PG_MODULE_MAGIC;


/* 
    SPI
*/
/*
DEPRECATED

double_array_data* fetch_data() {
    

    SPI_connect();
    SPIPlanPtr plan = SPI_prepare_cursor("select attrs from lorenzo_v3 limit 100000", 0, NULL, 0);
    
    Portal ptl = SPI_cursor_open(NULL, plan, NULL, NULL,true);
    int total = 0;
    int proc = 0;
    
    struct timespec start, finish, delta;
    clock_gettime(CLOCK_REALTIME, &start);
    
    double* values; 
    //struct array_data* A = palloc(1*sizeof(struct array_data));
    double_array_data* A = palloc(1*sizeof(double_array_data));

    do {
        SPI_cursor_fetch(ptl, true, 100000);
        proc = SPI_processed; 
        
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        for (int j = 0; j < proc; j++)
        {
            HeapTuple row = tuptable->vals[j];
            //Datum datumrow = PointerGetDatum(SPI_returntuple(row, tupdesc) );
            bool isnull;
            Datum col = SPI_getbinval(row, tupdesc, 1, &isnull);

            if(~isnull) {
                ArrayType* arr = DatumGetArrayTypeP(col);  
                int nElems = (int) ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
                values = (double*) ARR_DATA_PTR(arr);

                A[0].arr = values;
                A[0].size = nElems;
            
            }
        }


        total += proc;
    } while(proc > 0);

    struct timespec start, finish, delta;
    clock_gettime(CLOCK_REALTIME, &start);
    
	clock_gettime(CLOCK_REALTIME, &finish);
    sub_timespec(start, finish, &delta);
	elog(WARNING,"[DEBUG](RT): %d.%.9ld (%d)",(int)delta.tv_sec, delta.tv_nsec, total);

    SPI_cursor_close(ptl);
    
    SPI_finish();

	elog(WARNING,"[DEBUG](RT): %d.%.9ld (%d)",(int)delta.tv_sec, delta.tv_nsec, total);
    
    return A;
}
*/

PG_FUNCTION_INFO_V1(kmeans_gradients_cpu_float);
Datum
kmeans_gradients_cpu_float(PG_FUNCTION_ARGS) 
{
    if(worker_head == NULL) {
        worker_head = launch_dynamic_workers(8, true, false);
        pg_usleep(5000L);		/* 5msec */
    } 
    
    TupleDesc tupdesc; 
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("function returning record called in context "
                            "that cannot accept type record")));

    tupdesc = BlessTupleDesc(tupdesc);
    int natts = tupdesc->natts;
    Datum values[natts];
    bool* nulls = palloc0( natts * sizeof( bool ) );
   
    SpinLockAcquire(&worker_head->lock);
    /*
        Lock acquired
    */      
    if(!dlist_is_empty(&worker_head->free_list)) {
        dlist_node* dnode = dlist_pop_head_node(&worker_head->free_list);
        worker_exec_entry* entry = dlist_container(worker_exec_entry, node, dnode);

        char* class_name = "ai/sedn/unsupervised/Kmeans";
        //char* method_name = "background_test";
        //char* signature = "()Lai/sedn/unsupervised/GradientReturn;";
        char* method_name = "kmeans_gradients_cpu_float_ms";
        char* signature = "(Ljava/lang/String;Ljava/lang/String;IF[F)Lai/sedn/unsupervised/GradientReturn;";
        char* return_type = "O";
        
        strncpy(entry->class_name, class_name, strlen(class_name)+1);
        strncpy(entry->method_name, method_name, strlen(method_name)+1);
        strncpy(entry->signature, signature, strlen(signature)+1);
        strncpy(entry->return_type, return_type, 1);
   
        entry->n_return = natts;
        entry->notify_latch = MyLatch;
        
        // Serialize args
        entry->n_args = 5;
        //entry->n_args = 0;

        char* pos = entry->data;
        
        elog(WARNING,"entry->data: before S %d",(int) pos);

        strncpy(pos, "Ljava/lang/String;", strlen("Ljava/lang/String;")+1); // 18+1
        pos+=strlen("Ljava/lang/String;")+1;
        elog(WARNING,"entry->data: after S head %d",(int) pos);

        datumSerialize(PG_GETARG_DATUM(0), false, false, -1, &pos);
        elog(WARNING,"entry->data: before S %d",(int) pos);
       
        strncpy(pos, "Ljava/lang/String;", strlen("Ljava/lang/String;")+1);
        pos+=strlen("Ljava/lang/String;")+1;
        elog(WARNING,"entry->data: after S head %d",(int) pos);

        datumSerialize(PG_GETARG_DATUM(1), false, false, -1, &pos);

        elog(WARNING,"entry->data: before I %d",(int) pos);

        strncpy(pos, "I", strlen("I")+1);
        pos+=strlen("I")+1;
        
        elog(WARNING,"entry->data: after I head %d",(int) pos);

        datumSerialize(PG_GETARG_DATUM(2), false, true, -1, &pos);
       
        elog(WARNING,"entry->data: before F %d",(int) pos);

        strncpy(pos, "F", strlen("F")+1);
        pos+=strlen("F")+1;
        
        elog(WARNING,"entry->data: after F head %d",(int) pos);

        datumSerialize(PG_GETARG_DATUM(3), false, true, -1, &pos);
       
        elog(WARNING,"entry->data: before [F %d",(int) pos);

        strncpy(pos, "[F", strlen("[F")+1);
        pos+=strlen("[F")+1;

        elog(WARNING,"entry->data: after F head %d",(int) pos);

        datumSerialize(PG_GETARG_DATUM(4), false, false, -1, &pos);
       
        elog(WARNING,"entry->data: final %d, %d",(int) pos, (int) entry->data);
        

        // Push
        dlist_push_tail(&worker_head->exec_list,&entry->node);
    
        elog(WARNING,"TEST: %d",entry->taskid);
    
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
                    char buf[2048];
                    strncpy(buf, entry->data, 2048);
                    entry->error = false;

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

                HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
            
                pfree(nulls);
                PG_RETURN_DATUM( HeapTupleGetDatum(tuple ));    
            }
        }

    } else {
        SpinLockRelease(&worker_head->lock);
        pfree(nulls);
        elog(ERROR,"QUEUE is full");
    } 
}

jvalue PG_text_to_jvalue(text* txt) {
    jvalue val;
    int len = VARSIZE_ANY_EXHDR(txt)+1;
    char t[len];
    text_to_cstring_buffer(txt, &t, len);
    val.l = (*jenv)->NewStringUTF(jenv, t);

    return val;
}


/* <- NON BACKGROUND WORKER BASED ! */
PG_FUNCTION_INFO_V1(dbscan_batch);
Datum
dbscan_batch(PG_FUNCTION_ARGS) 
{
    TupleDesc tupdesc; 
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("function returning record called in context "
                            "that cannot accept type record")));

    tupdesc = BlessTupleDesc(tupdesc);
    int natts = tupdesc->natts;
    Datum values[natts];
    bool* nulls = palloc0( natts * sizeof( bool ) );
   
    // Start JVM
    if(jenv == NIL) {
        startJVM();
    }

    char* class_name = "ai/sedn/unsupervised/DBscan";
    char* method_name = "dbscan_batch_ms";
    char* signature = "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;IFIJI)Lai/sedn/unsupervised/dbscan_batch_ret;";
    char* return_type = "O";
    
    // Prep arguments
    jvalue args[9];
	
    args[0] = (jvalue) PG_text_to_jvalue( DatumGetTextP( PG_GETARG_DATUM(0) ));
    args[1] = (jvalue) PG_text_to_jvalue( DatumGetTextP( PG_GETARG_DATUM(1) ));
    args[2] = (jvalue) PG_text_to_jvalue( DatumGetTextP( PG_GETARG_DATUM(2) ));
    args[3] = (jvalue) PG_text_to_jvalue( DatumGetTextP( PG_GETARG_DATUM(3) ));
    args[4].i = (jint) PG_GETARG_INT32(4);     
    args[5].f = (jfloat) (float) PG_GETARG_FLOAT8(5);
    args[6].i = (jint) PG_GETARG_INT32(6);   
    args[7].j = (jlong) PG_GETARG_INT64(7);     
    args[8].i = (jint) PG_GETARG_INT32(8);   

    // Call java function
    bool primitive[natts];

    activeSPI = true;
    connect_SPI();
    PushActiveSnapshot(GetTransactionSnapshot());

    int jfr = call_java_function(values, primitive, class_name, method_name, signature, return_type, &args);

    disconnect_SPI();
    PopActiveSnapshot();

    if( jfr != 0 ) {
        jthrowable exh = (*jenv)->ExceptionOccurred(jenv);
			
        // Clear exception
        (*jenv)->ExceptionClear(jenv);
			
        if(exh !=0) {
            jclass Throwable_class = (*jenv)->FindClass(jenv, "java/lang/Throwable");
            jmethodID Throwable_getMessage =  (*jenv)->GetMethodID(jenv,Throwable_class, "getMessage", "()Ljava/lang/String;");

            // Get error msg			
            jstring jmsg = (jstring)(*jenv)->CallObjectMethod(jenv, exh, Throwable_getMessage);
	
            const char* msg = (*jenv)->GetStringUTFChars(jenv, jmsg, false);

            elog(ERROR,"Java exception: %s",msg);
				
        } else {
             elog(ERROR,"Unknown Java exception occured (%d)",jfr);
        }	
    }

    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
            
    pfree(nulls);
    PG_RETURN_DATUM( HeapTupleGetDatum(tuple )); 

}


/* <= PROBLEM WITH SPI AND BACKGROUND WORKER ON COORDINATOR ! 
PG_FUNCTION_INFO_V1(dbscan_batch);
Datum
dbscan_batch(PG_FUNCTION_ARGS) 
{
    if(worker_head == NULL) {
        worker_head = launch_dynamic_workers(2, true, false);
        pg_usleep(5000L);	
    } 
    
    TupleDesc tupdesc; 
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("function returning record called in context "
                            "that cannot accept type record")));

    tupdesc = BlessTupleDesc(tupdesc);
    int natts = tupdesc->natts;
    Datum values[natts];
    bool* nulls = palloc0( natts * sizeof( bool ) );
   
    SpinLockAcquire(&worker_head->lock);
    
    if(!dlist_is_empty(&worker_head->free_list)) {
        dlist_node* dnode = dlist_pop_head_node(&worker_head->free_list);
        worker_exec_entry* entry = dlist_container(worker_exec_entry, node, dnode);

        char* class_name = "ai/sedn/unsupervised/DBscan";
        char* method_name = "dbscan_batch_ms";
        char* signature = "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;IFIJI)Lai/sedn/unsupervised/dbscan_batch_ret;";
        char* return_type = "O";
        
        strncpy(entry->class_name, class_name, strlen(class_name)+1);
        strncpy(entry->method_name, method_name, strlen(method_name)+1);
        strncpy(entry->signature, signature, strlen(signature)+1);
        strncpy(entry->return_type, return_type, 1);
   
        entry->n_return = natts;
        entry->notify_latch = MyLatch;
        
        // Serialize args
        entry->n_args = 9;
        
        char* pos = entry->data;
        
        strncpy(pos, "Ljava/lang/String;", strlen("Ljava/lang/String;")+1); // 18+1
        pos+=strlen("Ljava/lang/String;")+1;
        datumSerialize(PG_GETARG_DATUM(0), false, false, -1, &pos);
        
        strncpy(pos, "Ljava/lang/String;", strlen("Ljava/lang/String;")+1);
        pos+=strlen("Ljava/lang/String;")+1;
        datumSerialize(PG_GETARG_DATUM(1), false, false, -1, &pos);

        strncpy(pos, "Ljava/lang/String;", strlen("Ljava/lang/String;")+1);
        pos+=strlen("Ljava/lang/String;")+1;
        datumSerialize(PG_GETARG_DATUM(2), false, false, -1, &pos);

        strncpy(pos, "Ljava/lang/String;", strlen("Ljava/lang/String;")+1);
        pos+=strlen("Ljava/lang/String;")+1;
        datumSerialize(PG_GETARG_DATUM(3), false, false, -1, &pos);

        strncpy(pos, "I", strlen("I")+1);
        pos+=strlen("I")+1;
        datumSerialize(PG_GETARG_DATUM(4), false, true, -1, &pos);

        strncpy(pos, "F", strlen("F")+1);
        pos+=strlen("F")+1;
        datumSerialize(PG_GETARG_DATUM(5), false, true, -1, &pos);

        strncpy(pos, "I", strlen("I")+1);
        pos+=strlen("I")+1;
        datumSerialize(PG_GETARG_DATUM(6), false, true, -1, &pos);

        strncpy(pos, "J", strlen("J")+1);
        pos+=strlen("J")+1;
        datumSerialize(PG_GETARG_DATUM(7), false, true, -1, &pos);

        strncpy(pos, "I", strlen("I")+1);
        pos+=strlen("I")+1;
        datumSerialize(PG_GETARG_DATUM(8), false, true, -1, &pos);

        // Push
        dlist_push_tail(&worker_head->exec_list,&entry->node);

        for(int w = 0; w < worker_head->n_workers; w++) {
            SetLatch( worker_head->latch[w] );
        }

        SpinLockRelease(&worker_head->lock);
        
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
                    char buf[2048];
                    strncpy(buf, entry->data, 2048);
                    entry->error = false;

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

                HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
            
                pfree(nulls);
                PG_RETURN_DATUM( HeapTupleGetDatum(tuple ));    
            }
        }

    } else {
        SpinLockRelease(&worker_head->lock);
        pfree(nulls);
        elog(ERROR,"QUEUE is full");
    } 
}
*/


/*
    Note: In future uniformize with equal code above or write general auto argument inference code
*/
PG_FUNCTION_INFO_V1(kmeans_gradients_tvm_float);
Datum
kmeans_gradients_tvm_float(PG_FUNCTION_ARGS) 
{
    if(worker_head == NULL) {
        worker_head = launch_dynamic_workers(8, true, false);
        pg_usleep(5000L);		/* 5msec */
    } 
    
    TupleDesc tupdesc; 
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("function returning record called in context "
                            "that cannot accept type record")));

    tupdesc = BlessTupleDesc(tupdesc);
    int natts = tupdesc->natts;
    Datum values[natts];
    bool* nulls = palloc0( natts * sizeof( bool ) );
   
    SpinLockAcquire(&worker_head->lock);
    /*
        Lock acquired
    */      
    if(!dlist_is_empty(&worker_head->free_list)) {
        dlist_node* dnode = dlist_pop_head_node(&worker_head->free_list);
        worker_exec_entry* entry = dlist_container(worker_exec_entry, node, dnode);

        char* class_name = "ai/sedn/unsupervised/Kmeans";
        //char* method_name = "background_test";
        //char* signature = "()Lai/sedn/unsupervised/GradientReturn;";
        char* method_name = "kmeans_gradients_tvm_float_ms";
        char* signature = "(Ljava/lang/String;Ljava/lang/String;IFI[F)Lai/sedn/unsupervised/GradientReturn;";
        char* return_type = "O";

        strncpy(entry->class_name, class_name, strlen(class_name)+1);
        strncpy(entry->method_name, method_name, strlen(method_name)+1);
        strncpy(entry->signature, signature, strlen(signature)+1);
        strncpy(entry->return_type, return_type, 1);
   
        entry->n_return = natts;
        entry->notify_latch = MyLatch;
        
        // Serialize args
        entry->n_args = 6;
        //entry->n_args = 0;

        char* pos = entry->data;
        
        elog(WARNING,"entry->data: before S %d",(int) pos);

        strncpy(pos, "Ljava/lang/String;", strlen("Ljava/lang/String;")+1); // 18+1
        pos+=strlen("Ljava/lang/String;")+1;
        elog(WARNING,"entry->data: after S head %d",(int) pos);

        datumSerialize(PG_GETARG_DATUM(0), false, false, -1, &pos);
        elog(WARNING,"entry->data: before S %d",(int) pos);
       
        strncpy(pos, "Ljava/lang/String;", strlen("Ljava/lang/String;")+1);
        pos+=strlen("Ljava/lang/String;")+1;
        elog(WARNING,"entry->data: after S head %d",(int) pos);

        datumSerialize(PG_GETARG_DATUM(1), false, false, -1, &pos);

        elog(WARNING,"entry->data: before I %d",(int) pos);

        strncpy(pos, "I", strlen("I")+1);
        pos+=strlen("I")+1;
        
        elog(WARNING,"entry->data: after I head %d",(int) pos);

        datumSerialize(PG_GETARG_DATUM(2), false, true, -1, &pos);
       
        elog(WARNING,"entry->data: before F %d",(int) pos);

        strncpy(pos, "F", strlen("F")+1);
        pos+=strlen("F")+1;
        
        elog(WARNING,"entry->data: after F head %d",(int) pos);

        datumSerialize(PG_GETARG_DATUM(3), false, true, -1, &pos);
       
        

        elog(WARNING,"entry->data: before I %d",(int) pos);

        strncpy(pos, "I", strlen("I")+1);
        pos+=strlen("I")+1;
        
        elog(WARNING,"entry->data: after I head %d",(int) pos);

        datumSerialize(PG_GETARG_DATUM(4), false, true, -1, &pos);
       
        elog(WARNING,"entry->data: before [F %d",(int) pos);

        strncpy(pos, "[F", strlen("[F")+1);
        pos+=strlen("[F")+1;

        elog(WARNING,"entry->data: after F head %d",(int) pos);

        datumSerialize(PG_GETARG_DATUM(5), false, false, -1, &pos);
       
        elog(WARNING,"entry->data: final %d, %d",(int) pos, (int) entry->data);
        

        // Push
        dlist_push_tail(&worker_head->exec_list,&entry->node);
    
        elog(WARNING,"TEST: %d",entry->taskid);
    
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
                    char buf[2048];
                    strncpy(buf, entry->data, 2048);
                    
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

                HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
            
                pfree(nulls);
                PG_RETURN_DATUM( HeapTupleGetDatum(tuple ));    
            }
        }

    } else {
        SpinLockRelease(&worker_head->lock);
        pfree(nulls);
        elog(ERROR,"QUEUE is full");
    }
   
}

/*
    Note: Test-wise period search
*/
PG_FUNCTION_INFO_V1(psearch);
Datum
psearch(PG_FUNCTION_ARGS) 
{
    char* class_name = "gaia/cu7/algo/character/periodsearch/AEROInterface";
    char* method_name = "DoPeriodSearch";
    
    char* signature = "(J[D[D[D)Lgaia/cu7/algo/character/periodsearch/PeriodResult;";
    char* return_type = "O";

    Datum ret = control_bgworkers(fcinfo, 2, false, true, class_name, method_name, signature, return_type);

    PG_RETURN_DATUM( ret );   
}

PG_FUNCTION_INFO_V1(psearch_ms_gpu);
Datum
psearch_ms_gpu(PG_FUNCTION_ARGS) 
{
    char* class_name = "gaia/cu7/algo/character/periodsearch/AEROInterface";
    char* method_name = "DoPeriodSearchGPU";
    
    char* signature = "(J[D[D[D)Lgaia/cu7/algo/character/periodsearch/PeriodResult;";
    char* return_type = "O";

    Datum ret = control_bgworkers(fcinfo, 2, false, true, class_name, method_name, signature, return_type);

    PG_RETURN_DATUM( ret );   
}

/*
    Main function to deliver tasks to bg workers and collect results
*/
Datum control_bgworkers(FunctionCallInfo fcinfo, int n_workers, bool need_SPI, bool globalWorker, char* class_name, char* method_name, char* signature, char* return_type) {

    // Start workers if not started yet
    if(worker_head == NULL) {
        worker_head = launch_dynamic_workers(2, false, true);
        pg_usleep(5000L);		/* 5msec */
    } 

    // Prepare return tuple
    TupleDesc tupdesc; 
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("function returning record called in context "
                            "that cannot accept type record")));

    tupdesc = BlessTupleDesc(tupdesc);
    int natts = tupdesc->natts;
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
        
        entry->n_args = argSerializer(entry->data, signature, &fcinfo->arg );
    
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
                    char buf[2048];
                    strncpy(buf, entry->data, 2048);
                    
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

                HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
            
                pfree(nulls);
                
                PG_RETURN_DATUM( HeapTupleGetDatum(tuple ));    
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
            if( ( (!openo || !opensb) && (signature[i] == '[' || signature[i] == 'L'))   ) {
                // Buffer 
                *target = signature[i];
                target++;

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
                    *target = signature[i];
                    target++;
                } else {
                    // Serialize object argument
                    *target = '\0';
                    target++;
                    datumSerialize(args[ac], false, false, -1, &target);
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

                    // Serialize array argument
                    target[0] = signature[i];
                    target[1] = '\0';
                    target+=2;
                    datumSerialize(args[ac], false, false, -1, &target);
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


PG_FUNCTION_INFO_V1(moonshot_clear_queue);
Datum
moonshot_clear_queue(PG_FUNCTION_ARGS) {
    if(worker_head == NULL) {
        Oid			roleid = GetUserId();
	    Oid			dbid = MyDatabaseId;
	
        char buf[12];
        snprintf(buf, 12, "MW_%d_%d", roleid, dbid); 
        
        bool found = false;
        worker_head = ShmemInitStruct(buf,
								   sizeof(worker_data_head),
								   &found);
        if(!found) {
            worker_head->n_workers = 0;
        }
    }

    if(worker_head->n_workers == 0) {
        elog(ERROR,"Can not restart workers if not started yet");
    }

    SpinLockAcquire(&worker_head->lock);
    int c = 0;
    while(!dlist_is_empty(&worker_head->exec_list)) {
        dlist_node* dnode = dlist_pop_head_node(&worker_head->exec_list);
        dlist_push_tail(&worker_head->free_list,dnode);
        c++;
    }
    
    SpinLockRelease(&worker_head->lock);   

    PG_RETURN_INT32(c);
}

PG_FUNCTION_INFO_V1(moonshot_show_queue);
Datum
moonshot_show_queue(PG_FUNCTION_ARGS) {
    
    if(worker_head == NULL) {
        Oid			roleid = GetUserId();
	    Oid			dbid = MyDatabaseId;
	
        char buf[12];
        snprintf(buf, 12, "MW_%d_%d", roleid, dbid); 
        
        bool found = false;
        worker_head = ShmemInitStruct(buf,
								   sizeof(worker_data_head),
								   &found);
        if(!found) {
            worker_head->n_workers = 0;
        }
    } 

    if(worker_head->n_workers == 0) {
        elog(ERROR,"Can not restart workers if not started yet");
    }

    SpinLockAcquire(&worker_head->lock);
    
    int c = 0;
    dlist_iter iter;
    
    dlist_foreach(iter, &worker_head->exec_list) {
        c++;
    }
    
    SpinLockRelease(&worker_head->lock);   

    PG_RETURN_INT32(c);
}

PG_FUNCTION_INFO_V1(moonshot_restart_workers);
Datum
moonshot_restart_workers(PG_FUNCTION_ARGS) {
   
    // Get global data structure
    if(worker_head == NULL) {
        Oid			roleid = GetUserId();
	    Oid			dbid = MyDatabaseId;
	
        char buf[12];
        snprintf(buf, 12, "MW_%d_%d", roleid, dbid); 
        
        bool found = false;
        worker_head = ShmemInitStruct(buf,
								   sizeof(worker_data_head),
								   &found);
        if(!found) {
            worker_head->n_workers = 0;
        }
    }

    SpinLockAcquire(&worker_head->lock);
    int n_workers = worker_head->n_workers;
    int pid = worker_head->pid[0];
    SpinLockRelease(&worker_head->lock);   

    if(n_workers == 0) {
        elog(ERROR,"Can not restart workers if not started yet");
    }
         
    int r = kill(pid, SIGTERM);
    
    PG_RETURN_INT32(r);
}


/*
DEPRECATED

PG_FUNCTION_INFO_V1(kmeans);

Datum
kmeans(PG_FUNCTION_ARGS)
{
    
    // Prep return
    TupleDesc tupdesc; 
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context "
                            "that cannot accept type record")));
  
    tupdesc = BlessTupleDesc(tupdesc);
    int natts = tupdesc->natts;
    Datum values[natts];
    bool primitive[natts];
    bool* nulls = palloc0( natts * sizeof( bool ) );
   
    // Start JVM
    if(jenv == NIL) {
        startJVM();
    }

    // Prep args
    jfloat batch_percent = PG_GETARG_FLOAT4(0);
    ArrayType* v = DatumGetArrayTypeP(PG_GETARG_DATUM(1));
   
    jsize      nElems = (jsize)ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));
	
    jfloatArray floatArray = (*jenv)->NewFloatArray(jenv,nElems);
    (*jenv)->SetFloatArrayRegion(jenv,floatArray, 0, nElems, (jfloat *)ARR_DATA_PTR(v));
    
    // Need to get Text -> String
    
    // Call function
    //call_java_function(values, primitive, "ai/sedn/unsupervised/Kmeans", "kmeans_gradients_cpu_float_test", "(F[F)Lai/sedn/unsupervised/GradientReturn;",batch_percent,floatArray);


    // Build return tuple

    //Datum values[2];// = palloc(2 * sizeof(Datum));
    	
    //values[0] = Int32GetDatum(1);
    //values[1] = Int32GetDatum(2);
    
   
    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
    
    pfree(nulls);


    fetch_data();

    PG_RETURN_DATUM( HeapTupleGetDatum(tuple ));
}

*/

