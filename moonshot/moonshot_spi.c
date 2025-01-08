#include "moonshot_spi.h"
#include "postgres.h"
#include "executor/spi.h" 
#include "utils/array.h"
#include "math.h"

bool SPI_connected = false;
bool activeSPI = false;

Portal prtl;

double_array_data* A;

Datum* prefetch;
int proc = 0;

row_cache RCACHE;
double_array_data* DOUBLE_ARRAY_CACHE;
float_array_data* FLOAT_ARRAY_CACHE;
char_array_data* CHAR_ARRAY_CACHE;

int connect_SPI() {
    if(!activeSPI) {
        return -1;
    }

    if(!SPI_connected) {
    
        SPI_connect();
        
        // Init
        A = palloc(1*sizeof(double_array_data));
        
        DOUBLE_ARRAY_CACHE = palloc(1*sizeof(double_array_data));
        FLOAT_ARRAY_CACHE = palloc(1*sizeof(float_array_data));
        CHAR_ARRAY_CACHE = palloc(1*sizeof(char_array_data));
        
        proc = 0;
        
        SPI_connected = true;
    }

    return 0;
}

void disconnect_SPI() {
    if(SPI_connected) {
        if(prtl!=NULL) {
            SPI_cursor_close(prtl);
            prtl = NULL;
        }
        if(RCACHE.data != NULL) {
            pfree(RCACHE.data);
            RCACHE.data = NULL;
            RCACHE.pos = -1;
        }     
        if(DOUBLE_ARRAY_CACHE!=NULL) {
            pfree(DOUBLE_ARRAY_CACHE);
            DOUBLE_ARRAY_CACHE = NULL;
        }
        if(FLOAT_ARRAY_CACHE!=NULL) {
            pfree(FLOAT_ARRAY_CACHE);
            FLOAT_ARRAY_CACHE = NULL;
        }
        if(CHAR_ARRAY_CACHE!=NULL) {
            pfree(CHAR_ARRAY_CACHE);
            CHAR_ARRAY_CACHE = NULL;
        }
        
        SPI_finish();
        SPI_connected = false;
    }
}


int execute(char* query, bool use_cursor) {
    
    bool error = false;

    if(SPI_connected) {
        // Close cursor if open
        if(prtl!=NULL) {
            SPI_cursor_close(prtl);
            prtl = NULL;

            //pfree(A);
            if(prefetch!=NULL) {
                pfree(prefetch);
                prefetch = NULL;
            }   
        }

        // Cleanup
        if(RCACHE.data != NULL) {
            pfree(RCACHE.data);
            RCACHE.data = NULL;
            RCACHE.pos = -1;
        }
        
        PG_TRY(); 
        {
            if(use_cursor) {
                SPIPlanPtr plan = SPI_prepare(query, 0, NULL);
                if(SPI_is_cursor_plan(plan)) {
                    prtl = SPI_cursor_open(NULL, plan, NULL, NULL, false);  
                    //elog(WARNING,"Cursor plan (%s) -> prtl",query);      
                } else {
                    int exec = SPI_execute_plan(plan, NULL, NULL, false, 0);  
                    RCACHE.proc = SPI_processed; 
                }
            } else {
                int exec = SPI_execute(query, false, 0);
                RCACHE.proc = SPI_processed;   
                //elog(WARNING,"Non-cursor plan (%s) -> %d -> %d",query,exec,RCACHE.proc);
            }  
        }
        PG_CATCH();
        {
            prtl = NULL;
            error = true;    
            FlushErrorState();
        }
        PG_END_TRY();
    }

    if(error) 
        return -1;
    else 
        return 0 ;
}

bool fetch_next() {
    if(SPI_connected) {
        if(RCACHE.data==NULL || RCACHE.pos == RCACHE.proc-1 || RCACHE.pos==-1) {  
            if(prtl != NULL) {
                /*
                struct timespec start, finish, delta;
                clock_gettime(CLOCK_REALTIME, &start);
                */
                SPI_cursor_fetch(prtl, true, 10000);
                RCACHE.proc = SPI_processed; 
                /*
                clock_gettime(CLOCK_REALTIME, &finish);
                sub_timespec(start, finish, &delta);
                elog(WARNING,"[DEBUG](RT fetch_next): %d.%.9ld (%d)",(int)delta.tv_sec, delta.tv_nsec, SPI_processed);
                */ 
            } else {
                if(RCACHE.pos == RCACHE.proc-1) return false;                
            }
            
            if(RCACHE.proc > 0) {
                
                TupleDesc tupdesc = SPI_tuptable->tupdesc;
                SPITupleTable *tuptable = SPI_tuptable;
                    
                RCACHE.ncols = tupdesc->natts;
                if(RCACHE.data != NULL) {
                    pfree(RCACHE.data);
                }
                RCACHE.data = (Datum*) palloc(RCACHE.proc * RCACHE.ncols * sizeof(Datum));
                
                for(int i = 0; i < RCACHE.proc; i++) {
                    HeapTuple row = tuptable->vals[i];
                    for(int c = 0; c < RCACHE.ncols; c++) {
                        bool isnull;
                        Datum col = SPI_getbinval(row, tupdesc, c+1, &isnull);
                        
                        if(!isnull) {
                            RCACHE.data[i*RCACHE.ncols + c] = col;
                        } else {
                            // ToDo: Setting for null treatment !
                            RCACHE.data[i*RCACHE.ncols + c] = NULL;
                            //elog(WARNING,"NULL DETECTED !");
                        }  
                    }      
                }

                RCACHE.pos = 0;
                return true;
            }    
            
        } else {
            RCACHE.pos++;
            if(RCACHE.pos <= RCACHE.proc-1) {
                return true;
            } 
        }
    }
    RCACHE.pos = -1;
    return false;
}

double getdouble(int column) {
    if(RCACHE.data != NULL && RCACHE.pos > -1 && column > 0 && column <= RCACHE.ncols) {
        return DatumGetFloat8( RCACHE.data[RCACHE.pos*RCACHE.ncols+column-1] );
    }
    return NAN;
}

float getfloat(int column) {
    if(RCACHE.data != NULL && RCACHE.pos > -1 && column > 0 && column <= RCACHE.ncols) {
        return DatumGetFloat4( RCACHE.data[RCACHE.pos*RCACHE.ncols+column-1] );
    }
    return NAN;
}

int getint(int column) {
    if(RCACHE.data != NULL && RCACHE.pos > -1 && column > 0 && column <= RCACHE.ncols) {
        if(RCACHE.data[RCACHE.pos*RCACHE.ncols+column-1] != NULL) {
            return DatumGetInt32( RCACHE.data[RCACHE.pos*RCACHE.ncols+column-1] );
        } else {
            // Return 0 for NULL
            return 0;
        }
    }
    return NAN;
}

long getlong(int column) {
    if(RCACHE.data != NULL && RCACHE.pos > -1 && column > 0 && column <= RCACHE.ncols) {
        return DatumGetInt64( RCACHE.data[RCACHE.pos*RCACHE.ncols+column-1] );
    }
    return NAN;
}

// NOT READY YET <- Need to pipe through varlena structure directly
/*
char_array_data* getstring(int column) {
   if(RCACHE.data != NULL && RCACHE.pos > -1 && column > 0 && column <= RCACHE.ncols) {
 
    	text* txt = DatumGetTextP( RCACHE.data[RCACHE.pos*RCACHE.ncols+column-1] );
        int len = VARSIZE_ANY_EXHDR(txt)+1;
        char t[len];
        text_to_cstring_buffer(txt, &t, len);

        return t;
   }
   return NULL;
}
*/

double_array_data* getdoublearray(int column) { 
    if(RCACHE.data != NULL && RCACHE.pos > -1 && column > 0 && column <= RCACHE.ncols) {
        int pos = RCACHE.pos*RCACHE.ncols+column-1;
        ArrayType* arr = DatumGetArrayTypeP( RCACHE.data[pos] );  
        DOUBLE_ARRAY_CACHE[0].size = (int) ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
        DOUBLE_ARRAY_CACHE[0].arr = (double*) ARR_DATA_PTR(arr);
        return DOUBLE_ARRAY_CACHE;
    } 
    DOUBLE_ARRAY_CACHE[0].arr = NULL;
    DOUBLE_ARRAY_CACHE[0].size = 0;

    return DOUBLE_ARRAY_CACHE;          
}

float_array_data* getvector(int column) { 
    if(RCACHE.data != NULL && RCACHE.pos > -1 && column > 0 && column <= RCACHE.ncols) {
        int pos = RCACHE.pos*RCACHE.ncols+column-1;
        
        Vector* V = (Vector *) PG_DETOAST_DATUM(  RCACHE.data[pos] );

        FLOAT_ARRAY_CACHE[0].size = (int) V->dim;
        FLOAT_ARRAY_CACHE[0].arr = (float*) V->x;
        
        return FLOAT_ARRAY_CACHE;
    } 
    FLOAT_ARRAY_CACHE[0].arr = NULL;
    FLOAT_ARRAY_CACHE[0].size = 0;

    return FLOAT_ARRAY_CACHE;          
}




double_array_data* fetch_next_double_array(int column) { 
    if(SPI_connected) {
        if(prefetch==NIL) {  
            SPI_cursor_fetch(prtl, true, 10000);
            proc = SPI_processed; 
            if(proc > 0) {
                prefetch = palloc(proc*sizeof(Datum));
                
                TupleDesc tupdesc = SPI_tuptable->tupdesc;
                SPITupleTable *tuptable = SPI_tuptable;
        
                for(int i = 0; i < proc; i++) {
                    HeapTuple row = tuptable->vals[proc-i-1];
                    bool isnull;
                    Datum col = SPI_getbinval(row, tupdesc, column, &isnull);
                    
                    if(!isnull) {
                        prefetch[i] = col;
                    }
                }
            }
        }

        proc--;

        if(proc >= 0) {
            Datum col = prefetch[proc];

            if(proc==0) {
                if(prefetch != NULL) {
                    pfree(prefetch);
                    prefetch = NULL;
                }
            }

            if(col != NIL) {
                ArrayType* arr = DatumGetArrayTypeP(col);  
                
                A[0].size = (int) ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
                A[0].arr = (double*) ARR_DATA_PTR(arr);     
            
                return A;
            }    
        } 
    }
    A[0].arr = NULL;
    A[0].size = 0;
    
    return A;
}
