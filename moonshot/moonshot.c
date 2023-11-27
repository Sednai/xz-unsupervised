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

typedef jint(JNICALL *JNI_CreateJavaVM_func)(JavaVM **pvm, void **penv, void *args);

JNIEnv *jenv;

enum { NS_PER_SECOND = 1000000000 };

struct array_data {
    double* arr;
    int size;
};

Portal prtl;
struct array_data* A;

Datum* prefetch;
int proc = 0;

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


void connect_SPI() {
    elog(WARNING,"Connecting SPI");
    
    SPI_connect();
    A = palloc(1*sizeof(struct array_data));
}

void disconnect_SPI() {
    elog(WARNING,"Disconnecting SPI");
    
    if(prtl!=NULL) {
        SPI_cursor_close(prtl);
        prtl = NULL;
    }
    pfree(A);
    SPI_finish();
}

void execute(char* query) {
    elog(WARNING,"Execute");

    // Close cursor if open
    if(prtl!=NULL) {
        SPI_cursor_close(prtl);
    }

    SPIPlanPtr plan = SPI_prepare_cursor(query, 0, NULL, 0);
    
    prtl = SPI_cursor_open(NULL, plan, NULL, NULL, true);  
}

struct array_data* fetch_next_double_array() {
    
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
                Datum col = SPI_getbinval(row, tupdesc, 1, &isnull);
                
                if(~isnull) {
                    prefetch[i] = col;
                }
            }
        }
    }

    proc--;

    if(proc >= 0) {
        Datum col = prefetch[proc];

        if(proc==0) {
            pfree(prefetch);
            prefetch = NULL;
            proc = 0;
        }

        if(col != NIL) {
                    ArrayType* arr = DatumGetArrayTypeP(col);  
                    
                    A[0].size = (int) ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
                    A[0].arr = (double*) ARR_DATA_PTR(arr);     
                
                    return A;
                } 
       
    } 
    
    A[0].arr = NULL;
    A[0].size = 0;
        
    return A;
}

/* 
    SPI
*/
struct array_data* fetch_data() {
    

/*
    int ret = SPI_execute("select attrs from lorenzo_v3 tablesample system(50)", true, 0);
    int proc = SPI_processed;
*/
    SPI_connect();
    SPIPlanPtr plan = SPI_prepare_cursor("select attrs from lorenzo_v3 limit 100000", 0, NULL, 0);
    
    Portal ptl = SPI_cursor_open(NULL, plan, NULL, NULL,true);
    int total = 0;
    int proc = 0;
    
    struct timespec start, finish, delta;
    clock_gettime(CLOCK_REALTIME, &start);
    
    double* values; 
    struct array_data* A = palloc(1*sizeof(struct array_data));

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

	clock_gettime(CLOCK_REALTIME, &finish);
    sub_timespec(start, finish, &delta);

    SPI_cursor_close(ptl);
    
    SPI_finish();

	elog(WARNING,"[DEBUG](RT): %d.%.9ld (%d)",(int)delta.tv_sec, delta.tv_nsec, total);
    
    return A;
}

//Use PG hashtable instead ?!
char* convert_name_to_JNI_signature(char* name) {
   
    // Native
    if (strcmp(name, "int") == 0) {
        return "I";
    } else if (strcmp(name, "double") == 0) {
        return "D";
    } else if (strcmp(name, "float") == 0) {
        return "F";
    } 
    // Arrays
    else if (strcmp(name, "[F") == 0) {
        return name;
    } else if (strcmp(name, "[[F") == 0) {
        return name;
    } else if (strcmp(name, "[I") == 0) {
        return name;
    } 

    elog(ERROR,"Unsupported Java type: %s",name);
    return NULL;
}

ArrayType* createArray(jsize nElems, size_t elemSize, Oid elemType, bool withNulls)
{
	ArrayType* v;
	Size nBytes = elemSize * nElems;
	//MemoryContext currCtx = Invocation_switchToUpperContext();

	Size dataoffset;
	if(withNulls)
	{
		dataoffset = ARR_OVERHEAD_WITHNULLS(1, nElems);
		nBytes += dataoffset;
	}
	else
	{
		dataoffset = 0;			/* marker for no null bitmap */
		nBytes += ARR_OVERHEAD_NONULLS(1);
	}

	v = (ArrayType*)palloc0(nBytes);
	AssertVariableIsOfType(v->dataoffset, int32);
	v->dataoffset = (int32)dataoffset;
	//MemoryContextSwitchTo(currCtx);

	SET_VARSIZE(v, nBytes);
	ARR_NDIM(v) = 1;
	ARR_ELEMTYPE(v) = elemType;
	*((int*)ARR_DIMS(v)) = nElems;
	*((int*)ARR_LBOUND(v)) = 1;

	return v;
}

ArrayType* create2dArray(jsize dim1, jsize dim2, size_t elemSize, Oid elemType, bool withNulls)
{
	ArrayType* v;
	jsize nElems = dim1*dim2;
	Size nBytes = nElems * elemSize;
	
	Size dataoffset;
	if(withNulls)
	{
		dataoffset = ARR_OVERHEAD_WITHNULLS(dim1, nElems);
		nBytes += dataoffset;
	}
	else
	{
		dataoffset = 0;			/* marker for no null bitmap */
		nBytes += ARR_OVERHEAD_NONULLS(dim1);
	}
	v = (ArrayType*)palloc0(nBytes);
	AssertVariableIsOfType(v->dataoffset, int32);
	v->dataoffset = (int32)dataoffset;
	
	SET_VARSIZE(v, nBytes);

	ARR_NDIM(v) = 2;
	ARR_ELEMTYPE(v) = elemType;
	ARR_DIMS(v)[0] = dim1;
	ARR_DIMS(v)[1] = dim2;
	ARR_LBOUND(v)[0] = 1;
	ARR_LBOUND(v)[1] = 1;
	
	return v;
}

// Build function hash table instead ?
Datum build_datum_from_return_field(jobject data, jclass cls, char* fieldname, char* sig) {
    jfieldID fid = (*jenv)->GetFieldID(jenv,cls,fieldname,sig);       
    
    // Natives
    if (strcmp(sig, "I") == 0) {
        return Int32GetDatum(  (*jenv)->GetIntField(jenv,data,fid) );
    } else if (strcmp(sig, "D") == 0) {
        return Float8GetDatum(  (*jenv)->GetDoubleField(jenv,data,fid) );
    } else if (strcmp(sig, "F") == 0) {
        return Float4GetDatum(  (*jenv)->GetFloatField(jenv,data,fid) );
    }
    // Native arrays
    else if (strcmp(sig, "[F") == 0) {
        jfloatArray arr = (jfloatArray) (*jenv)->GetObjectField(jenv,data,fid);
        int nElems = (*jenv)->GetArrayLength(jenv, arr); 
        ArrayType* v = createArray(nElems, sizeof(jfloat), FLOAT4OID, false);
		(*jenv)->GetFloatArrayRegion(jenv,arr, 0, nElems, (jfloat*)ARR_DATA_PTR(v));

        return PointerGetDatum(v); 

    } else if (strcmp(sig, "[I") == 0) {
        jarray arr = (jarray) (*jenv)->GetObjectField(jenv,data,fid);
        int nElems = (*jenv)->GetArrayLength(jenv, arr); 
        ArrayType* v = createArray(nElems, sizeof(jint), INT4OID, false);
		(*jenv)->GetIntArrayRegion(jenv,arr, 0, nElems, (jint*)ARR_DATA_PTR(v));

        return PointerGetDatum(v);

    }  else if (strcmp(sig, "[[F") == 0) {
       
        jarray arr = (jarray) (*jenv)->GetObjectField(jenv,data,fid);
        int nElems = (*jenv)->GetArrayLength(jenv, arr); 
      
        jfloatArray arr0 = (jfloatArray) (*jenv)->GetObjectArrayElement(jenv,arr,0); 
        jsize dim2 =  (*jenv)->GetArrayLength(jenv, arr0); 
 
		ArrayType* v = create2dArray(nElems, dim2, sizeof(jfloat), FLOAT4OID, false);

		// Copy first dim
		(*jenv)->GetFloatArrayRegion(jenv, arr0, 0, dim2, (jfloat*)ARR_DATA_PTR(v));
		 
        // Copy remaining
		for(int i = 1; i < nElems; i++) {
			jfloatArray els =  (jfloatArray) (*jenv)->GetObjectArrayElement(jenv,arr,i); 
			(*jenv)->GetFloatArrayRegion(jenv, els, 0, dim2,  (jfloat*) (ARR_DATA_PTR(v)+i*dim2*sizeof(jfloat)) );
		}
     
       return PointerGetDatum(v);
    }

    
    elog(ERROR,"Unsupported Java signature: %s",sig);
    return NULL;
}
// ToDo: Arguments !
// Idea: Pass pointer to struct and build signature ? 
void call_java_function(Datum* values, char* class_name, char* method_name, char* signature, ...) {

    // Prep and call function
    jclass clazz = (*jenv)->FindClass(jenv, class_name);

    if(clazz == NULL) {
        elog(ERROR,"Java class %s not found !",class_name);
    }

    jmethodID methodID = (*jenv)->GetStaticMethodID(jenv, clazz, method_name, signature);

    if(methodID == NULL) {
        elog(ERROR,"Java method %s with signature %s not found !",method_name, signature);
    }
    	va_list args;
	va_start(args, signature);
    jobject ret = (*jenv)->CallStaticObjectMethodV(jenv, clazz, methodID, args);
    va_end(args);

    // Analyis return
    jclass cls = (*jenv)->GetObjectClass(jenv, ret);
    
    // Cache ret info -> Datum mapping ?

    jmethodID getFields = (*jenv)->GetMethodID(jenv, (*jenv)->GetObjectClass(jenv,cls), "getFields", "()[Ljava/lang/reflect/Field;");
    
    jobjectArray fieldsList = (jobjectArray)  (*jenv)->CallObjectMethod(jenv, cls, getFields); 
    
    jsize len =  (*jenv)->GetArrayLength(jenv,fieldsList);

    // Check consistency
    //... (match of length)


    if(len == 1) {
        // Detect if iterator is present
        // ... -> Set return
        
        // Single return 
        // ...

       
       
    } else {
        // Composite return
        for(int i = 0; i < len; i++) {
            
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
            jstr = (jstring)(*jenv)->CallObjectMethod(jenv, value, m);
            char* typename =  (*jenv)->GetStringUTFChars(jenv, jstr, false);
        
            char* sig = convert_name_to_JNI_signature(typename);

            jfieldID fid = (*jenv)->GetFieldID(jenv,cls,fieldname,sig);       
            jint test = (*jenv)->GetIntField(jenv,ret,fid);
        
            values[i] = build_datum_from_return_field(ret, cls, fieldname, sig);
        }

    }
    


}

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
    call_java_function(values, "ai/sedn/unsupervised/Kmeans", "kmeans_gradients_cpu_float_test", "(F[F)Lai/sedn/unsupervised/TestReturn;",batch_percent,floatArray);


    // Build return tuple

    //Datum values[2];// = palloc(2 * sizeof(Datum));
    	
    //values[0] = Int32GetDatum(1);
    //values[1] = Int32GetDatum(2);
    
   
    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
    
    pfree(nulls);


    fetch_data();

    PG_RETURN_DATUM( HeapTupleGetDatum(tuple ));
}


/*
    JVM creation
*/

char** readOptions(char* filename, int* N) {
    FILE *file;
    file = fopen(filename,"r");
    char **lines = NULL;
    char *line =  NULL;
    *N = -1;
    size_t len = 0;
    size_t read;
    while((read = getline(&line,&len,file)) != -1) {
        if ((line[0] != '#') && (line[0] != '\n')) {
            (*N)++;
            
            // Insert = for add-exports
            char *p = strstr(line, "--add-exports ");
            if (p != NULL) {
                memcpy(p, "--add-exports=",14);
            }

            // Insert = for add-opens
            p = strstr(line, "--add-opens ");
            if (p != NULL) {
                memcpy(p, "--add-opens=",12);
            }

            // Alloc mem
            lines = (char**)realloc(lines, ( (*N)+1) * sizeof(char*));
            
            line[read-1] = '\0';
            char* newLine = (char*)malloc((read) * sizeof(char));
            strncpy(newLine,line,read);

            lines[*N] = newLine;
        }
    } 

    (*N)++;

    return lines;
}

JavaVMOption* setJVMoptions(int* numOptions) {
    
    // Read @ options
    int N1;
    char **lines1 =  readOptions("/data/TornadoVM/bin/sdk/etc/exportLists/common-exports",&N1);

    int N2;
    char **lines2 =  readOptions("/data/TornadoVM/bin/sdk/etc/exportLists/opencl-exports",&N2);
    
    *numOptions = N1+N2+21;
    JavaVMOption* options = malloc( (*numOptions)*sizeof(JavaVMOption));
    
    //options[0].optionString = "-Djava.class.path=/data/TornadoVM/tornado-examples/target/tornado-examples-0.15.2-dev-d9c095d.jar";
    options[0].optionString = "-Djava.class.path=/data/unsupervised/java/unsupervised/target/unsupervised-0.0.1-SNAPSHOT.jar";
    options[1].optionString = "-XX:-UseCompressedOops"; 
    options[2].optionString = "-XX:+UnlockExperimentalVMOptions";
    options[3].optionString = "-XX:+EnableJVMCI"; 
    //options[4].optionString = "--module-path=/data/unsupervised/java/unsupervised/target/unsupervised-0.0.1-SNAPSHOT.jar:/data/TornadoVM/bin/sdk/share/java/tornado/asm-9.5.jar:/data/TornadoVM/bin/sdk/share/java/tornado/commons-lang3-3.12.0.jar:/data/TornadoVM/bin/sdk/share/java/tornado/ejml-core-0.38.jar:/data/TornadoVM/bin/sdk/share/java/tornado/ejml-ddense-0.38.jar:/data/TornadoVM/bin/sdk/share/java/tornado/ejml-dsparse-0.38.jar:/data/TornadoVM/bin/sdk/share/java/tornado/ejml-simple-0.38.jar:/data/TornadoVM/bin/sdk/share/java/tornado/hamcrest-core-1.3.jar:/data/TornadoVM/bin/sdk/share/java/tornado/jmh-core-1.29.jar:/data/TornadoVM/bin/sdk/share/java/tornado/jopt-simple-4.6.jar:/data/TornadoVM/bin/sdk/share/java/tornado/jsr305-3.0.2.jar:/data/TornadoVM/bin/sdk/share/java/tornado/junit-4.13.2.jar:/data/TornadoVM/bin/sdk/share/java/tornado/log4j-api-2.17.1.jar:/data/TornadoVM/bin/sdk/share/java/tornado/log4j-core-2.17.1.jar:/data/TornadoVM/bin/sdk/share/java/tornado/lucene-core-8.2.0.jar:/data/TornadoVM/bin/sdk/share/java/tornado/tornado-annotation-0.16-dev.jar:/data/TornadoVM/bin/sdk/share/java/tornado/tornado-api-0.16-dev.jar:/data/TornadoVM/bin/sdk/share/java/tornado/tornado-benchmarks-0.16-dev.jar:/data/TornadoVM/bin/sdk/share/java/tornado/tornado-drivers-common-0.16-dev.jar:/data/TornadoVM/bin/sdk/share/java/tornado/tornado-drivers-opencl-0.16-dev.jar:/data/TornadoVM/bin/sdk/share/java/tornado/tornado-drivers-opencl-jni-0.16-dev-libs.jar:/data/TornadoVM/bin/sdk/share/java/tornado/tornado-examples-0.16-dev.jar:/data/TornadoVM/bin/sdk/share/java/tornado/tornado-matrices-0.16-dev.jar:/data/TornadoVM/bin/sdk/share/java/tornado/tornado-runtime-0.16-dev.jar:/data/TornadoVM/bin/sdk/share/java/tornado/tornado-unittests-0.16-dev.jar:/data/TornadoVM/bin/sdk/share/java/graalJars/collections-23.1.0.jar:/data/TornadoVM/bin/sdk/share/java/graalJars/compiler-23.1.0.jar:/data/TornadoVM/bin/sdk/share/java/graalJars/compiler-management-23.1.0.jar:/data/TornadoVM/bin/sdk/share/java/graalJars/graal-sdk-23.1.0.jar:/data/TornadoVM/bin/sdk/share/java/graalJars/polyglot-23.1.0.jar:/data/TornadoVM/bin/sdk/share/java/graalJars/truffle-api-23.1.0.jar:/data/TornadoVM/bin/sdk/share/java/graalJars/truffle-compiler-23.1.0.jar:/data/TornadoVM/bin/sdk/share/java/graalJars/word-23.1.0.jar";
    options[4].optionString = "--module-path=/data/TornadoVM/bin/sdk/share/java/tornado/";
    options[5].optionString = "-Djava.library.path=/data/TornadoVM/bin/sdk/lib";
    options[6].optionString = "-Dtornado.load.api.implementation=uk.ac.manchester.tornado.runtime.tasks.TornadoTaskGraph";
    options[7].optionString = "-Dtornado.load.runtime.implementation=uk.ac.manchester.tornado.runtime.TornadoCoreRuntime";
    options[8].optionString = "-Dtornado.load.tornado.implementation=uk.ac.manchester.tornado.runtime.common.Tornado";
    options[9].optionString = "-Dtornado.load.device.implementation.opencl=uk.ac.manchester.tornado.drivers.opencl.runtime.OCLDeviceFactory";
    options[10].optionString = "-Dtornado.load.device.implementation.ptx=uk.ac.manchester.tornado.drivers.ptx.runtime.PTXDeviceFactory";
    options[11].optionString = "-Dtornado.load.device.implementation.spirv=uk.ac.manchester.tornado.drivers.spirv.runtime.SPIRVDeviceFactory";
    options[12].optionString = "-Dtornado.load.annotation.implementation=uk.ac.manchester.tornado.annotation.ASMClassVisitor"; 
    options[13].optionString = "-Dtornado.load.annotation.parallel=uk.ac.manchester.tornado.api.annotations.Parallel";
    options[14].optionString = "--upgrade-module-path=/data/TornadoVM/bin/sdk/share/java/graalJars";
    options[15].optionString = "--upgrade-module-path=/data/unsupervised/java/unsupervised/target/unsupervised-0.0.1-SNAPSHOT.jar";
    options[16].optionString = "-XX:+UseParallelGC";
    options[17].optionString = "-Dtornado.profiler=True";
    options[18].optionString = "--add-modules=ALL-SYSTEM,tornado.runtime,tornado.annotation,tornado.drivers.common,tornado.drivers.opencl,unsupervised";
    options[19].optionString = "--enable-preview";
    options[20].optionString = "--enable-native-access=unsupervised";

    for(int i=0; i < N1; i++) {
        options[21+i].optionString = lines1[i];
    }

    for(int i=0; i < N2; i++) {
        options[21+N1+i].optionString = lines2[i];
    }
 
    return options;
} 


int startJVM() {

    elog(WARNING,"Starting JVM");
    
    int numOptions;
    JavaVMOption *options = setJVMoptions(&numOptions);

    JavaVM *jvm;
    JavaVMInitArgs vm_args;

    vm_args.version = JNI_VERSION_1_8;
    vm_args.nOptions = numOptions;
    vm_args.options = options;
    vm_args.ignoreUnrecognized = JNI_FALSE;


    void *jvmLibrary = dlopen("/data/TornadoVM/etc/dependencies/TornadoVM-jdk21/jdk-21.0.1/lib/server/libjvm.so", RTLD_NOW | RTLD_GLOBAL);

    JNI_CreateJavaVM_func JNI_CreateJavaVM = (JNI_CreateJavaVM_func) dlsym(jvmLibrary, "JNI_CreateJavaVM");

    jint result = JNI_CreateJavaVM(&jvm, (void **)&jenv, &vm_args);
    
    return result;
}


