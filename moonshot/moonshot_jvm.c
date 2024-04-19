#include "pg_config.h"
#include "pg_config_manual.h"

#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
#include "catalog/pg_type.h"
#include <dlfcn.h>
#include "moonshot_jvm.h"

#define JVM_SO_FILE "/data/TornadoVM/etc/dependencies/TornadoVM-jdk21/jdk-21.0.2/lib/server/libjvm.so"

//#define JVM_SO_FILE "/ZNVME/xz4/app/misc/TornadoVM/etc/dependencies/TornadoVM-jdk21/jdk-21.0.1/lib/server/libjvm.so"

JNIEnv *jenv;
JavaVM *jvm;
 
//Use PG hashtable instead ?!
char* convert_name_to_JNI_signature(char* name) {
   
    // Native
    if (strcmp(name, "int") == 0) {
        return "I";
    } else if (strcmp(name, "double") == 0) {
        return "D";
    } else if (strcmp(name, "float") == 0) {
        return "F";
    } else if (strcmp(name, "long") == 0) {
        return "J";
    }
    // Arrays
    else if (strcmp(name, "[F") == 0) {
        return name;
    } else if (strcmp(name, "[[F") == 0) {
        return name;
    } else if (strcmp(name, "[I") == 0) {
        return name;
    } else if (strcmp(name, "[D") == 0) {
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
Datum build_datum_from_return_field(bool* primitive, jobject data, jclass cls, char* fieldname, char* sig) {
    jfieldID fid = (*jenv)->GetFieldID(jenv,cls,fieldname,sig);       
    
    // Natives
    if (strcmp(sig, "I") == 0) {
        *primitive = true;
        return Int32GetDatum(  (*jenv)->GetIntField(jenv,data,fid) );
    } else if (strcmp(sig, "D") == 0) {
        *primitive = true;
        return Float8GetDatum(  (*jenv)->GetDoubleField(jenv,data,fid) );
    } else if (strcmp(sig, "F") == 0) {
        *primitive = true;
        jfloat test = (*jenv)->GetFloatField(jenv,data,fid);
        return Float4GetDatum(  (*jenv)->GetFloatField(jenv,data,fid) );
    } else if (strcmp(sig, "J") == 0) {
        *primitive = true;
        jfloat test = (*jenv)->GetLongField(jenv,data,fid);
        return Int64GetDatum(  (*jenv)->GetLongField(jenv,data,fid) );
    }

    // Native arrays
    else if (strcmp(sig, "[F") == 0) {
        *primitive = false;
        jfloatArray arr = (jfloatArray) (*jenv)->GetObjectField(jenv,data,fid);
        int nElems = (*jenv)->GetArrayLength(jenv, arr); 
        ArrayType* v = createArray(nElems, sizeof(jfloat), FLOAT4OID, false);
		(*jenv)->GetFloatArrayRegion(jenv,arr, 0, nElems, (jfloat*)ARR_DATA_PTR(v));

        return PointerGetDatum(v); 
    
    } else if (strcmp(sig, "[D") == 0) {
        *primitive = false;
        jdoubleArray arr = (jdoubleArray) (*jenv)->GetObjectField(jenv,data,fid);
        int nElems = (*jenv)->GetArrayLength(jenv, arr); 
        ArrayType* v = createArray(nElems, sizeof(jdouble), FLOAT8OID, false);
		(*jenv)->GetDoubleArrayRegion(jenv,arr, 0, nElems, (jdouble*)ARR_DATA_PTR(v));

        return PointerGetDatum(v); 

    } else if (strcmp(sig, "[I") == 0) {
        *primitive = false;
        jarray arr = (jarray) (*jenv)->GetObjectField(jenv,data,fid);
        int nElems = (*jenv)->GetArrayLength(jenv, arr); 
        ArrayType* v = createArray(nElems, sizeof(jint), INT4OID, false);
		(*jenv)->GetIntArrayRegion(jenv,arr, 0, nElems, (jint*)ARR_DATA_PTR(v));

        return PointerGetDatum(v);

    }  else if (strcmp(sig, "[[F") == 0) {
        *primitive = false;
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

// ToDo: Iterator / setof multi-row return
int call_java_function(Datum* values, bool* primitive, char* class_name, char* method_name, char* signature, char* return_type, jvalue* args) {

    // Prep and call function
    jclass clazz = (*jenv)->FindClass(jenv, class_name);

    if(clazz == NULL) {
        elog(WARNING,"Java class %s not found !",class_name);
        return -1;
    }

    jmethodID methodID = (*jenv)->GetStaticMethodID(jenv, clazz, method_name, signature);

    if(methodID == NULL) {
        elog(WARNING,"Java method %s with signature %s not found !",method_name, signature);
        return -2;
    }
       
    if(strcmp(return_type, "J") == 0) {
    
        jlong ret = (*jenv)->CallStaticLongMethodA(jenv, clazz, methodID, args);
        
        // Catch exception
        if( (*jenv)->ExceptionCheck(jenv) ) {
            return 1;
        }
        
        values[0] = Int64GetDatum( ret );
    
        return 0;

    } else if(strcmp(return_type, "I") == 0) {
    
        jint ret = (*jenv)->CallStaticIntMethodA(jenv, clazz, methodID, args);
        
        // Catch exception
        if( (*jenv)->ExceptionCheck(jenv) ) {
            return 1;
        }
        
        values[0] = Int32GetDatum( ret );
    
        return 0;
    
    } else if(strcmp(return_type, "D") == 0) {
    
        jdouble ret = (*jenv)->CallStaticDoubleMethodA(jenv, clazz, methodID, args);
        
        // Catch exception
        if( (*jenv)->ExceptionCheck(jenv) ) {
            return 1;
        }
        
        values[0] = Float8GetDatum( ret );
    
        return 0;
    
    } else if(strcmp(return_type, "F") == 0) {
    
        jdouble ret = (*jenv)->CallStaticFloatMethodA(jenv, clazz, methodID, args);
        
        // Catch exception
        if( (*jenv)->ExceptionCheck(jenv) ) {
            return 1;
        }
        
        values[0] = Float4GetDatum( ret );
    
        return 0;
    
    }  else {
        jobject ret = (*jenv)->CallStaticObjectMethodA(jenv, clazz, methodID, args);
     
        // Catch exception
        if( (*jenv)->ExceptionCheck(jenv) ) {
            return 1;
        }
           
        // Analyis return
        jclass cls = (*jenv)->GetObjectClass(jenv, ret);

        // Map to own function below ? <- Re-use for setof return

        // Cache ret info -> Datum mapping ?
    
        jmethodID getFields = (*jenv)->GetMethodID(jenv, (*jenv)->GetObjectClass(jenv,cls), "getFields", "()[Ljava/lang/reflect/Field;");

        jobjectArray fieldsList = (jobjectArray)  (*jenv)->CallObjectMethod(jenv, cls, getFields); 
        
        
        jsize len =  (*jenv)->GetArrayLength(jenv,fieldsList);

        if(len == 0) {
                // ToDo: CLEAN RETURN WITH ERROR MSG !
                elog(ERROR,"Empty composite return not implemented !");
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

                    values[i] = build_datum_from_return_field(&primitive[i], ret, cls, fieldname, sig);
                }
            }
    }

    
    return 0;
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

/* 
    xz4 
*/
/*
JavaVMOption* setJVMoptions(int* numOptions) {
    
    // Read @ options
    int N1;
    char **lines1 =  readOptions("/ZNVME/xz4/app/misc/TornadoVM/bin/sdk/etc/exportLists/common-exports",&N1);

    int N2;
    char **lines2 =  readOptions("/ZNVME/xz4/app/misc/TornadoVM/bin/sdk/etc/exportLists/opencl-exports",&N2);
    
    *numOptions = N1+N2+21;
    JavaVMOption* options = malloc( (*numOptions)*sizeof(JavaVMOption));
    
    options[0].optionString = "-Djava.class.path=/ZNVME/xz4/app/misc/xz-unsupervised/target/unsupervised-0.0.1-SNAPSHOT.jar";
    options[1].optionString = "-XX:-UseCompressedOops"; 
    options[2].optionString = "-XX:+UnlockExperimentalVMOptions";
    options[3].optionString = "-XX:+EnableJVMCI"; 
    options[4].optionString = "--module-path=/ZNVME/xz4/app/misc/TornadoVM/bin/sdk/share/java/tornado/";
    options[5].optionString = "-Djava.library.path=/data/TornadoVM/bin/sdk/lib";
    options[6].optionString = "-Dtornado.load.api.implementation=uk.ac.manchester.tornado.runtime.tasks.TornadoTaskGraph";
    options[7].optionString = "-Dtornado.load.runtime.implementation=uk.ac.manchester.tornado.runtime.TornadoCoreRuntime";
    options[8].optionString = "-Dtornado.load.tornado.implementation=uk.ac.manchester.tornado.runtime.common.Tornado";
    options[9].optionString = "-Dtornado.load.device.implementation.opencl=uk.ac.manchester.tornado.drivers.opencl.runtime.OCLDeviceFactory";
    options[10].optionString = "-Dtornado.load.device.implementation.ptx=uk.ac.manchester.tornado.drivers.ptx.runtime.PTXDeviceFactory";
    options[11].optionString = "-Dtornado.load.device.implementation.spirv=uk.ac.manchester.tornado.drivers.spirv.runtime.SPIRVDeviceFactory";
    options[12].optionString = "-Dtornado.load.annotation.implementation=uk.ac.manchester.tornado.annotation.ASMClassVisitor"; 
    options[13].optionString = "-Dtornado.load.annotation.parallel=uk.ac.manchester.tornado.api.annotations.Parallel";
    options[14].optionString = "--upgrade-module-path=/ZNVME/xz4/app/misc/TornadoVM/bin/sdk/share/java/graalJars";
    options[15].optionString = "--upgrade-module-path=/ZNVME/xz4/app/misc/xz-unsupervised/target/unsupervised-0.0.1-SNAPSHOT.jar";
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
*/


/*
    Local
*/
JavaVMOption* setJVMoptions(int* numOptions) {
    
    // Read @ options
    int N1;
    char **lines1 =  readOptions("/data/TornadoVM/bin/sdk/etc/exportLists/common-exports",&N1);

    int N2;
    char **lines2 =  readOptions("/data/TornadoVM/bin/sdk/etc/exportLists/opencl-exports",&N2);
    
    *numOptions = N1+N2+21;
    //*numOptions = 10;
    JavaVMOption* options = malloc( (*numOptions)*sizeof(JavaVMOption));
    
    options[0].optionString = "-Djava.class.path=.:/data/psearch/PS-Modules/runtime:/data/unsupervised/java/unsupervised/target/unsupervised-0.0.1-SNAPSHOT.jar:/data/psearch/PS-Modules/VariPeriodSearch-AERO-mod.jar";
  //    options[0].optionString = "-Djava.class.path=.:/data/psearch/AEROPeriodSearch/VariPeriodSearch-22.5.0-AERO.jar:/data/psearch/AEROPeriodSearch/runtime/commons-lang3-3.14.0.jar:/data/psearch/AEROPeriodSearch/runtime/commons-math3-3.6.1.jar:/data/psearch/AEROPeriodSearch/runtime/GaiaTools-20.6.1.jar:/data/psearch/AEROPeriodSearch/runtime/guava-31.1-jre.jar:/data/psearch/AEROPeriodSearch/runtime/log4j-api-2.22.0.jar:/data/psearch/AEROPeriodSearch/runtime/log4j-core-2.22.0.jar:/data/psearch/AEROPeriodSearch/runtime/log4j-slf4j2-impl-2.21.1.jar:/data/psearch/AEROPeriodSearch/runtime/slf4j-api-2.0.7.jar:/data/psearch/AEROPeriodSearch/runtime/VariConfiguration-SB-22.5.0-r785833-20240206122000.jar:/data/psearch/AEROPeriodSearch/runtime/VariFramework-SB-22.5.0-r785865M-20240206143549.jar:/data/psearch/AEROPeriodSearch/runtime/VariObjectModelInterface-22.5.0.jar:/data/psearch/AEROPeriodSearch/runtime/VariObjectModel-SB-22.5.0-r785718M-20240205141141.jar:/data/psearch/AEROPeriodSearch/runtime/VariStatistics-SB-22.5.0-r785006M-20240129170342.jar";
    //options[0].optionString = "-Xmx2G"; /data/psearch/AEROPeriodSearch/runtime
    options[1].optionString = "-XX:-UseCompressedOops"; 
    options[2].optionString = "-XX:+UnlockExperimentalVMOptions";
    options[3].optionString = "-XX:+EnableJVMCI"; 
    options[4].optionString = "--module-path=.:/data/psearch/PS-Modules/runtime:/data/psearch/PS-Modules/VariPeriodSearch-AERO-mod.jar:/data/unsupervised/java/unsupervised/target/unsupervised-0.0.1-SNAPSHOT.jar:/data/TornadoVM/bin/sdk/share/java/tornado";
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
    options[15].optionString = "-Xmx2G";
    //options[15].optionString = "--upgrade-module-path=/data/psearch/PS-Modules/runtime";
    options[16].optionString = "-XX:+UseParallelGC";
    options[17].optionString = "-Dtornado.profiler=False";
    options[18].optionString = "--add-modules=ALL-SYSTEM,tornado.runtime,tornado.annotation,tornado.drivers.common,tornado.drivers.opencl,unsupervised,gaia.cu7.algo.character.periodsearch";
    options[19].optionString = "--enable-preview";
    options[20].optionString = "--enable-native-access=unsupervised";
    //options[21].optionString = "-XX:-UseCompressedClassPointers";

    for(int i=0; i < N1; i++) {
        //elog(WARNING,"[DEBUG](JVM options): %s",options[i].optionString);
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

    JavaVMInitArgs vm_args;

    vm_args.version = JNI_VERSION_1_8;
    vm_args.nOptions = numOptions;
    vm_args.options = options;
    vm_args.ignoreUnrecognized = JNI_FALSE;


    void *jvmLibrary = dlopen(JVM_SO_FILE, RTLD_NOW | RTLD_GLOBAL);

    JNI_CreateJavaVM_func JNI_CreateJavaVM = (JNI_CreateJavaVM_func) dlsym(jvmLibrary, "JNI_CreateJavaVM");

    jint result = JNI_CreateJavaVM(&jvm, (void **)&jenv, &vm_args);
    
    elog(WARNING,"JVM result: %d",result);
    return result;
}

