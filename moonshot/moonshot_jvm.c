#include "pg_config.h"
#include "pg_config_manual.h"

#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
#include "catalog/pg_type.h"
#include <dlfcn.h>
#include "moonshot_jvm.h"
#include "utils/guc.h"

#include "utils/tuplestore.h"

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
       
    // Note: Keep non-switch for now for future extension to arrays

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

    /*} 
        // TO BE DONE ...
        // Problem for BG worker: limited space in queue package -> Distribute over several

        else if(strcmp(return_type,"ITER") == 0) {
        
        jobject ret = (*jenv)->CallStaticObjectMethodA(jenv, clazz, methodID, args);

        // Catch exception
        if( (*jenv)->ExceptionCheck(jenv) ) {
            return 1;
        }

        // Analyis return
        jclass cls = (*jenv)->GetObjectClass(jenv, ret);
        
        // Iterator
        jmethodID hasNext = (*jenv)->GetMethodID(jenv, cls, "hasNext", "()Z");
	    jmethodID next = (*jenv)->GetMethodID(jenv, cls, "next", "()Ljava/lang/Object;");

        if(hasNext == NULL || next == NULL) {
            // ToDo: CLEAN RETURN WITH ERROR MSG !
            elog(ERROR,"Object returned is not iterator");
        }

        bool hasNext = (bool) (*jenv)->CallBooleanMethod(jenv, cls, hasNext);
 
        if(hasNext) {
            // ToDo: Set function to return one item per call ?        
        
        }
    */    
    }  else {
        jobject ret = (*jenv)->CallStaticObjectMethodA(jenv, clazz, methodID, args);
     
        // Catch exception
        if( (*jenv)->ExceptionCheck(jenv) ) {
            return 1;
        }
           
        // Analyis return
        jclass cls = (*jenv)->GetObjectClass(jenv, ret);

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
                jstring jstr2 = (jstring)(*jenv)->CallObjectMethod(jenv, value, m);
                char* typename =  (*jenv)->GetStringUTFChars(jenv, jstr2, false);
            
                char* sig = convert_name_to_JNI_signature(typename);

                values[i] = build_datum_from_return_field(&primitive[i], ret, cls, fieldname, sig);

                // Cleanup
                (*jenv)->ReleaseStringUTFChars(jenv, jstr, fieldname);
                (*jenv)->ReleaseStringUTFChars(jenv, jstr2, typename);
            }
        }
    }

    return 0;
}

/*
    Call java function with iterator return (ONLY FOR FG WORKER !)
*/
int call_iter_java_function(Tuplestorestate* tupstore, TupleDesc tupdesc, char* class_name, char* method_name, char* signature, jvalue* args) {

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

    jobject ret = (*jenv)->CallStaticObjectMethodA(jenv, clazz, methodID, args);

    // Catch exception
    if( (*jenv)->ExceptionCheck(jenv) ) {
        return 1;
    }

    // Analyis return
    jclass cls = (*jenv)->GetObjectClass(jenv, ret);
    
    // Iterator
    jmethodID hasNextF = (*jenv)->GetMethodID(jenv, cls, "hasNext", "()Z");
    jmethodID nextF = (*jenv)->GetMethodID(jenv, cls, "next", "()Ljava/lang/Object;");

    if(hasNextF == NULL || nextF == NULL) {
        // ToDo: CLEAN RETURN WITH ERROR MSG !
        elog(ERROR,"Object returned is not iterator");
    }
    
    bool hasNext = (bool) (*jenv)->CallBooleanMethod(jenv, ret, hasNextF);

    while(hasNext) {
        // Get row object        
        jobject row = (*jenv)->CallObjectMethod(jenv, ret, nextF);
        jclass rcls = (*jenv)->GetObjectClass(jenv, row);
        
        // Prepare row
        jmethodID getFields = (*jenv)->GetMethodID(jenv, (*jenv)->GetObjectClass(jenv, rcls), "getFields", "()[Ljava/lang/reflect/Field;");

        jobjectArray fieldsList = (jobjectArray)  (*jenv)->CallObjectMethod(jenv, rcls, getFields); 
  
        jsize len =  (*jenv)->GetArrayLength(jenv,fieldsList);
        
        if(len == 0) {
            // NOTE: NATIVE RETURN 
        } else {
            // Composite return
            
            Datum values[len];
            bool*  nulls = palloc0( len * sizeof( bool ) );
            bool* primitive = palloc0( len * sizeof( bool ) );
            
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
                jstring jstr2 = (jstring)(*jenv)->CallObjectMethod(jenv, value, m);
                char* typename =  (*jenv)->GetStringUTFChars(jenv, jstr2, false);
            
                char* sig = convert_name_to_JNI_signature(typename);
                values[i] = build_datum_from_return_field(&primitive[i], row, rcls, fieldname, sig);

                // Cleanup
                (*jenv)->ReleaseStringUTFChars(jenv, jstr, fieldname);
                (*jenv)->ReleaseStringUTFChars(jenv, jstr2, typename);
            }
            tuplestore_putvalues(tupstore, tupdesc, values, nulls);

            pfree(primitive);
        }

        // Next
        hasNext = (bool) (*jenv)->CallBooleanMethod(jenv, ret, hasNextF);
    }

    return 0;
}



/*
    Read JVM options from file
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
    Read JVM options from GUC
*/
JavaVMOption* setJVMoptions(int* numOptions) {
    
    // Read option string from GUC
    char* OPTIONS = GetConfigOption("ms.jvmoptions",true,true);

    if(OPTIONS == NULL) {
        elog(ERROR,"ms.jvmoptions GUC not set");
    } 

    // Parse options
    JavaVMOption* opts = malloc( 1*sizeof(JavaVMOption) );
    
    int No = 0;
    bool active = false;
    int spos = 0;
    for(int i=0; i < strlen(OPTIONS); i++) {
        if( (OPTIONS[i] == '-' || OPTIONS[i] == '@') && !active) { 
            active = true;
            spos = i;
            continue;
        }

        if( (OPTIONS[i] == ' ' || i == strlen(OPTIONS)-1) && active) {
            active = false;
            int len;
            if( i < strlen(OPTIONS)-1) {
                len = i-spos+1;
            } else {
                len = i-spos+2;
            }

            // Create option
            if(OPTIONS[spos] != '@') {
                
                char* buf = malloc(len); 
                strncpy(buf,&OPTIONS[spos],len);
                buf[len-1] = '\0';
            
                opts[No].optionString = buf;
                No++;

                opts = realloc(opts,No*sizeof(JavaVMOption));

            } else {
                // Read from file
                int N;
                char buf[i-spos]; 
                strncpy(buf,&OPTIONS[spos],i-spos);
             
                char **lines =  readOptions(&buf[1],&N);
                for(int l = 0; l < N; l++ ) {
              
                    opts[No].optionString = lines[l];
                    No++;

                    opts = realloc(opts,No*sizeof(JavaVMOption));
                }
            }

            continue;
        }

    }
    
    *numOptions = No;
    return opts;
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

    // Read location of libjvm from GUC
    char* JVM_SO_FILE = GetConfigOption("ms.libjvm",true,true);

    if(JVM_SO_FILE == NULL) {
        elog(ERROR,"ms.libjvm GUC pointing to libjvm.so not set");
    }

    void *jvmLibrary = dlopen(JVM_SO_FILE, RTLD_NOW | RTLD_GLOBAL);

    JNI_CreateJavaVM_func JNI_CreateJavaVM = (JNI_CreateJavaVM_func) dlsym(jvmLibrary, "JNI_CreateJavaVM");

    jint result = JNI_CreateJavaVM(&jvm, (void **)&jenv, &vm_args);
    
    elog(NOTICE,"JVM result: %d",result);

    // Free
    for(int i = 0; i < numOptions; i++) {
        free(options[i].optionString);
    }
    free(options);

    return result;
}

