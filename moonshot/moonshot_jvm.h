#ifndef MOONSHOT_JVM_H
#define MOONSHOT_JVM_H

#include <jni.h>
#include "utils/tuplestore.h"

extern JNIEnv *jenv;
extern JavaVM *jvm;
 
typedef jint(JNICALL *JNI_CreateJavaVM_func)(JavaVM **pvm, void **penv, void *args);

extern int startJVM(char* error_msg);
extern int call_java_function(Datum* values, bool* primitive, char* class_name, char* method_name, char* signature, char* return_type, jvalue* args, char* error_msg);
extern int call_iter_java_function(Tuplestorestate* tupstore, TupleDesc tupdesc, char* class_name, char* method_name, char* signature, jvalue* args, char* error_msg);
extern char* convert_name_to_JNI_signature(char* name, char* error_msg);
extern int set_jobject_field_from_datum(jobject* obj, jfieldID* fid, Datum* dat, char* sig);
extern void freejvalues(jvalue* jvals, short* argprim, int N);

#endif