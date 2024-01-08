
#include <jni.h>
#ifndef MOONSHOT_JVM_H
#define MOONSHOT_JVM_H

extern JNIEnv *jenv;
extern JavaVM *jvm;
 
typedef jint(JNICALL *JNI_CreateJavaVM_func)(JavaVM **pvm, void **penv, void *args);

extern int startJVM();
extern int call_java_function(Datum* values, bool* primitive, char* class_name, char* method_name, char* signature, jvalue* args);

#endif