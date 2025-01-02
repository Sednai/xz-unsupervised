#include "moonshot_worker.h"


typedef struct {
    bool global;    
    bool need_SPI;
    char* mode;
    char* class_name;
    char* method_name;
    char* return_type;
    char* signature;
} control_entry;

Datum control_bgworkers(FunctionCallInfo fcinfo, int n_workers, bool need_SPI, bool globalWorker, char* class_name, char* method_name, char* signature, char* return_type);
Datum control_fgworker(FunctionCallInfo fcinfo, bool need_SPI, char* class_name, char* method_name, char* signature, char* return_type);
