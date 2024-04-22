#include "moonshot_worker.h"

Datum control_bgworkers(FunctionCallInfo fcinfo, int n_workers, bool need_SPI, bool globalWorker, char* class_name, char* method_name, char* signature, char* return_type);
Datum control_fgworker(FunctionCallInfo fcinfo, bool need_SPI, char* class_name, char* method_name, char* signature, char* return_type);
