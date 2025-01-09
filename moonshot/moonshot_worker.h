#include "postgres.h"
#include "storage/latch.h"
#include "postmaster/bgworker.h"

#define MAX_USERS 1+1
#define MAX_WORKERS 1
#define MAX_QUEUE_LENGTH 16
#define MAX_DATA 2097152*1

typedef struct 
{
    dlist_node node;
    int taskid;
    char class_name[128];
    char method_name[128];
    char signature[256];
    char return_type[1];
    Latch *notify_latch;
    int n_args;
    int n_return;
    bool error;
    char data[MAX_DATA];
} worker_exec_entry;

typedef struct
{
	volatile slock_t lock;
    dlist_head exec_list;
    dlist_head free_list;
    dlist_head return_list;
    int n_workers;
    pid_t pid[MAX_WORKERS];
    Latch *latch[MAX_WORKERS];
    worker_exec_entry list_data[MAX_QUEUE_LENGTH];
} worker_data_head;


worker_data_head* launch_dynamic_workers(int32 n_workers, bool needSPI, bool globalWorker);
Datum datumDeSerialize(char **address, bool *isnull);
void prepareErrorMsg(jthrowable exh, char* target, int cutoff);
