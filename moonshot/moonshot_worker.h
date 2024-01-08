#include "postgres.h"
#include "storage/latch.h"

#define MAX_WORKERS 3
#define MAX_QUEUE_LENGTH 12

typedef struct 
{
    dlist_node node;
    int taskid;
    char class_name[128];
    char method_name[128];
    char signature[128];
    Latch *notify_latch;
    int n_args;
    int n_return;
    bool error;
    char data[2*1024*1024];
} worker_exec_entry;

typedef struct
{
	volatile slock_t lock;
    dlist_head exec_list;
    dlist_head free_list;
    dlist_head return_list;
    int n_workers;
    Latch *latch[MAX_WORKERS];
    worker_exec_entry list_data[MAX_QUEUE_LENGTH];
} worker_data_head;


/*
typedef struct
{
	pg_atomic_uint32    num_active_workers;
	struct {
		Latch *latch;
	} workers[1];

} worker_state_data;
*/

worker_data_head* launch_dynamic_workers(int n_workers, bool needSPI, bool globalWorker);
Datum datumDeSerialize(char **address, bool *isnull);
