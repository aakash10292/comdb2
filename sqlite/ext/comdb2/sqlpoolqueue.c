/*
   Copyright 2020 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include "comdb2.h"
#include "sql.h"
#include "bdb_int.h"
#include "comdb2systblInt.h"
#include "ezsystables.h"
#include "thdpool.h"
#include "cdb2api.h"
#include "string_ref.h"

typedef struct systable_sqlpoolqueue {
    int64_t                 time_in_queue_ms;
    char                    *info;
    int                     info_is_null;
} systable_sqlpoolqueue_t;

typedef struct getsqlpoolqueue {
    int count;
    int alloc;
    systable_sqlpoolqueue_t *records;
} getsqlpoolqueue_t;

static void collect(struct thdpool *pool, struct workitem *item, void *user)
{
    getsqlpoolqueue_t *q = (getsqlpoolqueue_t *)user;
    systable_sqlpoolqueue_t *i;
    q->count++;
    if (q->count >= q->alloc) {
        if (q->alloc == 0) q->alloc = 16;
        else q->alloc = q->alloc * 2;
        q->records = realloc(q->records, q->alloc * sizeof(systable_sqlpoolqueue_t));
    }

    i = &q->records[q->count - 1];
    i->time_in_queue_ms = comdb2_time_epochms() - item->queue_time_ms;
    if (item->ref_persistent_info) {
        i->info = strdup(string_ref_cstr(item->ref_persistent_info));
        i->info_is_null = 0;
    } else {
        i->info = NULL;
        i->info_is_null = 1;
    }
}

static int get_sqlpoolqueue(void **data, int *records)
{
    getsqlpoolqueue_t q = {0};
    foreach_all_sql_pools(collect, &q);
    *data = q.records;
    *records = q.count;
    return 0;
}

static void free_sqlpoolqueue(void *p, int n)
{
    systable_sqlpoolqueue_t *t = (systable_sqlpoolqueue_t *)p;
    for (int i=0;i<n;i++) {
        if (t[i].info)
            free(t[i].info);
    }
    free(p);
}

sqlite3_module systblSqlpoolQueueModule = {
    .access_flag = CDB2_ALLOW_USER,
};

int systblSqlpoolQueueInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_sqlpool_queue",
        &systblSqlpoolQueueModule, get_sqlpoolqueue, free_sqlpoolqueue,
        sizeof(systable_sqlpoolqueue_t),
        CDB2_INTEGER, "time_in_queue_ms", -1, offsetof(systable_sqlpoolqueue_t,
                                                       time_in_queue_ms),
        CDB2_CSTRING, "sql", offsetof(systable_sqlpoolqueue_t, info_is_null),
        offsetof(systable_sqlpoolqueue_t, info),
        SYSTABLE_END_OF_FIELDS);
}

