#ifndef INCLUDED_LOG_PUBLISHER_H
#define INCLUDED_LOG_PUBLISHER_H
#include "sqlite3ext.h"
#include <assert.h>
#include <string.h>
#include "comdb2.h"
#include "build/db.h"
#include "dbinc/db_swap.h"
#include "dbinc_auto/txn_auto.h"
#include "parse_lsn.h"
#include "bbinc/comdb2_message_queue.h"
#include <bdb/bdb_int.h>
#define LSN_SIZE sizeof(uint32_t)
typedef struct log_message {
    DB_LSN lsn;
    DBT payload;
} log_message_t;
typedef struct log_publisher_args {
    comdb2_queue_pub_t *pub;
    uint32_t flags;
}log_publisher_args_t;

void *publish_logs(void *);
#endif
