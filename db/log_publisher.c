#include "sqlite3ext.h"
#include <assert.h>
#include <string.h>
#include "comdb2.h"
#include "build/db.h"
#include "dbinc/db_swap.h"
#include "dbinc_auto/txn_auto.h"
#include "comdb2systbl.h"
#include "parse_lsn.h"

#define LSN_SIZE sizeof(uint32_t)
typedef struct log_message {
    DB_LSN lsn;
    DBT payload;
} log_message_t;
typedef struct log_publisher_args {
    PUBLISHER *pub;
    uint32_t flags;
}log_publisher_args_t;

extern pthread_mutex_t gbl_logput_lk;
extern pthread_cond_t gbl_logput_cond;
extern pthread_mutex_t gbl_durable_lsn_lk;
extern pthread_cond_t gbl_durable_lsn_cond;

static inline void tranlog_lsn_to_str(char *st, DB_LSN *lsn)
{
    sprintf(st, "{%d:%d}", lsn->file, lsn->offset);
}

static inline void tranlog_copy_lsn(DB_LSN *to, DB_LSN *from){
    to->file = from->file;
    to->offset = from->offset;
}

void *log_publisher(void *args){
    bdb_state_type *bdb_state = thedb->bdb_env;
    log_publisher_args_t *publisher = (log_publisher_args_t *) args;
    int rc;
    DB_LOGC *logc;
    DB_LSN prev_lsn;
    DB_LSN cur_lsn;
    DBT data;
    uint32_t cur_flags = publisher->flags;
    /* first get a log cursor */
    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv,&logc,0);
    if(rc != 0){
        logmsg(LOGMSG_ERROR, "%s line %d error getting a log cursor rc=%d\n",
                __func__, __LINE__, rc);
        return;
    }
    // Initialize curLsn to start of Log
    cur_lsn.file = 0;
    cur_lsn.offset = 0;
    cur_flags = DB_FIRST;
    // Now that we have a log cursor, get the logs
    while(1){
        tranlog_copy_lsn(&prev_lsn, &cur_lsn);
        if((rc = logc->get(logc, &curLsn, &data, cur_flags))!=0){
            if(cur_flags != DB_NEXT && cur_flags != DB_PREV){
                // we encountered an error while fetching the first log itself
                // report and return (?)
                logmsg(LOGMSG_ERROR, "%s line %d error getting first log record rc=%d\n",
                        __func__, __LINE__, rc);
                return;
            }
            cur_flags = DB_NEXT;
            do {
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_nsec += (200 * 1000000);
                Pthread_mutex_lock(&gbl_logput_lk);
                pthread_cond_timedwait(&gbl_logput_cond, &gbl_logput_lk, &ts);
                Pthread_mutex_unlock(&gbl_logput_lk);

                int sleepms = 100;
                while (bdb_the_lock_desired()) {
                  if (thd == NULL) {
                      thd = pthread_getspecific(query_info_key);
                  }
                  recover_deadlock(thedb->bdb_env, thd, NULL, sleepms);
                  sleepms*=2;
                  if (sleepms > 10000)
                      sleepms = 10000;
                }
            }while((rc = logc->get(logc,&cur_lsn,&data,cur_flags))!=0);
        }
        // we have a log record. Serialize and publish it
        //
        char lsn_str[LSN_SIZE];
        tranlog_lsn_to_str(lsn_str, &prev_lsn);

        void *buf = (void *) malloc(LSN_SIZE + data.size);
        if(!buf){
            logmsg(LOGMSG_ERROR, "%s line %d malloc error!\n",
                    __func__, __LINE__);
            return;
        }
        memcpy(buf,lsn_str, LSN_SIZE);
        memcpy(buf,data.data, data.size);

        if(buf) {
            free(buf);
        }
        // publish the buffer
        int rc = publisher->publish(buf, sizeof(buf));
        if(rc !=0){
            logmsg(LOGMSG_ERROR, "%s line %d error publishing log rc=%d\n",
                    __func__, __LINE__, rc);
            
        }
        cur_flags = DB_NEXT;
    }
}

