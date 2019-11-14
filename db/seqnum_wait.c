#include<seqnum_wait.h>
#include<osqlblkseq.h>
#include<osqlblockproc.h>
#include<translistener.h>
#include<osqlcomm.h>
#include<socket_interfaces.h>
#include<gettimeofday_ms.h>
#include<lockmacros.h>
#include "comdb2.h"
#include <mem.h>
seqnum_wait_queue *work_queue = NULL;
static pool_t *seqnum_wait_queue_pool = NULL;
static pthread_mutex_t seqnum_wait_queue_pool_lk;
pthread_mutex_t max_lsn_lk;
int max_lsn;
extern uint64_t coherency_commit_timestamp;
extern pthread_rwlock_t commit_lock;
extern struct dbenv *thedb;
extern pool_t *p_reqs;
extern void (*comdb2_ipc_sndbak_len_sinfo)(struct ireq *, int);
extern int gbl_print_deadlock_cycles;
extern int last_slow_node_check_time;
extern __thread snap_uid_t *osql_snap_info;
//extern pthread_mutex_t lock;
void *queue_processor(void *);  
void destroy_ireq(struct dbenv *dbenv, struct ireq *iq);


void osql_postcommit_handle(struct ireq *);
void handle_postcommit_bpfunc(struct ireq *);
void osql_postabort_handle(struct ireq *);
void handle_postabort_bpfunc(struct ireq *);
int bdb_wait_for_seqnum_from_all_int(bdb_state_type *bdb_state,seqnum_type *seqnum, int *timeoutms,uint64_t txnsize, int newcoh);
int bdb_track_replication_time(bdb_state_type *bdb_state, seqnum_type *seqnum, const char *host);

void bdb_slow_replicant_check(bdb_state_type *bdb_state,
                                     seqnum_type *seqnum);
int bdb_wait_for_seqnum_from_node_nowait_int(bdb_state_type *bdb_state,
                                                    seqnum_type *seqnum,
                                                    const char *host);

int is_incoherent_complete(bdb_state_type *bdb_state,
                                         const char *host, int *incohwait);
void defer_commits(bdb_state_type *bdb_state, const char *host,
                                 const char *func);

int nodeix(const char *node);
void set_coherent_state(bdb_state_type *bdb_state,
                                      const char *hostname, int state,
                                      const char *func, int line);
void calculate_durable_lsn(bdb_state_type *bdb_state, DB_LSN *dlsn,
                                  uint32_t *gen, uint32_t flags);
unsigned long long osql_log_time(void);
void block_state_free(block_state_t *p_blkstate);
int reqlog_logl(struct reqlogger *logger, unsigned event_flag, const char *s);
void pack_tail(struct ireq *iq);

void *bdb_handle_from_ireq(const struct ireq *iq);
struct dbenv *dbenv_from_ireq(const struct ireq *iq);



static struct seqnum_wait *allocate_seqnum_wait(void){
    struct seqnum_wait *s;
    Pthread_mutex_lock(&seqnum_wait_queue_pool_lk);
    s = pool_getablk(seqnum_wait_queue_pool);
    Pthread_mutex_unlock(&seqnum_wait_queue_pool_lk);
    return s;
}

static int deallocate_seqnum_wait(struct seqnum_wait *item){
    int rc = 0;
    Pthread_mutex_lock(&seqnum_wait_queue_pool_lk);
    rc = pool_relablk(seqnum_wait_queue_pool, item);
    Pthread_mutex_unlock(&seqnum_wait_queue_pool_lk); 
    return rc;
}

int seqnum_wait_gbl_mem_init(){
    //return -1;
    work_queue = (seqnum_wait_queue *)malloc(sizeof(seqnum_wait_queue));
    if(work_queue == NULL){
        return -1;
    }
    work_queue->next_commit_timestamp = 0;
    listc_init(&work_queue->lsn_list, offsetof(struct seqnum_wait, lsn_lnk));
    listc_init(&work_queue->absolute_ts_list, offsetof(struct seqnum_wait, absolute_ts_lnk));
    pthread_mutex_init(&(work_queue->mutex), NULL);
    pthread_cond_init(&(work_queue->cond), NULL);
    
    pthread_t dummy_tid;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    // Init mem pool lock
    Pthread_mutex_init(&seqnum_wait_queue_pool_lk,NULL);
    // Create worker thread 
    Pthread_create(&dummy_tid, &attr, queue_processor, NULL);
    // Allocate the mem pool
    seqnum_wait_queue_pool = pool_setalloc_init(sizeof(struct seqnum_wait), 0, malloc, free);

    Pthread_mutex_init(&max_lsn_lk, NULL);
    max_lsn = 0;
    return 0;
}

int ts_compare(int ts1, int ts2){
    if(ts1 < ts2) return -1;
    else if(ts1 > ts2) return 1;
    else return 0;
}
//Utility method to print the work_queue
//assumes work_queue->mutex lock is held
void print_lists(){
    struct seqnum_wait *cur = NULL;
    //printf("locking at %d: \n",__LINE__);
    //pthread_mutex_lock(&(work_queue->mutex));
    printf("Size of lsn_list = %d\n",listc_size(&work_queue->lsn_list));
    printf("Size of absolute_ts_list = %d\n",listc_size(&work_queue->absolute_ts_list));
    LISTC_FOR_EACH(&work_queue->lsn_list,cur,lsn_lnk)
    {
        printf("{LSN: %d|%d}",cur->seqnum->lsn.file,cur->seqnum->lsn.offset);
        printf("->"); 
    }
    printf("\n");
    cur = NULL;
    LISTC_FOR_EACH(&work_queue->absolute_ts_list,cur, absolute_ts_lnk)
    {
        printf("{TS: %d|%d|%d}",cur->seqnum->lsn.file,cur->seqnum->lsn.offset,cur->next_ts);
        printf("->"); 
    }
    printf("\n");
    //printf("unlocking at %d: \n",__LINE__);
    //pthread_mutex_unlock(&(work_queue->mutex));
}
// Assumes that we already have lock on work_queue->mutex
void add_to_lsn_list(struct seqnum_wait *item){
    struct seqnum_wait *add_before_lsn = NULL;
    struct seqnum_wait *temp = NULL;
    logmsg(LOGMSG_DEBUG, "+++Adding item %d:%d to lsn_list\n", item->seqnum->lsn.file, item->seqnum->lsn.offset);
    LISTC_FOR_EACH_SAFE(&work_queue->lsn_list,add_before_lsn,temp,lsn_lnk)
    {
        if(log_compare(&(item->seqnum->lsn), &(add_before_lsn->seqnum->lsn)) < 0){
            listc_add_before(&(work_queue->lsn_list),item,add_before_lsn);
            printf("Successfully added new item %d:%d before %d:%d\n",item->seqnum->lsn.file, item->seqnum->lsn.offset,add_before_lsn->seqnum->lsn.file, add_before_lsn->seqnum->lsn.offset);
            return;
        }
    }
    
        // The new LSN is the highest yet... Adding to end of lsn list
    listc_abl(&(work_queue->lsn_list), item);
    printf("Successfully added new item %d:%d\n to the back of the list",item->seqnum->lsn.file, item->seqnum->lsn.offset);
    print_lists();
}

// Assumes that we already have lock on work_queue->mutex
void add_to_absolute_ts_list(struct seqnum_wait *item){
    struct seqnum_wait *add_before_ts = NULL;
    struct seqnum_wait *temp = NULL;
    logmsg(LOGMSG_DEBUG, "+++Adding item %d:%d to absolute_ts_list with next time stamp %d\n", item->seqnum->lsn.file, item->seqnum->lsn.offset, item->next_ts);
    LISTC_FOR_EACH_SAFE(&work_queue->absolute_ts_list,add_before_ts,temp,absolute_ts_lnk)
    {
        if(ts_compare(item->next_ts, add_before_ts->next_ts) <= 0){
           listc_add_before(&(work_queue->absolute_ts_list),item,add_before_ts);
            printf("Successfully added new item with ts %d before item with ts %d\n",item->next_ts,add_before_ts->next_ts);
           return;
        }
    }

    // updated next timestamp for this item is the highest yet... Adding to end of absolute_ts_list
    listc_abl(&(work_queue->absolute_ts_list), item);
    printf("Successfully added new with next_ts %d to the back of the list\n",item->next_ts);
    print_lists();
}


int add_to_seqnum_wait_queue(struct ireq *iq, seqnum_type *seqnum, int *timeoutms, uint64_t txnsize, int newcoh,int *is_wait_async){
    struct seqnum_wait *swait = allocate_seqnum_wait();
    if(swait==NULL){
        // Could not allocate memory...  return 0 here to relapse to waiting inline
        *(is_wait_async)=0;
        return 0; 
    }
    *(is_wait_async)=1;
    swait->cur_state = INIT;
    swait->iq = iq;
    swait->bdb_state = bdb_handle_from_ireq(iq);
    swait->dbenv = dbenv_from_ireq(iq);
    swait->seqnum = seqnum;
    swait->timeoutms = timeoutms;
    swait->txnsize = txnsize;
    swait->newcoh = newcoh;
    swait->do_slow_node_check = 0;
    swait->numfailed = 0;
    swait->num_incoh = 0;
    swait->we_used = 0;
    swait->base_node = NULL;
    swait->track_once = 1;
    swait->num_successfully_acked = 0;
    swait->lock_desired = 0;
    swait->next_ts = comdb2_time_epochms();
    swait->absolute_ts_lnk.prev = NULL;
    swait->absolute_ts_lnk.next = NULL;
    swait->outrc = 0;
    swait->lsn_lnk.next = NULL;
    swait->lsn_lnk.prev = NULL;

    printf("locking at %d: \n",__LINE__);
    Pthread_mutex_lock(&(work_queue->mutex));
    // Add to the lsn list in increasing order of LSN 
    printf("Adding to lsn list\n");
    add_to_lsn_list(swait);
    // Add to absolute_ts_list in increasing order of next_ts
    printf("Adding to absolute timestamp list\n");
    add_to_absolute_ts_list(swait);
    work_queue->size += 1;
    Pthread_cond_signal(&(work_queue->cond));
    printf("unlocking\nt %d: \n", __LINE__);
    Pthread_mutex_unlock(&(work_queue->mutex));
    // Signal seqnum_cond as the worker might be waiting on this condition.. 
    // We don't want the worker to wait needlessley i.e while there is still work to be done on the queue
    Pthread_mutex_lock(&(swait->bdb_state->seqnum_info->lock));
    Pthread_cond_broadcast(&(swait->bdb_state->seqnum_info->cond));
    Pthread_mutex_unlock(&(swait->bdb_state->seqnum_info->lock));
    return 1;
}
// assumes work_queue->mutex lock held
void remove_from_seqnum_wait_queue(struct seqnum_wait *item){
    listc_rfl(&work_queue->absolute_ts_list, item);
    listc_rfl(&work_queue->lsn_list, item);

}

// removes the work item from the two lists, and returns memory back to mempool
int free_work_item(struct seqnum_wait *item){
    int rc = 0;
    // First remove item from the two lists;
    Pthread_mutex_lock(&(work_queue->mutex));
    remove_from_seqnum_wait_queue(item);
    Pthread_mutex_unlock(&(work_queue->mutex));

    // Return memory to mempool
    rc = deallocate_seqnum_wait(item);
    return rc;
}

void process_work_item(struct seqnum_wait *item){
    extern pthread_mutex_t slow_node_check_lk;
    switch(item->cur_state){
        case INIT:
            /* if we were passed a child, find his parent*/
            if(item->bdb_state->parent)
            item->bdb_state = item->bdb_state->parent;

            /* short circuit if we are waiting on lsn 0:0  */
            if((item->seqnum->lsn.file == 0) && (item->seqnum->lsn.offset == 0))
            {
                // Do stuff corresponding to rc=0 in bdb_wait_for_seqnum_from_all_int
                item->outrc = 0;
                item->cur_state = COMMIT;
                goto commit_label;
            }
            logmsg(LOGMSG_DEBUG, "+++%s waiting for %s\n", __func__, lsn_to_str(item->str, &(item->seqnum->lsn)));
            item->start_time = comdb2_time_epochms();
            if(item->bdb_state->attr->durable_lsns){
                item->total_connected = net_get_sanctioned_replicants(item->bdb_state->repinfo->netinfo, REPMAX, item->connlist);
            } else {
                item->total_connected = net_get_all_commissioned_nodes(item->bdb_state->repinfo->netinfo, item->connlist); 
            }
            if (item->total_connected == 0){
                /* nobody is there to wait for! */
                item->cur_state = DONE_WAIT;
                goto done_wait_label;
            }

            if (item->track_once && item->bdb_state->attr->track_replication_times) {
                item->track_once = 0;

                Pthread_mutex_lock(&(item->bdb_state->seqnum_info->lock));
                for (int i = 0; i < item->total_connected; i++)
                    bdb_track_replication_time(item->bdb_state, item->seqnum, item->connlist[i]);
                Pthread_mutex_unlock(&(item->bdb_state->seqnum_info->lock));

                /* once a second, see if we have any slow replicants */
                item->now = comdb2_time_epochms();
                Pthread_mutex_lock(&slow_node_check_lk);
                if (item->now - last_slow_node_check_time > 1000) {
                    if (item->bdb_state->attr->track_replication_times) {
                        last_slow_node_check_time = item->now;
                        item->do_slow_node_check = 1;
                    }
                }
                Pthread_mutex_unlock(&slow_node_check_lk);

                /* do the slow replicant check - only if we need to ... */
                if (item->do_slow_node_check &&
                    item->bdb_state->attr->track_replication_times &&
                    (item->bdb_state->attr->warn_slow_replicants ||
                     item->bdb_state->attr->make_slow_replicants_incoherent)) {
                    bdb_slow_replicant_check(item->bdb_state, item->seqnum);
                }
            }
            // We're done with INIT state.. Move to FIRST_ACK and fall through this case.
            item->cur_state = FIRST_ACK;
        case FIRST_ACK:
            item->cur_state = FIRST_ACK;
            if(((comdb2_time_epochms() - item->start_time) < item->bdb_state->attr->rep_timeout_maxms) &&
                    !(item->lock_desired = bdb_lock_desired(item->bdb_state))){
                item->numnodes = 0;
                item->numskip = 0;
                item->numwait = 0;

                if(item->bdb_state->attr->durable_lsns){
                    item->total_connected = net_get_sanctioned_replicants(item->bdb_state->repinfo->netinfo, REPMAX, item->connlist);
                } else {
                    item->total_connected = net_get_all_commissioned_nodes(item->bdb_state->repinfo->netinfo, item->connlist); 
                }
                if (item->total_connected == 0){
                    /* nobody is there to wait for! */
                    item->cur_state = DONE_WAIT;
                    goto done_wait_label;
                }
                for (int i = 0; i < item->total_connected; i++) {
                    int wait = 0;
                    /* is_incoherent returns 0 for COHERENT & INCOHERENT_WAIT */
                    if (!(is_incoherent_complete(item->bdb_state, item->connlist[i], &wait))) {
                        item->nodelist[item->numnodes] = item->connlist[i];
                        item->numnodes++;
                        if (wait)
                            item->numwait++;
                    } else {
                        item->numskip++;
                        item->num_incoh++;
                    }
                }

                if (item->numnodes == 0) {
                    item->cur_state = DONE_WAIT;
                    goto done_wait_label;
                }
                
                for (int i = 0; i < item->numnodes; i++) {
                    if (item->bdb_state->rep_trace)
                        logmsg(LOGMSG_DEBUG,
                               "+++checking for initial NEWSEQ from node %s of >= <%s>\n",
                               item->nodelist[i], lsn_to_str(item->str, &(item->seqnum->lsn)));    
                    int rc = bdb_wait_for_seqnum_from_node_nowait_int(item->bdb_state, item->seqnum, item->nodelist[i]);
                    if(rc == 0){
                        item->base_node = item->nodelist[i];
                        item->num_successfully_acked++;
                        item->end_time = comdb2_time_epochms();
                        item->we_used = item->end_time - item->start_time;
                        item->waitms = (item->we_used * item->bdb_state->attr->rep_timeout_lag) / 100;
                        if (item->waitms < item->bdb_state->attr->rep_timeout_minms){
                            // If the first node responded really fast, we don't want to impose too harsh a timeout on the remaining nodes
                            item->waitms = item->bdb_state->attr->rep_timeout_minms;
                        }
                        if (item->bdb_state->rep_trace)
                            logmsg(LOGMSG_DEBUG, "+++fastest node to <%s> was %dms, will wait "
                                            "another %dms for remainder\n",
                                    lsn_to_str(item->str, &(item->seqnum->lsn)), item->we_used, item->waitms);
                        // WE got first ack.. move to next state.. 
                        item->cur_state = GOT_FIRST_ACK;
                        goto got_first_ack_label;
                    }
                }
                // If we get here, then none of the replicants have caught up yet, AND we are still within rep_timeout_maxms 
                // Let's wait for one second and check again.
                printf("locking at %d: \n",__LINE__);
                Pthread_mutex_lock(&(work_queue->mutex));
                item->next_ts = comdb2_time_epochms() + 500;
                listc_rfl(&work_queue->absolute_ts_list, item);
                add_to_absolute_ts_list(item);
                printf("unlocking at %d: \n",__LINE__);
                Pthread_mutex_unlock(&(work_queue->mutex));
                // We break out of the switch as we are done processing this item (for now)
                break;
              }
           else{
               // Either we timed out, or someone else is asking for bdb lock i.e master swing
               if(item->lock_desired){
                   logmsg(LOGMSG_DEBUG,"+++lock desired, not waiting for initial replication of <%s>\n", lsn_to_str(item->str, &(item->seqnum->lsn)));
                   // Not gonna wait for anymore acks...Set rcode and go to DONE_WAIT;
                   if(item->bdb_state->attr->durable_lsns){
                       item->outrc = BDBERR_NOT_DURABLE;
                   }
                   else{
                       item->outrc = -1;
                   }
                   item->cur_state = COMMIT;
                   goto commit_label;
               }
               // we timed out i.e exceeded bdb->attr->rep_timeout_maxms
               logmsg(LOGMSG_DEBUG, "+++timed out waiting for initial replication of <%s>\n",
                       lsn_to_str(item->str, &(item->seqnum->lsn)));
               item->end_time = comdb2_time_epochms(); 
               item->we_used = item->end_time - item->start_time;
               item->waitms = 0; // We've already exceeded max timeout... Not gonna wait anymore
               *(item->timeoutms) = item->we_used + item->waitms;
               item->cur_state = GOT_FIRST_ACK;
           } 
        case GOT_FIRST_ACK: got_first_ack_label:
           // First check if someone else wants bdb_lock a.k.a master swing
           if(bdb_lock_desired(item->bdb_state)){
               logmsg(LOGMSG_DEBUG,"+++%s line %d early exit because lock-is-desired\n",__func__,__LINE__);
               if(item->bdb_state->attr->durable_lsns){
                   item->outrc = BDBERR_NOT_DURABLE;
               }
               else{
                   item->outrc = -1;
               }
               item->cur_state = COMMIT;
               goto commit_label;
           }
           // Either we've received first ack or we've timed out
            item->numfailed = 0;
            int acked = 0;
            for(int i=0;i<item->numnodes;i++){
                if(item->nodelist[i] == item->base_node)
                    continue;      
                if (item->bdb_state->rep_trace)
                    logmsg(LOGMSG_DEBUG,
                           "+++checking for NEWSEQ from node %s of >= <%s> timeout %d\n",
                           item->nodelist[i], lsn_to_str(item->str, &(item->seqnum->lsn)), item->waitms);
                int rc = bdb_wait_for_seqnum_from_node_nowait_int(item->bdb_state, item->seqnum, item->nodelist[i]);
                if (bdb_lock_desired(item->bdb_state)) {
                    logmsg(LOGMSG_DEBUG,
                           "+++%s line %d early exit because lock-is-desired\n", __func__,
                           __LINE__);
                    if(item->bdb_state->attr->durable_lsns){
                       item->outrc = BDBERR_NOT_DURABLE;
                    }
                    else{
                       item->outrc = -1;
                    }
                    item->cur_state = COMMIT;
                    goto commit_label;
                }
                if (rc == -999){
                    logmsg(LOGMSG_DEBUG, "+++node %s hasn't caught up yet, base node "
                                    "was %s",
                            item->nodelist[i],item->base_node);
                    item->numfailed++;
                    // If even one replicant hasn't caught up, we come out of the loop and wait
                    // No point in checking the others as we will have to wait anyways
                    break;
                }
                else if (rc == 0){
                    acked++;
                }
            }
            if(item->numfailed == 0){
                // Awesome! Everyone's caught up. 
                item->num_successfully_acked += acked; 
                item->cur_state = DONE_WAIT;
                goto done_wait_label;
            }
            else{
                //If we are still within waitms timeout, we still have hope! 
                //Modify position of item appropriately in absolute_ts_list
                if((comdb2_time_epochms() - item->start_time) < item->waitms){
                    // Change position of current work item in absolute_ts_list based on new_ts (the next absolute timestamp that this node has to be worked on again 
                    printf("locking at %d: \n",__LINE__);
                    Pthread_mutex_lock(&(work_queue->mutex));
                    item->next_ts = comdb2_time_epochms() + item->waitms; 
                    listc_rfl(&work_queue->absolute_ts_list, item);
                    add_to_absolute_ts_list(item);
                    printf("unlocking at %d: \n",__LINE__);
                    Pthread_mutex_unlock(&(work_queue->mutex));
                    // We break out and handle next iten in the work queue
                    break;
                }
                else{
                    // We have timed out with numfailed !=0
                    // Go through list of connected nodes one last time to find how many acked or how many failed.
                    // Mark those that failed as incoherent
                    item->numfailed = 0;
                    for(int i=0;i<item->numnodes;i++){
                        if(item->nodelist[i] == item->base_node)
                            continue;      
                        if (item->bdb_state->rep_trace)
                            logmsg(LOGMSG_DEBUG,
                                   "+++checking for NEWSEQ from node %s of >= <%s> timeout %d\n",
                                   item->nodelist[i], lsn_to_str(item->str, &(item->seqnum->lsn)), item->waitms);
                        int rc = bdb_wait_for_seqnum_from_node_nowait_int(item->bdb_state, &(item->bdb_state->seqnum_info->seqnums[nodeix(item->bdb_state->repinfo->master_host)]), item->nodelist[i]);
                        if (bdb_lock_desired(item->bdb_state)) {
                            logmsg(LOGMSG_DEBUG,
                                   "+++%s line %d early exit because lock-is-desired\n", __func__,
                                   __LINE__);
                            if(item->bdb_state->attr->durable_lsns){
                               item->outrc = BDBERR_NOT_DURABLE;
                            }
                            else{
                               item->outrc = -1;
                            }
                            item->cur_state = COMMIT;
                            goto commit_label;
                        }
                        if (rc == -999){
                            logmsg(LOGMSG_DEBUG, "+++node %s hasn't caught up yet, base node "
                                            "was %s",
                                    item->nodelist[i],item->base_node);
                            item->numfailed++;
                            // Extract seqnum
                            Pthread_mutex_lock(&(item->bdb_state->seqnum_info->lock));
                            item->nodegen = item->bdb_state->seqnum_info->seqnums[nodeix(item->nodelist[i])].generation;
                            item->nodelsn = item->bdb_state->seqnum_info->seqnums[nodeix(item->nodelist[i])].lsn;
                            Pthread_mutex_unlock(&(item->bdb_state->seqnum_info->lock));
                            // We now mark the node incoherent
                            Pthread_mutex_lock(&(item->bdb_state->coherent_state_lock));
                            if(item->bdb_state->coherent_state[nodeix(item->nodelist[i])] == STATE_COHERENT){
                                defer_commits(item->bdb_state, item->nodelist[i], __func__);
                                if(item->bdb_state->attr->catchup_on_commit && item->bdb_state->attr->catchup_window){
                                    item->masterlsn = &(item->bdb_state->seqnum_info->seqnums[nodeix(item->bdb_state->repinfo->master_host)].lsn);
                                    item->cntbytes = subtract_lsn(item->bdb_state, item->masterlsn, &(item->nodelsn));
                                    set_coherent_state(item->bdb_state, item->nodelist[i], (item->cntbytes < item->bdb_state->attr->catchup_window)?STATE_INCOHERENT_WAIT: STATE_INCOHERENT,__func__, __LINE__);
   
                                }
                                else{
                                    set_coherent_state(item->bdb_state, item->nodelist[i], STATE_INCOHERENT,__func__, __LINE__);
                                }
                                // change next_commit_timestamp for the work queue, if new value of coherency_commit_timestamp is larger,
                                //  than current value of coherency_commit_timestamp 
                                work_queue->next_commit_timestamp = (work_queue->next_commit_timestamp < coherency_commit_timestamp)?coherency_commit_timestamp:work_queue->next_commit_timestamp;
                                item->bdb_state->last_downgrade_time[nodeix(item->nodelist[i])] = gettimeofday_ms();
                                item->bdb_state->repinfo->skipsinceepoch = comdb2_time_epoch();
                            }
                            Pthread_mutex_unlock(&(item->bdb_state->coherent_state_lock));
                            
                        }
                        else if (rc == 0){
                            item->num_successfully_acked++;
                        }
                    }
                    item->cur_state = DONE_WAIT;
                    goto done_wait_label;
                }
            }
        case DONE_WAIT: done_wait_label:
            if (!item->numfailed && !item->numskip && !item->numwait &&
                item->bdb_state->attr->remove_commitdelay_on_coherent_cluster &&
                item->bdb_state->attr->commitdelay) {
                logmsg(LOGMSG_DEBUG, "+++Cluster is in sync, removing commitdelay\n");
                item->bdb_state->attr->commitdelay = 0;
            }

            if (item->numfailed) {
                item->outrc = -1;
            }

            if (item->bdb_state->attr->durable_lsns) {
                uint32_t cur_gen;
                static uint32_t not_durable_count;
                static uint32_t durable_count;
                extern int gbl_durable_wait_seqnum_test;

                int istest = 0;
                int was_durable = 0;

                uint32_t cluster_size = item->total_connected + 1;
                uint32_t number_with_this_update = item->num_successfully_acked + 1;
                uint32_t durable_target = (cluster_size / 2) + 1;

                if ((number_with_this_update < durable_target) ||
                    (gbl_durable_wait_seqnum_test && (istest = (0 == (rand() % 20))))) {
                    if (istest)
                        logmsg(LOGMSG_DEBUG, 
                                "+++%s return not durable for durable wait seqnum test\n", __func__);
                    item->outrc = BDBERR_NOT_DURABLE;
                    not_durable_count++;
                    was_durable = 0;
                } else {
                    /* We've released the bdb lock at this point- the master could have
                     * changed while
                     * we were waiting for this to propogate.  The simple fix: get
                     * rep_gen & return
                     * not durable if it's changed */
                    bdb_state_type *bdb_state = item->bdb_state;// Need this so that macro below can work correctly
                    BDB_READLOCK("wait_for_seqnum");
                    item->bdb_state->dbenv->get_rep_gen(item->bdb_state->dbenv, &cur_gen);
                    BDB_RELLOCK();

                    if (cur_gen != item->seqnum->generation) {
                        item->outrc = BDBERR_NOT_DURABLE;
                        not_durable_count++;
                        was_durable = 0;
                    } else {
                        Pthread_mutex_lock(&(item->bdb_state->durable_lsn_lk));
                        item->bdb_state->dbenv->set_durable_lsn(item->bdb_state->dbenv,
                                                          &(item->seqnum->lsn), cur_gen);
                        if (item->seqnum->lsn.file == 0) {
                            logmsg(LOGMSG_DEBUG, "+++%s line %d: aborting on insane durable lsn\n",
                                    __func__, __LINE__);
                            abort();
                        }
                        Pthread_mutex_unlock(&(item->bdb_state->durable_lsn_lk));
                        durable_count++;
                        was_durable = 1;
                    }
                }

                if (item->bdb_state->attr->wait_for_seqnum_trace) {
                    DB_LSN calc_lsn;
                    uint32_t calc_gen;
                    calculate_durable_lsn(item->bdb_state, &calc_lsn, &calc_gen, 1);
                    /* This is actually okay- do_ack and the thread which broadcasts
                     * seqnums can race against each other.  If we got a majority of 
                     * these during the commit we are okay */
                    if (was_durable && log_compare(&calc_lsn, &(item->seqnum->lsn)) < 0) {
                        logmsg(LOGMSG_DEBUG,
                               "+++ERROR: calculate_durable_lsn trails seqnum, "
                               "but this is durable (%d:%d vs %d:%d)?\n",
                               calc_lsn.file, calc_lsn.offset, item->seqnum->lsn.file,
                               item->seqnum->lsn.offset);
                    }
                    logmsg(LOGMSG_DEBUG, 
                        "+++Last txn was %s, tot_connected=%d tot_acked=%d, "
                        "durable-commit-count=%u not-durable-commit-count=%u "
                        "commit-lsn=[%d][%d] commit-gen=%u calc-durable-lsn=[%d][%d] "
                        "calc-durable-gen=%u\n",
                        was_durable ? "durable" : "not-durable", item->total_connected,
                        item->num_successfully_acked, durable_count, not_durable_count,
                        item->seqnum->lsn.file, item->seqnum->lsn.offset, item->seqnum->generation,
                        calc_lsn.file, calc_lsn.offset, calc_gen);
                }
            }
            int now = comdb2_time_epochms();
            printf("locking at %d: \n",__LINE__);
            Pthread_mutex_lock(&(work_queue->mutex));
            if(now > work_queue->next_commit_timestamp){
            printf("unlocking at %d: \n",__LINE__);
                Pthread_mutex_unlock(&(work_queue->mutex));
                // We're safe to commit and return control to client
                item->cur_state = COMMIT;
                goto commit_label;
            }
            else{
                item->cur_state = COMMIT;
                item->next_ts = work_queue->next_commit_timestamp;
                listc_rfl(&work_queue->absolute_ts_list, item);
                add_to_absolute_ts_list(item);
            printf("unlocking at %d: \n",__LINE__);
                Pthread_mutex_unlock(&(work_queue->mutex));
                break;
            }
        case COMMIT: commit_label:
            if(item->outrc == BDBERR_NOT_DURABLE){
                item->outrc = ERR_NOT_DURABLE;
                logmsg(LOGMSG_DEBUG, "+++We\'ve committed to the btree, but not replicated: asking client to retry.. \n");
            }
            if (item->outrc == 0) {
                /* Committed new sqlite_stat1 statistics from analyze - reload sqlite
                 * engines */
                item->iq->dbenv->txns_committed++;
                if (item->iq->dbglog_file) {
                    dbglog_dump_write_stats(item->iq);
                    sbuf2close(item->iq->dbglog_file);
                    item->iq->dbglog_file = NULL;
                }
            } else {
                item->iq->dbenv->txns_aborted++;
            }
    /*if (item->iq->txnsize > item->iq->dbenv->biggest_txn)
        item->iq->dbenv->biggest_txn = item->iq->txnsize;
    item->iq->dbenv->total_txn_sz = item->iq->txnsize;
    item->iq->dbenv->num_txns++;
    if (item->iq->timeoutms > item->iq->dbenv->max_timeout_ms)
        item->iq->dbenv->max_timeout_ms = item->iq->timeoutms;
    item->iq->dbenv->total_timeouts_ms += item->iq->timeoutms;
    if (item->iq->reptimems > item->iq->dbenv->max_reptime_ms)
        item->iq->dbenv->max_reptime_ms = item->iq->reptimems;
    item->iq->dbenv->total_reptime_ms += item->iq->reptimems;

    if (item->iq->debug) {
        uint64_t rate;
        if (item->iq->reptimems)
            rate = item->iq->txnsize / item->iq->reptimems;
        else
            rate = 0;
        reqprintf(item->iq, "%p:TRANSACTION SIZE %llu TIMEOUT %d REPTIME %d RATE "
                      "%llu",
                  trans, item->iq->txnsize, item->iq->timeoutms, item->iq->reptimems, rate);
    }

    int diff_time_micros = (int)reqlog_current_us(item->iq->reqlogger);

    Pthread_mutex_lock(&commit_stat_lk);
    n_commit_time += diff_time_micros;
    n_commits++;
    Pthread_mutex_unlock(&commit_stat_lk);

    if (item->outrc == 0) {
        if (item->iq->__limits.maxcost_warn &&
            (item->iq->cost > iq->__limits.maxcost_warn)) {
            logmsg(LOGMSG_WARN, "[%s] warning: transaction exceeded cost threshold "
                            "(%f >= %f)\n",
                    item->iq->corigin, item->iq->cost, item->iq->__limits.maxcost_warn);
        }
    }*/
            bdb_checklock(thedb->bdb_env);    
            item->iq->timings.req_finished = osql_log_time();
            item->iq->timings.retries++;
            osql_blkseq_unregister(item->iq);
            if(item->iq->backed_out)
            {
                item->iq->should_wait_async = 0;
                item->outrc=trans_wait_for_last_seqnum(item->iq,item->iq->source_host);
            }
            if (item->outrc != 0 && item->outrc != ERR_BLOCK_FAILED && item->outrc != ERR_READONLY &&
                item->outrc != ERR_SQL_PREP && item->outrc != ERR_NO_AUXDB && item->outrc != ERR_INCOHERENT &&
                item->outrc != ERR_SC_COMMIT && item->outrc != ERR_CONSTR && item->outrc != ERR_TRAN_FAILED &&
                item->outrc != ERR_CONVERT_DTA && item->outrc != ERR_NULL_CONSTRAINT &&
                item->outrc != ERR_CONVERT_IX && item->outrc != ERR_BADREQ && item->outrc != ERR_RMTDB_NESTED &&
                item->outrc != ERR_NESTED && item->outrc != ERR_NOMASTER && item->outrc != ERR_READONLY &&
                item->outrc != ERR_VERIFY && item->outrc != RC_TRAN_CLIENT_RETRY &&
                item->outrc != RC_INTERNAL_FORWARD && item->outrc != RC_INTERNAL_RETRY &&
                item->outrc != ERR_TRAN_TOO_BIG && /* THIS IS SENT BY BLOCKSQL WHEN TOOBIG */
                item->outrc != 999 && item->outrc != ERR_ACCESS && item->outrc != ERR_UNCOMMITABLE_TXN &&
                (item->outrc != ERR_NOT_DURABLE || !item->iq->sorese.type)) {
                /* XXX CLIENT_RETRY DOESNT ACTUALLY CAUSE A RETRY USUALLY, just
                   a bad rc to the client! */
                /*rc = RC_TRAN_CLIENT_RETRY;*/

                item->outrc = ERR_NOMASTER;
            }

            javasp_trans_end(item->iq->jsph);
            block_state_free(item->iq->blkstate);
            if(item->iq->usedb && item->iq->usedb->tablename)
                reqlog_logl(item->iq->reqlogger, REQL_INFO, item->iq->usedb->tablename);

            // Send back the response -> code reference db/sltdbt.c line 297
            if(item->iq->debug){
                reqprintf(item->iq, "iq->reply_len=%td RC %d\n",
                          (ptrdiff_t) (item->iq->p_buf_out - item->iq->p_buf_out_start), item->outrc);
            }
            /* pack data at tail of reply */
            pack_tail(item->iq);
            if(item->iq->sorese.type){
                if (item->outrc && (!item->iq->sorese.rcout || item->outrc == ERR_NOT_DURABLE))
                    item->iq->sorese.rcout = item->outrc;

                int sorese_rc = item->outrc;
                if (item->outrc == 0 && item->iq->sorese.rcout == 0 &&
                    item->iq->errstat.errval == COMDB2_SCHEMACHANGE_OK) {
                    // pretend error happend to get errstat shipped to replicant
                    sorese_rc = 1;
                } else {
                    item->iq->errstat.errval = item->iq->sorese.rcout;
                }
                if (item->iq->debug) {
                    uuidstr_t us;
                    reqprintf(item->iq,
                              "sorese returning rqid=%llu uuid=%s node=%s type=%d "
                              "nops=%d rcout=%d retried=%d RC=%d errval=%d\n",
                              item->iq->sorese.rqid, comdb2uuidstr(item->iq->sorese.uuid, us),
                              item->iq->sorese.host, item->iq->sorese.type, item->iq->sorese.nops,
                              item->iq->sorese.rcout, item->iq->sorese.osql_retry, item->outrc,
                              item->iq->errstat.errval);
                }

                if (item->iq->sorese.rqid == 0)
                    abort();
                logmsg(LOGMSG_USER, "Calling osql_comm_signal_sqlthr_rc from %s:%d with sorese_rc: %d\n", __func__, __LINE__, sorese_rc);
                osql_comm_signal_sqlthr_rc(&item->iq->sorese, &item->iq->errstat, sorese_rc);

                item->iq->timings.req_sentrc = osql_log_time();
            } else if (item->iq->is_dumpresponse) {
                signal_buflock(item->iq->request_data);
                if (item->outrc != 0) {
                    logmsg(LOGMSG_DEBUG,
                           "\n +++Unexpected error %d in block operation", item->outrc);
                }
            } else if (item->iq->is_fromsocket) {
                net_delay(item->iq->frommach);
                /* process socket end request */
                if (item->iq->is_socketrequest) {
                    if (item->iq->sb == NULL) {
                        item->outrc = offload_comm_send_blockreply(
                            item->iq->frommach, item->iq->rqid, item->iq->p_buf_out_start,
                            item->iq->p_buf_out - item->iq->p_buf_out_start, item->outrc);
                        free_bigbuf_nosignal(item->iq->p_buf_out_start);
                    } else {
                        /* The tag request is handled locally.
                           We know for sure `request_data' is a `buf_lock_t'. */
                        struct buf_lock_t *p_slock =
                            (struct buf_lock_t *)item->iq->request_data;
                        {
                            Pthread_mutex_lock(&p_slock->req_lock);
                            if (p_slock->reply_state == REPLY_STATE_DISCARD) {
                                Pthread_mutex_unlock(&p_slock->req_lock);
                                cleanup_lock_buffer(p_slock);
                                free_bigbuf_nosignal(item->iq->p_buf_out_start);
                            } else {
                                sndbak_open_socket(
                                    item->iq->sb, item->iq->p_buf_out_start,
                                    item->iq->p_buf_out - item->iq->p_buf_out_start, item->outrc);
                                free_bigbuf(item->iq->p_buf_out_start, item->iq->request_data);
                                Pthread_mutex_unlock(&p_slock->req_lock);
                            }
                        }
                    }
                    item->iq->request_data = item->iq->p_buf_out_start = NULL;
                } else {
                    sndbak_socket(item->iq->sb, item->iq->p_buf_out_start,
                                  item->iq->p_buf_out - item->iq->p_buf_out_start, item->outrc);
                    free(item->iq->p_buf_out_start);
                }
                item->iq->p_buf_out_end = item->iq->p_buf_out_start = item->iq->p_buf_out = NULL;
                item->iq->p_buf_in_end = item->iq->p_buf_in = NULL;
            } else if (comdb2_ipc_sndbak_len_sinfo) {
                comdb2_ipc_sndbak_len_sinfo(item->iq, item->outrc);
            }
            /* Unblock anybody waiting for stuff that was added in this transaction. */
            clear_trans_from_repl_list(item->iq->repl_list);

            /* records were added to queues, and we committed successfully.  wake
             * up queue consumers. */
            if (item->outrc == 0 && item->iq->num_queues_hit > 0) {
                if (item->iq->num_queues_hit > MAX_QUEUE_HITS_PER_TRANS) {
                    /* good heavens.  wake up all consumers */
                    dbqueuedb_wake_all_consumers_all_queues(item->iq->dbenv, 0);
                } else {
                    unsigned ii;
                    for (ii = 0; ii < item->iq->num_queues_hit; ii++)
                        dbqueuedb_wake_all_consumers(item->iq->queues_hit[ii], 0);
                }
            }

            /* Finish off logging. */
            /*if (item->iq->blocksql_tran) {
                osql_bplog_reqlog_queries(item->iq);
            }
            reqlog_end_request(item->iq->reqlogger, item->outrc, __func__, __LINE__);
            release_node_stats(NULL, NULL, item->iq->frommach);*/
            if (gbl_print_deadlock_cycles)
                osql_snap_info = NULL;

            if (item->iq->sorese.type) {
                if (item->iq->p_buf_out_start) {
                    free(item->iq->p_buf_out_start);
                    item->iq->p_buf_out_end = item->iq->p_buf_out_start = item->iq->p_buf_out = NULL;
                    item->iq->p_buf_in_end = item->iq->p_buf_in = NULL;
                }
            }

            /* Make sure we do not leak locks */
            bdb_checklock(thedb->bdb_env); 
            //comdb2bma_yield_all();
            //LOCK(&lock)
            //{
                if (item->iq->usedb && item->iq->ixused >= 0 &&
                    item->iq->ixused < item->iq->usedb->nix &&
                    item->iq->usedb->ixuse) {
                    item->iq->usedb->ixuse[item->iq->ixused] += item->iq->ixstepcnt;
                }
                item->iq->ixused = -1;
                item->iq->ixstepcnt = 0;

                if (item->iq->dbglog_file) {
                    sbuf2close(item->iq->dbglog_file);
                    item->iq->dbglog_file = NULL;
                }
                if (item->iq->nwrites) {
                    free(item->iq->nwrites);
                    item->iq->nwrites = NULL;
                }
                if (item->iq->vfy_genid_hash) {
                    hash_free(item->iq->vfy_genid_hash);
                    item->iq->vfy_genid_hash = NULL;
                }
                if (item->iq->vfy_genid_pool) {
                    pool_free(item->iq->vfy_genid_pool);
                    item->iq->vfy_genid_pool = NULL;
                }
                if (item->iq->sorese.osqllog) {
                    sbuf2close(item->iq->sorese.osqllog);
                    item->iq->sorese.osqllog = NULL;
                }
                item->iq->vfy_genid_track = 0;
#if 0
                fprintf(stderr, "%s:%d: THD=%d relablk iq=%p\n", __func__, __LINE__, pthread_self(), item->iq);
#endif
                logmsg(LOGMSG_DEBUG, "+++%s:%d releasing iq\n",__func__,__LINE__);
                destroy_ireq(item->dbenv, item->iq); /* this request is done, so release resource */
                //pool_relablk(p_reqs, item->iq); 
                                                
            //}
            //UNLOCK(&lock);
            // We are done handling this request completely.
            item->cur_state = FREE; 
            break;
    }// End of Switch
}

void *queue_processor(void *arg){
    struct seqnum_wait *item = NULL;
    struct seqnum_wait *next_item = NULL;
    struct timespec waittime;
    int wait_rc = 0;
    int now = 0;
    logmsg(LOGMSG_DEBUG,"+++Starting seqnum_wait worker thread\n");
    while(1){
                printf("locking at %d: \n",__LINE__);
        Pthread_mutex_lock(&(work_queue->mutex));
        while(listc_size(&(work_queue->lsn_list))==0){
            printf("unlocking at %d: \n",__LINE__);
            logmsg(LOGMSG_DEBUG,"+++LSN list is empty.... Waiting for work item\n");
            Pthread_cond_wait(&(work_queue->cond),&(work_queue->mutex));
            printf("locking at %d: \n",__LINE__);
        }
                printf("unlocking at %d: \n",__LINE__);
        Pthread_mutex_unlock(&(work_queue->mutex));
        logmsg(LOGMSG_DEBUG,"+++Finally! got a work item\n");

        // if timed_wait below resulted in a timeout, we need to go over absolute_ts_list
        if(wait_rc == ETIMEDOUT){
            logmsg(LOGMSG_DEBUG,"+++Timed out on conditional wait.. Going over absolute ts list\n");
                printf("locking at %d: \n",__LINE__);
            Pthread_mutex_lock(&(work_queue->mutex));
            item = LISTC_TOP(&(work_queue->absolute_ts_list));
                printf("unlocking at %d: \n",__LINE__);
            Pthread_mutex_unlock(&(work_queue->mutex));
            now = comdb2_time_epochms();
            while(item!=NULL && (item->next_ts <= now)){
                if(item->cur_state == FREE){
                    logmsg(LOGMSG_DEBUG, "+++Done processing work item: %d:%d... Freeing it\n",item->seqnum->lsn.file, item->seqnum->lsn.offset);
                    Pthread_mutex_lock(&(work_queue->mutex));
                    next_item = item->lsn_lnk.next;
                    Pthread_mutex_unlock(&(work_queue->mutex));
                    free_work_item(item);
                    item = next_item;
                    continue;
                }
                logmsg(LOGMSG_DEBUG, "+++Processing work item: %d:%d, in state: %d\n",item->seqnum->lsn.file, item->seqnum->lsn.offset,item->cur_state);
                process_work_item(item);
                printf("locking at %d: \n",__LINE__);
                Pthread_mutex_lock(&(work_queue->mutex));
                item = item->absolute_ts_lnk.next;
                printf("unlocking at %d: \n",__LINE__);
                Pthread_mutex_unlock(&(work_queue->mutex));
            }
        }
        else{
            // wait_rc == 0, which means either this is the first run of infinite while loop , or..
            // the timed_wait below was signalled...i.e... we got new seqnum -> we iterate over lsn_list upto max_lsn_seen 
            logmsg(LOGMSG_DEBUG, "+++Either got new seqnum or going through lsn list for the first time\n");
                printf("locking at %d: \n",__LINE__);
            Pthread_mutex_lock(&(work_queue->mutex));
            item = LISTC_TOP(&(work_queue->lsn_list));
                printf("unlocking at %d: \n",__LINE__);
            Pthread_mutex_unlock(&(work_queue->mutex));
            while(item!=NULL){
                if(item->cur_state == FREE){
                    logmsg(LOGMSG_DEBUG, "+++Done processing work item: %d:%d... Freeing it\n",item->seqnum->lsn.file, item->seqnum->lsn.offset);
                    Pthread_mutex_lock(&(work_queue->mutex));
                    next_item = item->lsn_lnk.next;
                    Pthread_mutex_unlock(&(work_queue->mutex));
                    free_work_item(item);
                    item = next_item;
                    continue;
                }
                logmsg(LOGMSG_DEBUG, "+++Processing work item: %d:%d, in state: %d\n",item->seqnum->lsn.file, item->seqnum->lsn.offset,item->cur_state);
                process_work_item(item);
                printf("locking at %d: \n",__LINE__);
                Pthread_mutex_lock(&(work_queue->mutex));
                item = item->lsn_lnk.next;
                printf("unlocking at %d: \n",__LINE__);
                Pthread_mutex_unlock(&(work_queue->mutex));
            }
        }
        // Check first item on absolute_ts_list 
                printf("locking at %d: \n",__LINE__);
        Pthread_mutex_lock(&(work_queue->mutex));
        item = LISTC_TOP(&(work_queue->absolute_ts_list));
                printf("unlocking at %d: \n",__LINE__);
        Pthread_mutex_unlock(&(work_queue->mutex));
        if(item!=NULL){
            int now = comdb2_time_epochms();
            if(now <  item->next_ts){
                // we are early, wait till the earliest time that a node has to be checked
                // Or, till we get signalled by got_new_seqnum_from_node
                setup_waittime(&waittime, item->next_ts-now);
                logmsg(LOGMSG_DEBUG, "+++Current time :%d before earliest time a work item has to be checked:%d ... going into timedcondwait\n",now,item->next_ts);
                Pthread_mutex_lock(&(item->bdb_state->seqnum_info->lock));
                wait_rc = pthread_cond_timedwait(&(item->bdb_state->seqnum_info->cond),
                                            &(item->bdb_state->seqnum_info->lock), &waittime); 
                if(wait_rc !=   ETIMEDOUT)
                    Pthread_mutex_unlock(&(item->bdb_state->seqnum_info->lock));
            }
            else{
                // We are at(or have crossed) the smallest next_ts in absolute_ts_list
                wait_rc = ETIMEDOUT;
            }
        }
        else{
            //Nothing on the lists... reset wait_rc
            wait_rc = 0;
        }
    }
}

void seqnum_wait_cleanup(){
    struct seqnum_wait *item = NULL;

    //FREE THE WORK QUEUE
    printf("Cleaning up the work queue\n");
    pthread_mutex_lock(&work_queue->mutex);
    LISTC_FOR_EACH(&work_queue->absolute_ts_list,item,absolute_ts_lnk)
    {
        free(listc_rfl(&work_queue->absolute_ts_list, item));
    }

    LISTC_FOR_EACH(&work_queue->lsn_list,item,lsn_lnk)
    {
        free(listc_rfl(&work_queue->lsn_list, item));
    }
    //listc_free(&work_queue->absolute_ts_list);
    //listc_free(&work_queue->lsn_list);
    pthread_mutex_unlock(&work_queue->mutex);
    printf("Finished cleaning work queue\n");
    printf("releasing work_queue\n");
    free(work_queue);
    printf("work queue release\n");
    // FREE THE MEM POOL
    printf("releasing threadpool\n");
    pthread_mutex_lock(&seqnum_wait_queue_pool_lk);
    pool_free(seqnum_wait_queue_pool);
    pthread_mutex_unlock(&seqnum_wait_queue_pool_lk);
    printf("cleanup done\n");
}
