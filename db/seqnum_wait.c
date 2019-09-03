#include<seqnum_wait.h>
seqnum_wait_queue *work_queue = NULL;
static pool_t *seqnum_wait_queue_pool = NULL;
static pthread_mutex_t seqnum_wait_queue_pool_lk;
pthread_mutex_t max_lsn_lk;
int max_lsn;

void *queue_processor(void *);
extern void osql_postcommit_handle(struct ireq *);
extern void handle_postcommit_bpfunc(struct ireq *);
extern void osql_postabort_handle(struct ireq *);
extern void handle_postabort_bpfunc(struct ireq *);
extern int bdb_wait_for_seqnum_from_all_int(bdb_state_type *bdb_state,seqnum_type *seqnum, int *timeoutms,uint64_t txnsize, int newcoh);
static struct seqnum_wait *allocate_seqnum_wait(void){
    struct seqnum_wait *s;
    Pthread_mutex_lock(&seqnum_wait_queue_pool_lk);
    s = pool_getablk(seqnum_wait_queue_pool);
    Pthread_mutex_unlock(&seqnum_wait_queue_pool_lk);
    return s;
}

int seqnum_wait_gbl_mem_init(){
    work_queue = (seqnum_wait_queue *)malloc(sizeof(seqnum_wait_queue));
    if(work_queue == NULL){
        return -1;
    }
    Pthread_mutex_init(&work_queue->mutex, NULL);
    Pthread_cond_init(&work_queue->got_new_item_cond, NULL);
    work_queue->size = 0;
    listc_init(&work_queue->lsn_list, offsetof(struct seqnum_wait, lsn_lnk));
    listc_init(&work_queue->absolute_ts_list, offsetof(struct seqnum_wait, absolute_ts_lnk));
    
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

int add_to_seqnum_wait_queue(struct ireq *iq,bdb_state_type *bdb_state, seqnum_type *seqnum, int *timeoutms, uint64_t txnsize, int newcoh){
    struct seqnum_wait *swait = allocate_seqnum_wait(); 
    struct seqnum_wait *add_before_lsn = NULL;
    int status = 0;
    int is_first_work_item = 0;
    swait->cur_state = INIT;
    swait-.cur_node_idx = 0;
    swait->iq = iq;
    swait->bdb_state = bdb_state;
    swait->seqnum = seqnum;
    swait->timeoutms = timeoutms;
    swait->txnsize = txnsize;
    swait->newcoh = newcoh;
    swait->do_slow_node_check = 0;
    swait->numfailed = 0;
    swait->um_incoh = 0;
    swait->we_used = 0;
    swait->base_node = NULL;
    swait->track_once = 1;
    swait->num_successfully_acked = 0;
    swait->lock_desired = 0;
    swait->durable_lsns = bdb_state->attr->durable_lsns;
    swait->catchup_window = bdb_state->attr->catchup_window;
    swait->next_ts = comdb2_time_epochms();

    status = pthread_mutex_lock(&seqnum_wait_queue_lk);
    if (status != 0){
        fprintf(stderr,"Error while getting lock at %s:%d\n",__FILE__,__LINE__);
        free(swait);
        return status;
    }
    if(listc_size(&work_queue) == 0){
        is_first_work_item = 1;
    }
    printf("Adding to absolute timestamp queue\n");
    listc_atl(&work_queue->absolute_ts_list, swait);
    printf("Adding to seqnum queue\n");
    LISTC_FOR_EACH(&work_queue->lsn_list,add_before_lsn,lsn_lnk)
    {
        if(log_compare(&swait->lsn, &add_before_lsn->lsn) <= 0){
            listc_add_before(&seqnum_wait_queue->lsn_list,swait,add_before_lsn);
            break;
        }
    }
    
    if(add_before_lsn == NULL){
        // The new LSN is the highest yet... Adding to end of lsn list
        listc_abl(&seqnum_wait_queue->lsn_list, swait);
    }
    work_queue->size += 1;
    pthread_mutex_unlock(&seqnum_wait_queue_lk);
    if(is_first_work_item)
        pthread_cond_signal(&seqnum_wait_queue_new_item_cond);

    return 0;
}

void *queue_processor(void *arg){
    while(1){
        struct seqnum_wait *item = NULL;
        int rc;
        pthread_mutex_lock(&work_queue->mutex);
        while(work_queue->size==0){
            pthread_cond_wait(&work_queue->got_new_item_cond, &work_queue->mutex);
        }
        item = LISTC_TOP(&work_queue->lsn_list);
        pthread_mutex_unlock(&seqnum_wait_queue_lk);
     
        while(item != NULL) {
            switch(item->cur_state){
                case INIT:
                    /* if we were passed a child, find his parent*/
                    if(bdb_state->parent)
                    bdb_state = bdb_state->parent;

                    /* short circuit if we are waiting on lsn 0:0  */
                    if((seqnum->lsn_file == 0) && (seqnum->lsn.offset == 0))
                    {
                        // Do stuff corresponding to rc=0 in bdb_wait_for_seqnum_from_all_int
                    }
                    logmsg(LOGMSG_DEBUG, "%s waiting for %s\n", __func__, lsn_to_str(str, &(seqnum->lsn)));
                    item->begin_time = comdb2_time_epochms();
                    do{
                        item->numnodes = 0;
                        item->numskip = 0;
                        item->numwait = 0;

                        if(item->durable_lsns){
                            item->total_connected = net_get_sanctioned_replicants(item->bdb_state->rep_info->netinfo, REPMAX, item->connlist);
                        } else {
                            item->total_connected = net_get_all_commissioned_nodes(item->bdb_state->rep_info->netinfo, item->connlist); 
                        }
					if (item->total_connected == 0){
						/* nobody is there to wait for! */
						item->cur_state = DONE_WAIT;
                        goto case DONE_WAIT;
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
						if (item->now - item->last_slow_node_check_time > 1000) {
							if (item->bdb_state->attr->track_replication_times) {
								item->last_slow_node_check_time = item->now;
								item->do_slow_node_check = 1
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

					for (i = 0; i < item->total_connected; i++) {
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
						goto case done_wait;
					}

					for (item->cur_node_idx = 0; item->cur_node_idx < item->numnodes; item->cur_node_idx++) {
						if (item->bdb_state->rep_trace)
							logmsg(LOGMSG_USER,
								   "waiting for initial NEWSEQ from node %s of >= <%s>\n",
								   item->nodelist[item->cur_node_idx], lsn_to_str(item->str, &(item->seqnum->lsn)));

                        


                    }while
                    break;
                case GOT_ACK:
                    break;
                case DONE_WAIT:
                    break;
            }

            if(rc!=0){
                logmsg(LOGMSG_FATAL, "%s:%d TRANS_COMMIT failed while waiting for seqnum RC %d", __func__,__LINE__,rc);
            }
            // Taken from toblock.c line 5908
            if(rc !=0 ){
                osql_postcommit_handle(item->iq);
                handle_postcommit_bpfunc(item->iq);
            }
            else{
                osql_postabort_handle(item->iq);
                handle_postabort_bpfunc(item->iq);
            }
            // free current work item and move to next
            pthread_mutex_lock(&seqnum_wait_queue_lk);
            item = LISTC_NEXT(item, lnk);
            listc_rtl(&seqnum_wait_queue);
            pthread_mutex_unlock(&seqnum_wait_queue_lk);
        }
    }
}


void seqnum_wait_cleanup(){
    struct seqnum_wait *item, *tmp;
    listc_t *list_ptr = (listc_t *)&seqnum_wait_queue;

    //FREE THE WORK QUEUE
    pthread_mutex_lock(&seqnum_wait_queue_lk);
    LISTC_FOR_EACH_SAFE(list_ptr, item, tmp, lnk)
        free(listc_rfl(&seqnum_wait_queue, item));

    listc_free((listc_t *)&seqnum_wait_queue);
    pthread_mutex_unlock(&seqnum_wait_queue_lk);

    // FREE THE MEM POOL
    pthread_mutex_lock(&seqnum_wait_queue_pool_lk);
    pool_free(seqnum_wait_queue_pool);
    pthread_mutex_unlock(&seqnum_wait_queue_pool_lk);
}
