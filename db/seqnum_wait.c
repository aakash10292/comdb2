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

int timestamp_compare(int ts1, int ts2){
    if(ts1 < ts2) return -1;
    else if(ts1 > ts2) return 1;
    else return 0;
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
    struct seqnum_wait *item = NULL;
    struct timespec waittime;
    int wait_rc = 0;
    listc_t *cur_list = NULL;
    linkc_t *cur_link = NULL;
    while(1){
        if(wait_rc == 0){
            // we're going to iterate over lsn_list
            cur_list = &work_queue->lsn_list;
            pthread_mutex_lock(&work_queue->mutex);
            while(work_queue->lsn_list->size==0){
                pthread_cond_wait(&work_queue->got_new_item_cond, &work_queue->mutex);
            }
            LISTC_FOR_EACH(&work_queue->lsn_list,item, lsn_lnk)
        }
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

                case FIRST_ACK:
                    if(comdb2_time_epochms() - item->begin_time < item->bdb_state->attr->rep_timeout_maxms &&
                            !(lock_desired = bdb_lock_desired(item->bdb_state))){
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
                        struct seqnum_wait *add_before_ts = NULL;
                        int new_ts = 0;
                        item->cur_state = FIRST_ACK;
                        for (item->cur_node_idx = 0; item->cur_node_idx < item->numnodes; item->cur_node_idx++) {
                            if (item->bdb_state->rep_trace)
                                logmsg(LOGMSG_USER,
                                       "waiting for initial NEWSEQ from node %s of >= <%s>\n",
                                       item->nodelist[item->cur_node_idx], lsn_to_str(item->str, &(item->seqnum->lsn)));    
                            rc = bdb_wait_for_seqnum_from_node_nowait_int(item->bdb_state, &(item->bdb_state->seqnum_info->seqnums[nodeix(item->bdb_state->repinfo->master_host)]), item->nodelist[item->cur_node_idx]);
                            if(rc == 0){
                                item->got_ack_from_atleast_one_node = 1;
                                item->base_node = item->nodelist[item->cur_node_idx];
                                item->num_successfully_acked++;
                                item->end_time = comdb2_time_epochms();
                                item->we_used = item->end_time - item->start_time;
                                item->waitms = item->we_used * item->bdb_state->attr->rep_timeout_lag) / 100;
                                if (item->waitms < item->bdb_state->attr->rep_timeout_minms)
                                    waitms = item->bdb_state->attr->rep_timeout_minms;
                                if (item->bdb_state->rep_trace)
                                    logmsg(LOGMSG_USER, "fastest node to <%s> was %dms, will wait "
                                                    "another %dms for remainder\n",
                                            lsn_to_str(item->str, &(item->seqnum->lsn)), item->we_used, item->waitms);

                                goto case GOT_FIRST_ACK;
                            }
                        }
                        // If we get here, then none of the replicants have caught up yet, 
                        // Let's wait for one second and check again.
                        item->previous_ts = comdb2_time_epochms();
                        new_ts = comdb2_time_epochms() + 1000;


                        // Change position of current work item in absolute_ts_list based on new_ts (the next absolute timestamp that this node has to be worked on again
                        LISTC_FOR_EACH(&work_queue->absolute_ts_list,add_before_ts,absolute_ts_lnk)
                        {
                            if(item!=add_before_ts && add_before_ts!=NULL && ts_compare(new_ts, add_before_ts->next_ts) >= 0){
                                // Make sure next and previous nodes of item are made to point to the correct nodes in the list
                                if(item->absolute_ts_lnk.prev!=NULL){
                                    item->absolute_ts_lnk.prev.next = item->absolute_ts_lnk.next;
                                }
                                else if(item->absolute_ts_lnk.next!=NULL) {
                                    item->absolute_ts_lnk.next.prev = item->absolute_ts_lnk.prev;
                                }
                                if(new_ts == add_before_ts->next_ts){
                                    // add item after add_before ( we want to follow FCFS for same next_ts)
                                    listc_add_after(&work_queue->absolute_ts_list,item,add_before_lsn);

                                    
                                    /*item->absolute_ts_lnk.next = add_before_ts->absolute_ts_lnk.next;
                                    item->absolute_ts_lnk.prev = add_before_ts->absolute_ts_lnk;
                                    if(add_before_ts->absolute_ts_lnk.next!=NULL){
                                        add_before_ts->absolute_ts_lnk.next.prev = item->absolute_ts_lnk;
                                        add_before_ts->absolute_ts_lnk.next = item->absolute_ts_lnk;
                                    }*/
                                }
                                else{
                                    // add item before add_before
                                    listc_add_after(&work_queue->absolute_ts_list,item,add_before_lsn);
                                }
                                break;
                            }
                        }

                        if(add_before_ts == NULL){
                            // Make sure next and previous nodes of item are made to point to the correct nodes in the list
                            if(item->absolute_ts_lnk.prev!=NULL){
                                item->absolute_ts_lnk.prev.next = item->absolute_ts_lnk.next;
                            }
                            else if(item->absolute_ts_lnk.next!=NULL) {
                                item->absolute_ts_lnk.next.prev = item->absolute_ts_lnk.prev;
                            }
                            // updated next timestamp for this item is the highest yet... Adding to end of absolute_ts_list
                            listc_abl(&work_queue->absolute_ts_list, item);
                        }
                        item->next_ts = new_ts;
                      }
                   else{
                       // we timed out i.e exceeded bdb->attr->rep_timeout_maxms
                       item->end_time = comdb2_time_epochms(); 
                       item->we_used = item->end_time - item->begin_time;
                       item->waitms = 0; // We've already exceeded max timeout. 
                        if(!lock_desired)
                            logmsg(LOGMSG_WARN, "timed out waiting for initial replication of <%s>\n",
                                   lsn_to_str(item->str, &(item->seqnum->lsn)));
                        else
                            logmsg(LOGMSG_WARN,
                                   "lock desired, not waiting for initial replication of <%s>\n",
                                   lsn_to_str(item->str, &(item->seqnum->lsn)));
                   } 
                case GOT_FIRST_ACK:
                    item->cur_state = GOT_FIRST_ACK;
                    item->begin_time = comdb2_time_epochms(); // We reset time to now (It's a new state, it's a new time)
                    if(item->got_ack_from_atleast_one_node && item->waitms < item->bdb_state->attr->rep_timeout_minms){
                        // If the first node responded really fast, we don't want to impose too harsh a timeout on the remaining nodes
                        item->waitms = item->bdb_state->attr->rep_timeout_minms;
                    }
                    *(item->timeoutms) = item->we_used + item->waitms;
                    for(int i=0;i<item->numnodes;i++){
                        if(item->nodelist[i] = item->base_node)
                            continue;      
                        if (item->bdb_state->rep_trace)
                            logmsg(LOGMSG_USER,
                                   "waiting for NEWSEQ from node %s of >= <%s> timeout %d\n",
                                   item->nodelist[i], lsn_to_str(item->str, &(item->seqnum->lsn)), item->waitms);
                        rc = bdb_wait_for_seqnum_from_node_nowait_int(item->bdb_state, &(item->bdb_state->seqnum_info->seqnums[nodeix(item->bdb_state->repinfo->master_host)]), item->nodelist[i]);
                        if (bdb_lock_desired(item->bdb_state)) {
                            logmsg(LOGMSG_ERROR,
                                   "%s line %d early exit because lock-is-desired\n", __func__,
                                   __LINE__);

                            //Dont return , handle it the correct way 
                            //return (item->durable_lsns ? BDBERR_NOT_DURABLE : -1);
                        }
                        if (rc == -999){
                            logmsg(LOGMSG_WARN, "node %s hasn't caught up yet, base node "
                                            "was %s",
                                    item->nodelist[i],item->base_node);
                            item->numfailed++;
                            // If even one replicant hasn't caught up, we come out of the loop and wait
                            // No point in checking the others as we will have to wait anyways
                            break;
                        }
                    }
                    if(item->numfailed == 0){
                        // Awesome! Everyone's caught up. 
                        goto case DONE_WAIT;
                    }
                    else{
                        //If we are still within waitms timeout, we still have hope! 
                        //Modify position of item appropriately in absolute_ts_list
                        if(comdb2_time_epochms() - item->begin_time() < item->waitms){
                            item->previous_ts = comdb2_time_epochms();
                            new_ts = comdb2_time_epochms() + waitms;


                            // Change position of current work item in absolute_ts_list based on new_ts (the next absolute timestamp that this node has to be worked on again
                            LISTC_FOR_EACH(&work_queue->absolute_ts_list,add_before_ts,absolute_ts_lnk)
                            {
                                if(item!=add_before_ts && add_before_ts!=NULL && ts_compare(new_ts, add_before_ts->next_ts) >= 0){
                                    // Make sure next and previous nodes of item are made to point to the correct nodes in the list
                                    if(item->absolute_ts_lnk.prev!=NULL){
                                        item->absolute_ts_lnk.prev.next = item->absolute_ts_lnk.next;
                                    }
                                    else if(item->absolute_ts_lnk.next!=NULL) {
                                        item->absolute_ts_lnk.next.prev = item->absolute_ts_lnk.prev;
                                    }
                                    if(new_ts == add_before_ts->next_ts){
                                        // add item after add_before ( we want to follow FCFS for same next_ts)
                                        listc_add_after(&work_queue->absolute_ts_list,item,add_before_lsn);

                                        
                                        /*item->absolute_ts_lnk.next = add_before_ts->absolute_ts_lnk.next;
                                        item->absolute_ts_lnk.prev = add_before_ts->absolute_ts_lnk;
                                        if(add_before_ts->absolute_ts_lnk.next!=NULL){
                                            add_before_ts->absolute_ts_lnk.next.prev = item->absolute_ts_lnk;
                                            add_before_ts->absolute_ts_lnk.next = item->absolute_ts_lnk;
                                        }*/
                                    }
                                    else{
                                        // add item before add_before
                                        listc_add_after(&work_queue->absolute_ts_list,item,add_before_lsn);
                                    }
                                    break;
                                }
                            }

                            if(add_before_ts == NULL){
                                // Make sure next and previous nodes of item are made to point to the correct nodes in the list
                                if(item->absolute_ts_lnk.prev!=NULL){
                                    item->absolute_ts_lnk.prev.next = item->absolute_ts_lnk.next;
                                }
                                else if(item->absolute_ts_lnk.next!=NULL) {
                                    item->absolute_ts_lnk.next.prev = item->absolute_ts_lnk.prev;
                                }
                                // updated next timestamp for this item is the highest yet... Adding to end of absolute_ts_list
                                listc_abl(&work_queue->absolute_ts_list, item);
                            }
                            item->next_ts = new_ts;
                            
                        } 
                    }
                case DONE_WAIT:
                    item->cur_state = DONE_WAIT;
                    outrc = 0;

                    if (!numfailed && !numskip && !numwait &&
                        bdb_state->attr->remove_commitdelay_on_coherent_cluster &&
                        bdb_state->attr->commitdelay) {
                        logmsg(LOGMSG_INFO, "Cluster is in sync, removing commitdelay\n");
                        bdb_state->attr->commitdelay = 0;
                    }

                    if (numfailed) {
                        outrc = -1;
                    }

                    if (durable_lsns) {
                        uint32_t cur_gen;
                        static uint32_t not_durable_count;
                        static uint32_t durable_count;
                        extern int gbl_durable_wait_seqnum_test;

                        int istest = 0;
                        int was_durable = 0;

                        uint32_t cluster_size = total_connected + 1;
                        uint32_t number_with_this_update = num_successfully_acked + 1;
                        uint32_t durable_target = (cluster_size / 2) + 1;

                        if ((number_with_this_update < durable_target) ||
                            (gbl_durable_wait_seqnum_test && (istest = (0 == (rand() % 20))))) {
                            if (istest)
                                logmsg(LOGMSG_USER, 
                                        "%s return not durable for durable wait seqnum test\n", __func__);
                            outrc = BDBERR_NOT_DURABLE;
                            not_durable_count++;
                            was_durable = 0;
                        } else {
                            /* We've released the bdb lock at this point- the master could have
                             * changed while
                             * we were waiting for this to propogate.  The simple fix: get
                             * rep_gen & return
                             * not durable if it's changed */
                            BDB_READLOCK("wait_for_seqnum");
                            bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &cur_gen);
                            BDB_RELLOCK();

                            if (cur_gen != seqnum->generation) {
                                outrc = BDBERR_NOT_DURABLE;
                                not_durable_count++;
                                was_durable = 0;
                            } else {
                                Pthread_mutex_lock(&bdb_state->durable_lsn_lk);
                                bdb_state->dbenv->set_durable_lsn(bdb_state->dbenv,
                                                                  &seqnum->lsn, cur_gen);
                                if (seqnum->lsn.file == 0) {
                                    logmsg(LOGMSG_FATAL, "%s line %d: aborting on insane durable lsn\n",
                                            __func__, __LINE__);
                                    abort();
                                }
                                Pthread_mutex_unlock(&bdb_state->durable_lsn_lk);
                                durable_count++;
                                was_durable = 1;
                            }
                        }

                        if (bdb_state->attr->wait_for_seqnum_trace) {
                            DB_LSN calc_lsn;
                            uint32_t calc_gen;
                            calculate_durable_lsn(bdb_state, &calc_lsn, &calc_gen, 1);
                            /* This is actually okay- do_ack and the thread which broadcasts
                             * seqnums can race against each other.  If we got a majority of 
                             * these during the commit we are okay */
                            if (was_durable && log_compare(&calc_lsn, &seqnum->lsn) < 0) {
                                logmsg(LOGMSG_USER,
                                       "ERROR: calculate_durable_lsn trails seqnum, "
                                       "but this is durable (%d:%d vs %d:%d)?\n",
                                       calc_lsn.file, calc_lsn.offset, seqnum->lsn.file,
                                       seqnum->lsn.offset);
                            }
                            logmsg(LOGMSG_USER, 
                                "Last txn was %s, tot_connected=%d tot_acked=%d, "
                                "durable-commit-count=%u not-durable-commit-count=%u "
                                "commit-lsn=[%d][%d] commit-gen=%u calc-durable-lsn=[%d][%d] "
                                "calc-durable-gen=%u\n",
                                was_durable ? "durable" : "not-durable", total_connected,
                                num_successfully_acked, durable_count, not_durable_count,
                                seqnum->lsn.file, seqnum->lsn.offset, seqnum->generation,
                                calc_lsn.file, calc_lsn.offset, calc_gen);
                        }
                    }
            }
        }
        // Now we traverse the absolute_ts_list.
        item = LISTC_TOP(&work_queue->lsn_list);
        if(item!=null){
            now = comdb2_time_epochms();
            if(now <  item->next_ts){
                // we are early, wait till the earliest time that a node has to be checked
                setup_waittime(&waittime, now-item->next_ts);
                rc = pthread_cond_timedwait(&(item->bdb_state->seqnum_info->cond),
                                            &(item->bdb_state->seqnum_info->lock), &waittime); 
            }
            else{
                // We are at(or have crossed) the smallest next_ts in absolute_ts_list
                wait_rc = ETIMEDOUT;
            }
        }else{
            //Nothing on the absolute_ts_list.. Go back and process lsn_list
            wait_rc = 0;
        }
            // free current work item and move to next
            pthread_mutex_lock(&seqnum_wait_queue_lk);
            item = LISTC_NEXT(item, lnk);
            listc_rtl(&seqnum_wait_queue);
            pthread_mutex_unlock(&seqnum_wait_queue_lk);
        }
    }

void bdb_wait_for_seqnum_from_node_async(bdb_state_type *bdb_state, seqnum_type *seqnum, const char *host, int timeoutms, int lineno, int *out_rc){
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
