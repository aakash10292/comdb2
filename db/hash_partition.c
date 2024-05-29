#include "hash_partition.h"
#include <plhash_glue.h>
#include "views.h"
#include "cson/cson.h"
struct hash_view {
    char *viewname;
    char *tblname;
    int num_keys;
    char **keynames;
    int num_partitions;
    char **partitions;
    int version;
};

pthread_rwlock_t hash_partition_lk;
const char *hash_view_get_viewname(struct hash_view *view)
{
    return view->viewname;
}

const char *hash_view_get_tablename(struct hash_view *view)
{
    return view->tblname;
}
char **hash_view_get_keynames(struct hash_view *view)
{
    return view->keynames;
}
int hash_view_get_num_partitions(struct hash_view *view)
{
    return view->num_partitions;
}
int hash_view_get_num_keys(struct hash_view *view)
{
    return view->num_keys;
}

int hash_view_get_sqlite_view_version(struct hash_view *view)
{
    return view->version;
}

char** hash_view_get_partitions(struct hash_view *view) {
    return view->partitions;
}

static void free_hash_view(hash_view_t *mView)
{
    if (mView) {
        if (mView->viewname) {
            free(mView->viewname);
        }
        if (mView->tblname) {
            free(mView->tblname);
        }
        if (mView->keynames) {
            for (int i = 0; i < mView->num_keys; i++) {
                free(mView->keynames[i]);
            }
            free(mView->keynames);
        }
        if (mView->partitions) {
            for (int i=0; i< mView->num_partitions;i++) {
                free(mView->partitions[i]);
            }
            free(mView->partitions);
        }
        free(mView);
    }
}

hash_view_t *create_hash_view(const char *viewname, const char *tablename, uint32_t num_columns,
                            char columns[][MAXCOLNAME], uint32_t num_partitions,
                            char partitions[][MAXPARTITIONLEN], struct errstat *err)
{
    hash_view_t *mView;

    mView = (hash_view_t *)calloc(1, sizeof(hash_view_t));

    if (!mView) {
        logmsg(LOGMSG_ERROR, "%s: Failed to allocate view %s\n", __func__, viewname);
        goto oom;
    }

    mView->viewname = strdup(viewname);
    if (!mView->viewname) {
        logmsg(LOGMSG_ERROR, "%s: Failed to allocate view name string %s\n", __func__, viewname);
        goto oom;
    }

    mView->tblname = strdup(tablename);
    if (!mView->tblname) {
        logmsg(LOGMSG_ERROR, "%s: Failed to allocate table name string %s\n", __func__, tablename);
        goto oom;
    }
    mView->num_keys = num_columns;
    mView->keynames = (char **)malloc(sizeof(char *) * mView->num_keys);
    if (!mView->keynames) {
        logmsg(LOGMSG_ERROR, "%s: Failed to allocate keynames\n", __func__);
        goto oom;
    }

    for (int i = 0; i < mView->num_keys; i++) {
        mView->keynames[i] = strdup(columns[i]);
        if (!mView->keynames[i]) {
            logmsg(LOGMSG_ERROR, "%s: Failed to allocate key name string %s\n", __func__, columns[i]);
            goto oom;
        }
    }
    mView->num_partitions = num_partitions;

    mView->partitions = (char **)calloc(1, sizeof(char *) * num_partitions);
    for (int i = 0; i < num_partitions; i++) {
        mView->partitions[i] = strdup(partitions[i]);
        if (!mView->partitions[i]) {
            goto oom;
        }
    }

    return mView;
oom:
    free_hash_view(mView);
    errstat_set_rcstrf(err, VIEW_ERR_MALLOC, "calloc oom");
    return NULL;
}

static int create_inmem_view(hash_t *hash_views, hash_view_t *view)
{
    Pthread_rwlock_wrlock(&hash_partition_lk);
    hash_add(hash_views, view);
    Pthread_rwlock_unlock(&hash_partition_lk);
    return VIEW_NOERR;
}

static int destroy_inmem_view(hash_t *hash_views, hash_view_t *view)
{
    if (!view) {
        logmsg(LOGMSG_USER, "SOMETHING IS WRONG. VIEW CAN'T BE NULL\n");
        return VIEW_ERR_NOTFOUND;
    }
    Pthread_rwlock_wrlock(&hash_partition_lk);
    struct hash_view *v = hash_find_readonly(hash_views, &view->viewname);
    int rc = VIEW_NOERR;
    if (!v) {
        rc = VIEW_ERR_NOTFOUND;
        goto done;
    }
    hash_del(hash_views, v);
    free_hash_view(v);
done:
    Pthread_rwlock_unlock(&hash_partition_lk);
    return rc;
}

static int find_inmem_view(hash_t *hash_views, const char *name, hash_view_t **oView)
{
    int rc = VIEW_NOERR;
    Pthread_rwlock_wrlock(&hash_partition_lk);
    *oView = hash_find_readonly(hash_views, &name);
    if (!(*oView)) {
        rc = VIEW_ERR_NOTFOUND;
        goto done;
    }
done:
    Pthread_rwlock_unlock(&hash_partition_lk);
    return rc;
}

unsigned long long hash_partition_get_partition_version(const char *name)
{
    struct dbtable *db = get_dbtable_by_name(name);
    if (!db) {
        logmsg(LOGMSG_USER, "Could not find partition %s\n", name);
        return VIEW_ERR_NOTFOUND;
    }
    logmsg(LOGMSG_USER, "RETURNING PARTITION VERSION %lld\n", db->tableversion);
    return db->tableversion;
}

int is_hash_partition(const char *name)
{
    struct hash_view *v = NULL;
    hash_get_inmem_view(name, &v);
    return v != NULL;
}

unsigned long long hash_view_get_version(const char *name)
{
    struct hash_view *v = NULL;
    if (find_inmem_view(thedb->hash_partition_views, name, &v)) {
        logmsg(LOGMSG_USER, "Could not find partition %s\n", name);
        return VIEW_ERR_NOTFOUND;
    }

    char **partitions = hash_view_get_partitions(v);
    return hash_partition_get_partition_version(partitions[0]);
}

int hash_get_inmem_view(const char *name, hash_view_t **oView)
{
    int rc;
    if (!name) {
        logmsg(LOGMSG_ERROR, "%s: Trying to retrieve nameless view!\n", __func__);
        return VIEW_ERR_NOTFOUND;
    }
    rc = find_inmem_view(thedb->hash_partition_views, name, oView);
    if (rc != VIEW_NOERR) {
        logmsg(LOGMSG_ERROR, "%s: failed to find in-memory view %s. rc: %d\n", __func__, name, rc);
    }
    return rc;
}

int hash_create_inmem_view(hash_view_t *view)
{
    int rc;
    if (!view) {
        return VIEW_ERR_NOTFOUND;
    }
    rc = create_inmem_view(thedb->hash_partition_views, view);
    if (rc != VIEW_NOERR) {
        logmsg(LOGMSG_ERROR, "%s: failed to create in-memory view %s. rc: %d\n", __func__, view->viewname, rc);
    }
    return rc;
}

int hash_destroy_inmem_view(hash_view_t *view)
{
    int rc;
    if (!view) {
        return VIEW_ERR_NOTFOUND;
    }
    rc = destroy_inmem_view(thedb->hash_partition_views, view);
    if (rc != VIEW_NOERR) {
        logmsg(LOGMSG_ERROR, "%s: failed to destroy in-memory view %s. rc: %d\n", __func__, view->viewname, rc);
    }
    return rc;
}
int hash_partition_llmeta_erase(void *tran, hash_view_t *view, struct errstat *err)
{
    int rc = 0;
    const char *view_name = hash_view_get_viewname(view);
    logmsg(LOGMSG_USER, "Erasing view %s\n", view_name);
    rc = hash_views_write_view(tran, view_name, NULL, 0);
    if (rc != VIEW_NOERR) {
        logmsg(LOGMSG_USER, "Failed to erase llmeta entry for partition %s. rc: %d\n", view_name, rc);
    }
    ++gbl_views_gen;
    view->version = gbl_views_gen;
    return rc;
}
int hash_partition_llmeta_write(void *tran, hash_view_t *view, struct errstat *err)
{
    /*
     * Get serialized view string
     * write to llmeta
     */
    char *view_str = NULL;
    int view_str_len;
    int rc;

    rc = hash_serialize_view(view, &view_str_len, &view_str);
    if (rc != VIEW_NOERR) {
        errstat_set_strf(err, "Failed to serialize view %s", view->viewname);
        errstat_set_rc(err, rc = VIEW_ERR_BUG);
        goto done;
    }

    logmsg(LOGMSG_USER, "%s\n", view_str);
    /* save the view */
    rc = hash_views_write_view(tran, view->viewname, view_str, 0);
    if (rc != VIEW_NOERR) {
        if (rc == VIEW_ERR_EXIST)
            errstat_set_rcstrf(err, VIEW_ERR_EXIST, "View %s already exists", view->viewname);
        else
            errstat_set_rcstrf(err, VIEW_ERR_LLMETA, "Failed to llmeta save view %s", view->viewname);
        goto done;
    }

    ++gbl_views_gen;
    view->version = gbl_views_gen;

done:
    if (view_str)
        free(view_str);
    return rc;
}

char *hash_views_read_all_views();
/*
 * Create hash map of all hash based partitions
 *
 */
hash_t *hash_create_all_views()
{
    hash_t *hash_views;
    char *views_str;
    int rc;
    struct hash_view *view = NULL;
    cson_value *rootVal = NULL;
    cson_object *rootObj = NULL;
    cson_object_iterator iter;
    cson_kvp *kvp = NULL;
    struct errstat err;
    hash_views = hash_init_strcaseptr(offsetof(struct hash_view, viewname));
    views_str = hash_views_read_all_views();

    if (!views_str) {
        logmsg(LOGMSG_ERROR, "Failed to read hash views from llmeta\n");
        goto done;
    }

    rc = cson_parse_string(&rootVal, views_str, strlen(views_str));
    if (rc) {
        logmsg(LOGMSG_ERROR, "error parsing cson string. rc: %d err: %s\n", rc, cson_rc_string(rc));
        goto done;
    }
    if (!cson_value_is_object(rootVal)) {
        logmsg(LOGMSG_ERROR, "error parsing cson: expected object type\n");
        goto done;
    }

    rootObj = cson_value_get_object(rootVal);

    if (!rootObj) {
        logmsg(LOGMSG_ERROR, "error parsing cson: couldn't retrieve object\n");
        goto done;
    }

    cson_object_iter_init(rootObj, &iter);
    kvp = cson_object_iter_next(&iter);
    while (kvp) {
        cson_value *val = cson_kvp_value(kvp);
        /*
         * Each cson_value above is a cson string representation
         * of a hash based partition. Validate that it's a string value
         * and deserialize into an in-mem representation.
         */
        if (!cson_value_is_string(val)) {
            logmsg(LOGMSG_ERROR, "error parsing cson: expected string type\n");
            goto done;
        }
        const char *view_str = cson_string_cstr(cson_value_get_string(val));
        view = hash_deserialize_view(view_str, &err);

        if (!view) {
            logmsg(LOGMSG_ERROR, "%s: failed to deserialize hash view %d %s\n", __func__, err.errval, err.errstr);
            goto done;
        }
        /* we've successfully deserialized a view.
         * now add it to the global hash of all hash-parititon based views */
        hash_add(hash_views, view);
        kvp = cson_object_iter_next(&iter);
    }
done:
    if (rootVal) {
        cson_value_free(rootVal);
    }

    if (views_str) {
        free(views_str);
    }
    return hash_views;
}

static int hash_update_partition_version(hash_view_t *view, void *tran)
{
    int num_partitions = hash_view_get_num_partitions(view);
    char **partitions = hash_view_get_partitions(view);
    for (int i=0;i<num_partitions;i++) {
        struct dbtable *db = get_dbtable_by_name(partitions[i]);
        if (db) {
            db->tableversion = table_version_select(db, tran);
        } else {
            logmsg(LOGMSG_USER, "UNABLE TO LOCATE partition %s\n", partitions[i]);
            return -1;
        }
    }
    return 0;
}

int hash_views_update_replicant(void *tran, const char *name)
{
    hash_t *hash_views = thedb->hash_partition_views;
    hash_view_t *view = NULL, *v = NULL;
    int rc = VIEW_NOERR;
    logmsg(LOGMSG_USER, "++++++ Replicant updating views\n");
    char *view_str = NULL;
    struct errstat xerr = {0};

    /* read the view str from updated llmeta */
    rc = hash_views_read_view(tran, name, &view_str);
    if (rc == VIEW_ERR_EXIST) {
        view = NULL;
        goto update_view_hash;
    } else if (rc != VIEW_NOERR || !view_str) {
        logmsg(LOGMSG_ERROR, "%s: Could not read metadata for view %s\n", __func__, name);
        goto done;
    }

    logmsg(LOGMSG_USER, "The views string is %s\n", view_str);
    /* create an in-mem view object */
    view = hash_deserialize_view(view_str, &xerr);
    if (!view) {
        logmsg(LOGMSG_ERROR, "%s: failed to deserialize hash view %d %s\n", __func__, xerr.errval, xerr.errstr);
        goto done;
    }
    hash_update_partition_version(view, tran);
update_view_hash:
    /* update global hash views hash.
     * - If a view with the given name exists, destroy it, create a new view and add to hash
     * - If a view with the given name does not exist, add to hash
     * - If view is NULL (not there in llmeta), destroy the view and remove from hash */

    if (!view) {
        logmsg(LOGMSG_USER, "The deserialized view is NULL. This is a delete case \n");
        /* It's okay to do this lockless. The subsequent destroy method
         * grabs a lock and does a find again */
        v = hash_find_readonly(hash_views, &name);
        if (!v) {
            logmsg(LOGMSG_ERROR, "Couldn't find view in llmeta or in-mem hash\n");
            goto done;
        }
        rc = hash_destroy_inmem_view(v);
        if (rc != VIEW_NOERR) {
            logmsg(LOGMSG_ERROR, "%s:%d Failed to destroy inmem view\n", __func__, __LINE__);
        }
    } else {
        logmsg(LOGMSG_USER, "The deserialized view is NOT NULL. this is a view create/update case \n");
        rc = hash_destroy_inmem_view(view);
        if (rc != VIEW_NOERR && rc != VIEW_ERR_NOTFOUND) {
            logmsg(LOGMSG_ERROR, "%s:%d Failed to destroy inmem view\n", __func__, __LINE__);
            goto done;
        }
        rc = hash_create_inmem_view(view);
        if (rc != VIEW_NOERR) {
            logmsg(LOGMSG_ERROR, "%s:%d Failed to create inmem view\n", __func__, __LINE__);
            goto done;
        }
    }

    ++gbl_views_gen;
    if (view) {
        view->version = gbl_views_gen;
    }
    return rc;
done:
    if (view_str) {
        free(view_str);
    }
    if (view) {
        free_hash_view(view);
    }
    return rc;
}
