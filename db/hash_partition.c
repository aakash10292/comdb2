#include "hash_partition.h"
#include <plhash_glue.h>
#include "views.h"
#include "cson/cson.h"
#include "schemachange.h"
#include <regex.h>
#include "consistent_hash.h"
struct hash_view {
    char *viewname;
    char *tblname;
    int num_keys;
    char **keynames;
    int num_partitions;
    char **partitions;
    int version;
    ch_hash_t *partition_hash;
};
extern char gbl_dbname[MAX_DBNAME_LENGTH];
pthread_rwlock_t hash_partition_lk;
int gbl_sharding_local = 1;
void dump_alias_info();
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
        if (mView->partition_hash) {
            ch_hash_free(mView->partition_hash);
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

    ch_hash_t *ch = ch_hash_create(mView->num_partitions, ch_hash_sha);
    mView->partition_hash = ch;
    if (!mView->partition_hash) {
        logmsg(LOGMSG_ERROR, "Failed create consistent_hash\n");
        goto oom;
    }

    for(int i=0;i<mView->num_partitions;i++){
        if (ch_hash_add_node(ch, (uint8_t *)mView->partitions[i], strlen(mView->partitions[i]), ch->key_hashes[i]->hash_val)) {
        logmsg(LOGMSG_ERROR, "Failed to add node %s\n", mView->partitions[i]);
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
    hash_view_t *view = NULL;
    Pthread_rwlock_wrlock(&hash_partition_lk);
    view = hash_find_readonly(hash_views, &name);
    if (!view) {
        rc = VIEW_ERR_NOTFOUND;
        goto done;
    }
    if (oView) {
        *oView = view;
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
static int getDbHndl(cdb2_hndl_tp **hndl, const char *dbname, const char *tier) {
    int rc;
    
    if (!tier) {
        rc = cdb2_open(hndl, dbname, "localhost", CDB2_DIRECT_CPU);
    } else {
        rc = cdb2_open(hndl, dbname, tier, 0);
    }
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s:%d Failed to connect to %s@%s (rc: %d)\n",
                       __func__, __LINE__, gbl_dbname, "local", rc);
        cdb2_close(*hndl);
    }
    return rc;
}

/* extract everything after "create table <tblname>"
 * and before "partitioned by ("*/
char *extractSchema(const char *insertQuery) {
    const char *start = strchr(insertQuery, '('); // Find the first '('
    const char *end = strchr(insertQuery, ')');   // Find the first ')'
    char *result = NULL;
    if (start != NULL && end != NULL && end > start) {
        size_t length = end - start - 1; // Calculate the length of the substring
        result = (char *)malloc(length + 1);         // Allocate memory for the result

        strncpy(result, start + 1, length); // Copy the substring between parentheses
        result[length] = '\0';              // Null-terminate the result

        logmsg(LOGMSG_USER, "Extracted string: %s\n", result);
    } else {
        logmsg(LOGMSG_ERROR, "No match found\n");
    }
    return result;
}

/* Create an insert query against shards 
 * create table tableName(...)
 * */
static char *getCreateStatement(const char *insertQuery, const char *tableName) {
    const char *schema = extractSchema(insertQuery);
    size_t createStatementLen = 13 + strlen(tableName) + strlen(schema) + 2 + 1; /* 13 -> "CREATE TABLE ", 2-> (, ) */
    char *createStatement = (char *)malloc(createStatementLen);
    strcpy(createStatement, "CREATE TABLE ");
    strcat(createStatement, tableName);
    strcat(createStatement, "(");
    strcat(createStatement, schema);
    strcat(createStatement, ")");
    logmsg(LOGMSG_USER, "The create statment is %s\n", createStatement);
    return createStatement;
}

static char *getDropStatement(const char *tableName) {
    size_t dropStatementLen = 21 + strlen(tableName) + 1;  /* 11-> "DROP TABLE IF EXISTS "*/
    char *dropStatement = (char *)malloc(dropStatementLen);
    strcpy(dropStatement, "DROP TABLE IF EXISTS ");
    strcat(dropStatement, tableName);
    logmsg(LOGMSG_USER, "The drop statemetn is %s\n", dropStatement);
    return dropStatement;
}

/*
 */
void deleteRemoteTables(struct comdb2_partition *partition, int startIdx) {
    cdb2_hndl_tp *hndl;
    int rc;
    char *savePtr = NULL, *remoteDbName = NULL, *remoteTableName = NULL;
    char *tier = NULL; 
    int i;
    for(i = startIdx; i >= 0; i--) {
        char *p = partition->u.hash.partitions[i];
        remoteDbName = strtok_r(p,".", &savePtr);
        remoteTableName = strtok_r(NULL, ".", &savePtr);
        if (remoteTableName == NULL) {
            remoteTableName = remoteDbName;
            remoteDbName = gbl_dbname;
        }
        logmsg(LOGMSG_USER, "The db is %s, the table is %s\n", remoteDbName, remoteTableName);

        if (!strcmp(gbl_dbname, remoteDbName)) {
            rc = getDbHndl(&hndl, gbl_dbname, NULL);
        } else {
            if (gbl_sharding_local) {
                tier = "local";
            } else {
                tier = (char *)mach_class_class2name(get_my_mach_class());
            }
            if (!tier) {
                logmsg(LOGMSG_ERROR, "Failed to get tier for remotedb %s\n", p);
                abort();
            }
            logmsg(LOGMSG_USER, "GOT THE TIER AS %s\n", tier);
            rc = getDbHndl(&hndl, remoteDbName, tier);
        }
        if (rc) {
            logmsg(LOGMSG_ERROR, "Failed to get handle. rc: %d, err: %s\n", rc, cdb2_errstr(hndl));
            logmsg(LOGMSG_ERROR, "Failed to drop table %s on remote db %s\n", remoteDbName, remoteTableName);
            goto cleanup;
        }
        char *dropStatement = getDropStatement(remoteTableName);
        if (!dropStatement) {
            logmsg(LOGMSG_ERROR, "Failed to generate drop Query\n");
            logmsg(LOGMSG_ERROR, "Failed to drop table %s on remote db %s\n", remoteDbName, remoteTableName);
            goto close_handle;
        } else {
            logmsg(LOGMSG_USER, "The generated drop statement is %s\n", dropStatement);
        }

        rc = cdb2_run_statement(hndl, dropStatement);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Failed to drop table %s on database %s. rc: %d, err: %s\n", remoteTableName, remoteDbName, rc, cdb2_errstr(hndl));
        }
close_handle:
        cdb2_close(hndl);
cleanup:
        free(remoteDbName);
        free(remoteTableName);
    }
}

int createRemoteTables(struct comdb2_partition *partition) {
    cdb2_hndl_tp *hndl;
    int rc;
    int num_partitions = partition->u.hash.num_partitions;
    int i;
    char *tier = NULL; 
    char *savePtr = NULL, *remoteDbName = NULL, *remoteTableName = NULL, *p = NULL;
    for (i = 0; i < num_partitions; i++) {
        p = strdup(partition->u.hash.partitions[i]);
        remoteDbName = strtok_r(p,".", &savePtr);
        remoteTableName = strtok_r(NULL, ".", &savePtr);
        if (remoteTableName == NULL) {
            remoteTableName = remoteDbName;
            remoteDbName = gbl_dbname;
        }

        logmsg(LOGMSG_USER, "The db is %s, the table is %s\n", remoteDbName, remoteTableName);

        if (!strcmp(gbl_dbname, remoteDbName)) {
            rc = getDbHndl(&hndl, gbl_dbname, NULL);
        } else {
            if (gbl_sharding_local) {
                tier = "local";
            } else {
                tier = (char *)mach_class_class2name(get_my_mach_class());
            }
            if (!tier) {
                logmsg(LOGMSG_ERROR, "Failed to get tier for remotedb %s\n", p);
                abort();
            }
            logmsg(LOGMSG_USER, "GOT THE TIER AS %s\n", tier);
            rc = getDbHndl(&hndl, remoteDbName, tier);
            /*const char *tier = mach_class_class2name(get_my_mach_class());
            if (!tier) {
                logmsg(LOGMSG_ERROR, "Failed to get tier for remotedb %s\n", p);
                abort();
            }
            logmsg(LOGMSG_USER, "GOT THE TIER AS %s\n", tier);*/
        }
        if (rc) {
            goto cleanup_tables;
        }

        char *createStatement = getCreateStatement(partition->u.hash.createQuery, remoteTableName);
        if (!createStatement) {
            logmsg(LOGMSG_ERROR, "Failed to generate createQuery\n");
        } else {
            logmsg(LOGMSG_USER, "The generated create statement is %s\n", createStatement);
        }

        rc = cdb2_run_statement(hndl, createStatement);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Failed to create table %s on database %s. rc: %d, err: %s\n", remoteTableName, remoteDbName, rc, cdb2_errstr(hndl));
            goto cleanup_tables;
        }
        cdb2_close(hndl);
        free(p);
    }
    return 0;
cleanup_tables:
    /* close most recent handle*/
    cdb2_close(hndl);
    free(p);
    deleteRemoteTables(partition, i);
    return -1;
}


int createLocalAliases(struct comdb2_partition *partition) {
    cdb2_hndl_tp *hndl;
    int rc;
    int num_partitions = partition->u.hash.num_partitions;
    int i;
    char *savePtr = NULL, *remoteDbName = NULL, *remoteTableName = NULL, *p = NULL;
    /* get handle to local db*/
    rc = getDbHndl(&hndl, gbl_dbname, NULL);
    if (rc) {
        goto cleanup_aliases;
    }
    for (i = 0; i < num_partitions; i++) {
        p = strdup(partition->u.hash.partitions[i]);
        remoteDbName = strtok_r(p,".", &savePtr);
        remoteTableName = strtok_r(NULL, ".", &savePtr);
        if (remoteTableName == NULL) {
            remoteTableName = remoteDbName;
            remoteDbName = gbl_dbname;
        }

        logmsg(LOGMSG_USER, "The db is %s, the table is %s\n", remoteDbName, remoteTableName);

        char localAlias[MAXPARTITIONLEN+1]; /* remoteDbName_remoteTableName */
        char createAliasStatement[MAXPARTITIONLEN*2+3+10+1]; /* 3->spaces 8->'PUT ALIAS' */
        rc = snprintf(localAlias, sizeof(localAlias), "%s_%s", remoteDbName, remoteTableName);
        if (!rc) {
            logmsg(LOGMSG_ERROR, "Failed to generate local alias name. rc: %d %s\n", rc, localAlias);
            goto cleanup_aliases;
        } else {
            logmsg(LOGMSG_ERROR, "The generated alias is %s\n", localAlias);
        }
        rc = snprintf(createAliasStatement,sizeof(createAliasStatement), "PUT ALIAS %s '%s.%s'", localAlias, remoteDbName, remoteTableName);
        if (!rc) {
            logmsg(LOGMSG_ERROR, "Failed to generate create alias query\n");
            goto cleanup_aliases;
        } else {
            logmsg(LOGMSG_USER, "The generated create statement for Alias is %s\n", createAliasStatement);
        }

        rc = cdb2_run_statement(hndl, createAliasStatement);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Failed to create alias %s on database %s. rc: %d, err: %s\n", remoteTableName, remoteDbName, rc, cdb2_errstr(hndl));
            goto cleanup_aliases;
        }
        free(p);
    }
    dump_alias_info();
    cdb2_close(hndl);
    return 0;
cleanup_aliases:
    /* close most recent handle*/
    cdb2_close(hndl);
    free(p);
    /* TODO : IMPLEMENT BELOW */
    //deleteLocalAliases(partition, i);
    return -1;
}
