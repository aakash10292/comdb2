#include <string>
#include <vector>
#include <limits.h>
#include <string.h>
#include <arpa/inet.h>
#include <cdb2api.h>
#include <time.h>
#include <sstream>
#include <iostream>

typedef struct {
    unsigned int count;
    unsigned int thrid;
}thr_info_t;


int runsql(cdb2_hndl_tp *h, std::string &sql)
{
    int rc = cdb2_run_statement(h, sql.c_str());
    if (rc != 0) {
        fprintf(stderr, "Error: cdb2_run_statement failed: %d %s\n", rc,
                cdb2_errstr(h));
        return rc;
    }

    rc = cdb2_next_record(h);
    while (rc == CDB2_OK) {
        int n = cdb2_numcolumns(h);
        const char *c = "";
        for (int i = 0; i < n; ++i) {
            if(cdb2_column_type(h, i) == CDB2_CSTRING) {
                char * v = (char *)cdb2_column_value(h, i);
                printf("%s%s", c, v);
                c = ", ";
            }
        }
        if(*c) printf("\n");
        rc = cdb2_next_record(h);
    }

    if (rc == CDB2_OK_DONE)
        rc = 0;
    else
        fprintf(stderr, "Error: cdb2_next_record failed: %d %s\n", rc, cdb2_errstr(h));
    return rc;
}

char * dbname; 
char * table;
void *thr(void *arg){
    cdb2_hndl_tp *db;
    int rc = cdb2_open(&db, dbname, "default",0);
    if(rc != 0){
        fprintf(stderr,"Error: cdb2_open failed: %d\n",rc);
        exit(1);
    }

    thr_info_t *tinfo = (thr_info_t *)arg;
    int i = tinfo->thrid;
    std::ostringstream ss;
    ss << "select * from " << table ;
    std::string s = ss.str();
    for (unsigned int i = 0; i < tinfo->count ; i++){
        int rc = runsql(db, s);
        if(rc) return NULL;
    }

    cdb2_close(db);
    std::cout<< "Done thr" << i << std::endl; 
    return NULL;
}

int main(int argc, char *argv[])
{
    if(argc < 5) {
        fprintf(stderr, "Usage %s DBNAME TABLENAME NUMTHREADS CNTPERTHREAD ITERATIONS\n", argv[0]);
        return 1;
    }

    dbname = argv[1];
    table = argv[2];
    unsigned int numthreads = atoi(argv[3]);
    unsigned int cntperthread = atoi(argv[4]);
    unsigned int iterations = atoi(argv[5]);

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);
    else
        fprintf(stderr, "Error: no config was set\n");

    pthread_t *t = (pthread_t *) malloc(sizeof(pthread_t) * numthreads);
    thr_info_t *tinfo = (thr_info_t *) malloc(sizeof(thr_info_t) * numthreads);
    for(unsigned int it = 0; it < iterations; it++) {
        fprintf(stderr, "starting %d threads\n", numthreads);

        /* create threads */
        for (unsigned long long i = 0; i < numthreads; ++i) {
            tinfo[i].thrid = i;
            tinfo[i].count = cntperthread;
            pthread_create(&t[i], NULL, thr, (void *)&tinfo[i]);
        }

        void *r;
        for (unsigned int i = 0; i < numthreads; ++i)
            pthread_join(t[i], &r);
    }
    std::cout << "Done Main" << std::endl;
    return 0;
}
