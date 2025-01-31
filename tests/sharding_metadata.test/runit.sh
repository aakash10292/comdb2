#!/bin/bash

# Setup comdb2db tables- modeled from incoh_remsql test
function create_comdb2db
{
    # Create comdb2db tables
    $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "create table if not exists clusters {`cat clusters.csc2`}" >/dev/null 2>&1
    $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "create table if not exists machines {`cat machines.csc2`}" >/dev/null 2>&1
    $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "create table if not exists databases {`cat databases.csc2`}" >/dev/null 2>&1

    # Populate databases
    $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "insert into databases(name, dbnum) values('${DBNAME}', 1234) on conflict do nothing" >/dev/null 2>&1
    $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "insert into databases(name, dbnum) values('${SECONDARY_DBNAME}', 1235) on conflict do nothing" >/dev/null 2>&1
    $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "insert into databases(name, dbnum) values('${TERTIARY_DBNAME}', 1236) on conflict do nothing" >/dev/null 2>&1
    $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "insert into databases(name, dbnum) values('${QUATERNARY_DBNAME}', 1237) on conflict do nothing" >/dev/null 2>&1
    $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "insert into databases(name, dbnum) values('${QUINARY_DBNAME}', 1237) on conflict do nothing" >/dev/null 2>&1
    $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "insert into databases(name, dbnum) values('${SENARY_DBNAME}', 1238) on conflict do nothing" >/dev/null 2>&1
    $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "insert into databases(name, dbnum) values('${COMDB2DB_DBNAME}', 1239) on conflict do nothing" >/dev/null 2>&1

    # Populate clusters
    $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "insert into clusters(name, cluster_name, cluster_machs) values('${DBNAME}', 'prod', 'KABOOM') on conflict do nothing" >/dev/null 2>&1
    $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "insert into clusters(name, cluster_name, cluster_machs) values('${SECONDARY_DBNAME}', 'prod', 'KABOOM') on conflict do nothing" >/dev/null 2>&1
    $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "insert into clusters(name, cluster_name, cluster_machs) values('${TERTIARY_DBNAME}', 'prod', 'KABOOM') on conflict do nothing" >/dev/null 2>&1
    $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "insert into clusters(name, cluster_name, cluster_machs) values('${QUATERNARY_DBNAME}', 'prod', 'KABOOM') on conflict do nothing" >/dev/null 2>&1
    $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "insert into clusters(name, cluster_name, cluster_machs) values('${QUINARY_DBNAME}', 'prod', 'KABOOM') on conflict do nothing" >/dev/null 2>&1
    $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "insert into clusters(name, cluster_name, cluster_machs) values('${SENARY_DBNAME}', 'prod', 'KABOOM') on conflict do nothing" >/dev/null 2>&1
    $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "insert into clusters(name, cluster_name, cluster_machs) values('${COMDB2DB_DBNAME}', 'prod', 'KABOOM') on conflict do nothing" >/dev/null 2>&1

    # Populate machines
    if [[ -z ${CLUSTER} ]]; then
        $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "insert into machines(name, cluster, room) values ('localhost', 'KABOOM', 'NY') on conflict do nothing" >/dev/null 2>&1
        comdb2db_hosts="${comdb2db_hosts}:localhost"
    else
        let nnodes=0
        for mach in ${CLUSTER} ; do
            $CDB2SQL_EXE $COMDB2DB_OPTIONS $COMDB2DB_DBNAME default "insert into machines(name, cluster, room) values('${mach}', 'KABOOM', 'NY') on conflict do nothing" >/dev/null 2>&1
            comdb2db_hosts="${comdb2db_hosts}:${mach}"
            let nnodes=nnodes+1
        done
    fi
}

function setup {
	create_comdb2db
}
function stop_all_databases
{
    for node in $CLUSTER ; do
         kill -9 $(cat ${TMPDIR}/${DBNAME}.${node}.pid)
         kill -9 $(cat ${TMPDIR}/${SECONDARY_DBNAME}.${node}.pid)
         kill -9 $(cat ${TMPDIR}/${TERTIARY_DBNAME}.${node}.pid)
         kill -9 $(cat ${TMPDIR}/${QUATERNARY_DBNAME}.${node}.pid)
         kill -9 $(cat ${TMPDIR}/${QUINARY_DBNAME}.${node}.pid)
         kill -9 $(cat ${TMPDIR}/${SENARY_DBNAME}.${node}.pid)
    done
}

function fail_exit()
{
    echo "Failed $@" | tee ${DBNAME}.fail_exit # runtestcase script looks for this file
    touch $stopfile
    if [[ $keepup_on_failure == 0 ]]; then
        stop_all_databases
        exit -1
    fi
}

function setup_testcase {
	$CDB2SQL_EXE $CDB2_OPTIONS $DBNAME default "DROP TABLE IF EXISTS t DELETEPARTITIONS"
}

function verify_metadata {
	for shard in $SHARDS_LIST; do
		count=`$CDB2SQL_EXE $CDB2_OPTIONS $SHARD default "SELECT count(*) from comdb2_hashpartitions"`
		if [[ $count != 6 ]]; then
			fail_exit "verify_metadata failed on $shard. Expected 6 shards but got $count shards"
		fi
	done	
}

function verify_remote_tables {
	for shard in $SHARDS_LIST; do
		count=`$CDB2SQL_EXE $CDB2_OPTIONS $SHARD default "SELECT count(*) from comdb2_tables where tablename='t'"`
		if [[ $count != 1 ]]; then
			fail_exit "verify_remote_tables failed on $shard. Expected 1 table but found $count shards"
		fi
	done	

}

function partition_only_test {
	$CDB2SQL_EXE $CDB2_OPTIONS $DBNAME default "CREATE TABLE IF NOT EXISTS t(a int) PARTITIONED BY (a) PARTITIONS(${SHARDS_QUERY})"
	verify_metadata
}

function partition_with_remote_tables_test {
	$CDB2SQL_EXE $CDB2_OPTIONS $DBNAME default "CREATE TABLE IF NOT EXISTS t(a int) PARTITIONED BY (a) PARTITIONS(${SHARDS_QUERY}) CREATEPARTITIONS"
	verify_metadata
	verify_remote_tables
}

function run_test {
	testcase="testing only partition creation"
	setup_testcase  
	partition_only_test

	testcase="testing partition and remote table creation"
	setup_testcase
	partition_with_remote_tables_test
}

rm $stopfile >/dev/null 2>&1 


SHARDS_LIST="$DBNAME $SECONDARY_DBNAME"
#$TERTIARY_DBNAME $QUATERNARY_DBNAME $QUINARY_DBNAME"
SHARDS_QUERY=""
for shard in $SHARDS_LIST; do
	SHARDS_QUERY += " ${shard}.t"
done
echo $SHARDS_QUERY

setup
run_test
stop_all_databases

if [[ -f $stopfile ]]; then
    echo "Testcase failed"
    exit -1
fi

echo "Success" 
