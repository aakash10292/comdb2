DROP TABLE t1;

SELECT '---- test #1 ----' as test;
CREATE TABLE t1(i INT, j VARCHAR(1000)) $$
SELECT * FROM t1;
INSERT INTO t1 VALUES(1, 'foo');
INSERT INTO t1 VALUES(2, 'foo baz');
SELECT * FROM t1 ORDER BY i;
DROP TABLE t1;

SELECT '---- test #2 ----' as test;
CREATE TABLE t1(d DATETIME)$$
INSERT INTO t1 VALUES(1);
SELECT * FROM t1;
DROP TABLE t1;

SELECT '---- test #3 ----' as test;
CREATE TABLE t1(i INT, j INT NULL)$$
INSERT INTO t1(i) VALUES(1);
SELECT * FROM t1;
DROP TABLE t1;
