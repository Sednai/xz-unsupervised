CREATE EXTENSION MOONSHOT;

-- int
CREATE OR REPLACE FUNCTION f_test_int1(int) RETURNS int AS 'F|ai/sedn/moonshot/Tests|test_int1' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION b_test_int1(int) RETURNS int AS 'B|ai/sedn/moonshot/Tests|test_int1' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION g_test_int1(int) RETURNS int AS 'G|ai/sedn/moonshot/Tests|test_int1' LANGUAGE MSJAVA;

SELECT f_test_int1(3);
SELECT b_test_int1(7);
SELECT g_test_int1(9);

CREATE OR REPLACE FUNCTION f_test_int2(int,int) RETURNS int AS 'F|ai/sedn/moonshot/Tests|test_int2' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION b_test_int2(int,int) RETURNS int AS 'B|ai/sedn/moonshot/Tests|test_int2' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION g_test_int2(int,int) RETURNS int AS 'G|ai/sedn/moonshot/Tests|test_int2' LANGUAGE MSJAVA;

SELECT f_test_int2(3,5);
SELECT b_test_int2(7,3);
SELECT g_test_int2(9,11);

CREATE OR REPLACE FUNCTION f_test_int3(int[]) RETURNS int AS 'F|ai/sedn/moonshot/Tests|test_int3|([I)I' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION b_test_int3(int[]) RETURNS int AS 'B|ai/sedn/moonshot/Tests|test_int3|([I)I' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION g_test_int3(int[]) RETURNS int AS 'G|ai/sedn/moonshot/Tests|test_int3|([I)I' LANGUAGE MSJAVA;

SELECT f_test_int3('{1,2,3,4,5}');
SELECT b_test_int3('{6,5,4,3,2,1}');
SELECT g_test_int3('{1,2,2,1}');

CREATE OR REPLACE FUNCTION f_test_int4(int[]) RETURNS int AS 'F|ai/sedn/moonshot/Tests|test_int4|([[I)I' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION b_test_int4(int[]) RETURNS int AS 'B|ai/sedn/moonshot/Tests|test_int4|([[I)I' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION g_test_int4(int[]) RETURNS int AS 'G|ai/sedn/moonshot/Tests|test_int4|([[I)I' LANGUAGE MSJAVA;

SELECT f_test_int4('{{1,2,3,4,5},{5,4,3,2,1}}');
SELECT b_test_int4('{{6,5,4,3,2,1},{1,2,3,4,5,6}}');
SELECT g_test_int4('{{1,2,2,1},{1,2,2,1}}');


-- double
CREATE OR REPLACE FUNCTION f_test_double1(float8) RETURNS float8 AS 'F|ai/sedn/moonshot/Tests|test_double1' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION b_test_double1(float8) RETURNS float8 AS 'B|ai/sedn/moonshot/Tests|test_double1' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION g_test_double1(float8) RETURNS float8 AS 'G|ai/sedn/moonshot/Tests|test_double1' LANGUAGE MSJAVA;

SELECT f_test_double1(3.3);
SELECT b_test_double1(7.7);
SELECT g_test_double1(9.9);

CREATE OR REPLACE FUNCTION f_test_double2(float8,float8) RETURNS float8 AS 'F|ai/sedn/moonshot/Tests|test_double2' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION b_test_double2(float8,float8) RETURNS float8 AS 'B|ai/sedn/moonshot/Tests|test_double2' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION g_test_double2(float8,float8) RETURNS float8 AS 'G|ai/sedn/moonshot/Tests|test_double2' LANGUAGE MSJAVA;

SELECT f_test_double2(3.2,5.5);
SELECT b_test_double2(7.6,3.1);
SELECT g_test_double2(9.3,11.1);

CREATE OR REPLACE FUNCTION f_test_double3(float8[]) RETURNS float8 AS 'F|ai/sedn/moonshot/Tests|test_double3|([D)D' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION b_test_double3(float8[]) RETURNS float8 AS 'B|ai/sedn/moonshot/Tests|test_double3|([D)D' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION g_test_double3(float8[]) RETURNS float8 AS 'G|ai/sedn/moonshot/Tests|test_double3|([D)D' LANGUAGE MSJAVA;

SELECT f_test_double3('{1.01,2.23,3.11,4.2,5.433}');
SELECT b_test_double3('{6.,231.,5.764,4.43,3.665,2.4323,1.34234}');
SELECT g_test_double3('{1.43,2.,2.3434,1.3}');

CREATE OR REPLACE FUNCTION f_test_double4(float8[]) RETURNS float8 AS 'F|ai/sedn/moonshot/Tests|test_double4|([[D)D' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION b_test_double4(float8[]) RETURNS float8 AS 'B|ai/sedn/moonshot/Tests|test_double4|([[D)D' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION g_test_double4(float8[]) RETURNS float8 AS 'G|ai/sedn/moonshot/Tests|test_double4|([[D)D' LANGUAGE MSJAVA;

SELECT f_test_double4('{{1.23,2.2,3.,4.23,5.2},{5.6,4.45,3.3434,2.3,1.3}}');
SELECT b_test_double4('{{6.,5.34,4.1,3.434,2.4,1.23},{1.75,2.354,3.34,4.2,5.4,6.64}}');
SELECT g_test_double4('{{1.1,2.24,2.232,1.1},{1.4,2.23,2.65,1.676}}');

--complex types
CREATE TYPE TESTTYPE1 as (A int, B float8);

CREATE OR REPLACE FUNCTION f_test_complextype1(int,float8) RETURNS TESTTYPE1 AS 'F|ai/sedn/moonshot/Tests|test_complextype1|(ID)Lai/sedn/moonshot/TestType1;' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION b_test_complextype1(int,float8) RETURNS TESTTYPE1 AS 'B|ai/sedn/moonshot/Tests|test_complextype1|(ID)Lai/sedn/moonshot/TestType1;' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION g_test_complextype1(int,float8) RETURNS TESTTYPE1 AS 'G|ai/sedn/moonshot/Tests|test_complextype1|(ID)Lai/sedn/moonshot/TestType1;' LANGUAGE MSJAVA;

SELECT f_test_complextype1(999,0.333);
SELECT b_test_complextype1(111,0.111);
SELECT g_test_complextype1(321,0.321);

CREATE OR REPLACE FUNCTION f_test_complextype2(TESTTYPE1[]) RETURNS TESTTYPE1 AS 'F|ai/sedn/moonshot/Tests|test_complextype2|([Lai/sedn/moonshot/TestType1;)Lai/sedn/moonshot/TestType1;' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION b_test_complextype2(TESTTYPE1[]) RETURNS TESTTYPE1 AS 'B|ai/sedn/moonshot/Tests|test_complextype2|([Lai/sedn/moonshot/TestType1;)Lai/sedn/moonshot/TestType1;' LANGUAGE MSJAVA;
CREATE OR REPLACE FUNCTION g_test_complextype2(TESTTYPE1[]) RETURNS TESTTYPE1 AS 'G|ai/sedn/moonshot/Tests|test_complextype2|([Lai/sedn/moonshot/TestType1;)Lai/sedn/moonshot/TestType1;' LANGUAGE MSJAVA;

SELECT f_test_complextype2(ARRAY[(1,0.1)::TESTTYPE1]);
SELECT b_test_complextype2(ARRAY[(2,0.2)::TESTTYPE1]);
SELECT g_test_complextype2(ARRAY[(3,0.3)::TESTTYPE1]);

--setof
CREATE OR REPLACE FUNCTION f_test_setof1(TESTTYPE1[]) RETURNS SETOF TESTTYPE1 AS 'F|ai/sedn/moonshot/Tests|test_setof1|([Lai/sedn/moonshot/TestType1;)Ljava/util/Iterator;' LANGUAGE MSJAVA;

SELECT f_test_setof1(ARRAY[(1,0.1)::TESTTYPE1,(2,0.2)::TESTTYPE1,(3,0.3)::TESTTYPE1]);

--Non-JDBC
CREATE TABLE test_table1(id int, data float8[]);
INSERT INTO test_table1 (id,data) VALUES (1,'{0.1,0.03,0.05,1.23}'),(2,'{0.5,0.23,0.15,1.53}'),(3,'{0.25,0.63,0.29,3.19}');

CREATE OR REPLACE FUNCTION f_test_njdbc1() RETURNS SETOF TESTTYPE1 AS 'S|ai/sedn/moonshot/Tests|test_njdbc1|()Ljava/util/Iterator;' LANGUAGE MSJAVA;

SELECT f_test_njdbc1();

--Cleanup
DROP TABLE test_table1;
DROP TYPE TESTTYPE1 CASCADE;
DROP EXTENSION MOONSHOT CASCADE;
