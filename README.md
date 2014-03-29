PIGUDFs
=======

TOHEX and TOUUID PIG UDFS used to convert the cassandra blob and UUID columns to corresponding HEX string and UUID in PIG

Usage

Use this as follows
newkeyspacetest2 = LOAD 'cql://keyspace1/users' USING CqlStorage() as (fname:tuple(t1,t2),age:tuple(t3,t4),blobcol:tuple(t5,t6),lname:tuple(t7,t8),time:tuple(t9,t10));
newkeyspacetest3 = FOREACH newkeyspacetest2 generate fname.t2, age.t4, TOHEX(blobcol.t6), lname.t8, TOUUID(time.t10);
store newkeyspacetest3 into users;

blobcol.t6 in cassandra is a column of datatype blob - when you use inside TOHEX() - You will get the blob values as a HEX string
time.t10 in cassandra is a column of datatype TIMEUUID - when you use inside TOUUID() - You will get the timeuuid values as a UUID string