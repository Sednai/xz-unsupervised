# xz-unsupervised

Distributed K-Means unsupervised clustering via gradient descent for TBase (distributed database PostgreSQL fork).

Note that paths below have to be adapted for your local install.


## postgres install of pljava 
```
select sqlj.install_jar('file:///pathto/unsupervised-0.0.1-SNAPSHOT.jar','unsupervised', true);
select sqlj.set_classpath('public','unsupervised');

create type kmeans_grads as (gradients Float4[],counts int[],stats Float4[]);
create function kmeans_plj(Text,Text,int,int,Float4,bool,int,bool) returns setof Float4[][] as 'ai.sedn.unsupervised.Kmeans.kmeans_control_float' LANGUAGE java;
create function kmeans_gradients_tvm_float(Text,Text,int,Float4,int,Float4[]) returns  kmeans_grads as 'ai.sedn.unsupervised.Kmeans.kmeans_gradients_tvm_float' LANGUAGE java;
create function kmeans_gradients_cpu_float(Text,Text,int,Float4,Float4[]) returns kmeans_grads as 'ai.sedn.unsupervised.Kmeans.kmeans_gradients_cpu_float' LANGUAGE java;
```
## Usage under pljava

Necessary to switch first to native arrays via
```
set pljava.nativearrays=on;
```
(Should be set BEFORE pljava is first invocated, otherwise may take some time to propagate)

CPU example for data in double array, 5 centroids, 3 iterations and 50% of data sampled on each iteration and centroid history returned: 

`select kmeans_plj('dr4_ops_cs48_tmp.lorenzo_v3','attrs',5,3,50.,False,0,True);`

GPU example for data in double array, 5 centroids, 3 iterations, 10000 gpu batch size and 50% of data sampled on each iteration, no centroid history returned: 

`select kmeans_plj('dr4_ops_cs48_tmp.lorenzo_v3','attrs',5,3,50.,True,10000,False);`

CPU example for data in float columns, 3 centroids, 10 iterations, 1% of data, centroid history returned:

`select kmeans_plj('dr4_ops_cs48_tmp.testdata','d1,d2,d3',3,10,1.,False,30000,True);`

## postgres install of moonshot
```
CREATE FUNCTION ms_restart_workers() RETURNS int
     AS '/pathto/moonshot.so', 'moonshot_restart_workers'
     LANGUAGE C;

CREATE FUNCTION ms_show_exec_queue() RETURNS int
     AS '/pathto/moonshot.so', 'moonshot_show_queue'
     LANGUAGE C;

CREATE FUNCTION ms_clear_exec_queue() RETURNS int
     AS '/pathto/moonshot.so', 'moonshot_clear_queue'
     LANGUAGE C;

CREATE FUNCTION kmeans_gradients_cpu_float_ms(Text,Text,int,Float4,Float4[]) returns  kmeans_grads as '/pathto/moonshot.so','kmeans_gradients_cpu_float' LANGUAGE C;

CREATE FUNCTION kmeans_gradients_tvm_float_ms(Text,Text,int,Float4,int,Float4[]) returns  kmeans_grads as '/pathto/moonshot.so','kmeans_gradients_tvm_float' LANGUAGE C;

CREATE FUNCTION kmeans_ms(Text,Text,int,int,Float4,bool,int,bool) returns setof Float4[][] as 'ai.sedn.unsupervised.Kmeans.kmeans_control_float_ms' LANGUAGE java;
```
## Usage under moonshot
As for pljava (in particular `set pljava.nativearrays=on`), but `kmeans_ms`. 

Note that the moonshot background workers live on the datanodes. Hence the maintenance functions have to be called via execute direct.
