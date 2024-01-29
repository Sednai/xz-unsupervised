# xz-unsupervised - branch active on gaiadbgpu

## postgres install for pljava 
```
select sqlj.install_jar('file:///ZNVME/xz4/app/misc/xz-unsupervised/target/unsupervised-0.0.1-SNAPSHOT.jar','unsupervised', true);
select sqlj.set_classpath('public','unsupervised');

create type kmeans_grads as (gradients Float4[],counts int[],stats Float4[]);
create function kmeans_plj(Text,Text,int,int,Float4,bool,int) returns setof Float4[][] as 'ai.sedn.unsupervised.Kmeans.kmeans_control_float' LANGUAGE java;
create function kmeans_gradients_tvm_float(Text,Text,int,Float4,int,Float4[]) returns  kmeans_grads as 'ai.sedn.unsupervised.Kmeans.kmeans_gradients_tvm_float' LANGUAGE java;
create function kmeans_gradients_cpu_float(Text,Text,int,Float4,Float4[]) returns kmeans_grads as 'ai.sedn.unsupervised.Kmeans.kmeans_gradients_cpu_float' LANGUAGE java;
```
