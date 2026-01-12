-- Create types for distributed kmeans
create type kmeansret as (A float4[][]);
create type kmeans_grads_pluj as (gradients Float4[][],counts int[],stats Float4[]);

-- Create functions for distributed kmeans
create or replace function kmeans(Text,Text,int,int,Float4,bool,int,bool) returns setof kmeansret as 'S|ai/sedn/unsupervised/Kmeans|kmeans_control_float_pluj|(Ljava/lang/String;Ljava/lang/String;IIFZIZ)Ljava/util/Iterator;' language ujava;
create or replace function kmeans_gradients_tvm_float_pluj(Text,Text,int,Float4,int,Float4[]) returns  kmeans_grads_pluj as 'B|ai/sedn/unsupervised/Kmeans|kmeans_gradients_tvm_float_pluj|(Ljava/lang/String;Ljava/lang/String;IFI[F)Lai/sedn/unsupervised/GradientReturn;' LANGUAGE ujava;
create or replace function kmeans_gradients_cpu_float_pluj(Text,Text,int,Float4,Float4[]) returns kmeans_grads_pluj as 'B|ai/sedn/unsupervised/Kmeans|kmeans_gradients_cpu_float_pluj|(Ljava/lang/String;Ljava/lang/String;IF[F)Lai/sedn/unsupervised/GradientReturn;' LANGUAGE ujava;
create or replace function kmeans_inference_cpu_float_pluj(Float8[],Float4[]) returns int as 'F|ai/sedn/unsupervised/Kmeans|euclidean_distance_classmembership_cpu_float|([D[[F)I' LANGUAGE ujava;

