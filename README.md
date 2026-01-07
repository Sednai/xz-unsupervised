# xz-unsupervised - AERO branch

## Installation
Requires Postgres v15+, Java 21+, plUniJava and TornadoVM.

Clone and run
```
mvn install
```

Then create the control functions via executing `xz-unsupervised--init.sql` in your database. Note that this branch only works with plUniJava. The build `.jar` has to be on the classpath in `pluj.vmoptions`.

## Usage

Distributed `kmeans` can be called as follows
```
select kmeans('tablename','colname', K, iterations, sampleRate, useGPU, GPUbatchsize, returnCentroidHistory);
```
with
```
tablename : Name of the data table
colname : Name of the column with the data array in double precision
K : Number of centroids
iterations : Number of iterations to run
sampleRate : % of data to sample in each iteration
useGPU : Usage of TornadoVM (True or False)
GPUbatchsize: Batch size to use for TornadoVM
returCentroidHistory: Return the centroids of each iteration (True or False)
```