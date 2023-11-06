package ai.sedn.unsupervised;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ArrayList;
import org.postgresql.pljava.ResultSetProvider;
import org.postgresql.pljava.adt.Datetime.Time;

import uk.ac.manchester.tornado.api.GridScheduler;
import uk.ac.manchester.tornado.api.ImmutableTaskGraph;
import uk.ac.manchester.tornado.api.TaskGraph;
import uk.ac.manchester.tornado.api.TornadoExecutionPlan;
import uk.ac.manchester.tornado.api.WorkerGrid;
import uk.ac.manchester.tornado.api.WorkerGrid1D;
import uk.ac.manchester.tornado.api.enums.DataTransferMode;
import uk.ac.manchester.tornado.api.KernelContext;

/**
 * Distributed kmeans via stochastic gradient descent
 * 
 * (c) 2023 sednai sarl
 * 
 * @author krefl
 */
public class Kmeans {
	private static String m_url = "jdbc:default:connection";

	private static db_object prepare_db_data(String table, String cols, float batch_percent) throws SQLException {
		// Init db connection
		Connection conn = DriverManager.getConnection(m_url);
		
		// Detect # cols:
		int Nc = cols.split(",").length;
		
		String query;
		boolean array;
		if (Nc > 1) {
			// Query for individual columns
			query = "select "+cols+" from "+table+" TABLESAMPLE SYSTEM("+batch_percent+");"; 
			array = false;
		} else {
			// Check if array length is encoded in cols:
			String[] parts = cols.split(":");
			if(parts.length < 2) {
				// Query db for Nc
				PreparedStatement stmt = conn.prepareStatement("select ARRAY_LENGTH("+cols+",1) from "+table+" limit 1;");
				ResultSet rs = stmt.executeQuery();	
				rs.next();
				Nc = rs.getInt(1);
			} else {
				Nc = Integer.valueOf(parts[1]);
			}
			
			// Query for 1D array
			query = "select "+parts[0]+" from "+table+" TABLESAMPLE SYSTEM("+batch_percent+") where cardinality("+parts[0]+")!=0;"; 	
			array = true;
		}
		
		PreparedStatement stmt = conn.prepareStatement(query);
		ResultSet rs = stmt.executeQuery();	
			
		return new db_object(rs,Nc,array);	
	}
	
	
	public static String kmeans_test(float[][] in) {
		
		String s = "TEST:";
		
		/*
		for(int i = 0; i< in.length; i++) {
			for(int j = 0; j < in[i].length; i++) {
				s += in[i][j]+" ";
			}
			s+= "\n";
		}
		*/
		
		return s;
	}
	
	// TEST <- To Remove !
	public static void kmeans_main(String[] args) {
		
		int Kin = 5;
		int Ncin = 80;
		int tvm_batch_size = 100000;
		float[] in_centroids = new float[Kin*Ncin];
		
		
		// Vars for Tornado
		int[] Nc = new int[1];
		Nc[0] = Ncin;
		
		int[] N = new int[1];
		float[] centroids = new float[Kin*Nc[0]];
		System.arraycopy(in_centroids, 0, centroids, 0, in_centroids.length);
		
		int[] ccentroid = new int[tvm_batch_size];
		float[] v_batch = new float[tvm_batch_size*Nc[0]];
		float[] d = new float[tvm_batch_size*Kin];
		int[] K = new int[1];
		K[0] = Kin;
			
		// Centroids length
		float[] centroids_L = approx_eucld_centroid_length(float_1D_to_float_2D(in_centroids, K[0], Nc[0]), K[0], Nc[0]);
				
		
		// Init Tornado
		WorkerGrid  gridworker = new WorkerGrid1D(N[0]);
		
		GridScheduler gridScheduler = new GridScheduler();
		
		KernelContext context = new KernelContext();    
		TaskGraph taskGraph = new TaskGraph("s0")
				.transferToDevice(DataTransferMode.FIRST_EXECUTION, centroids, ccentroid, d, Nc, K, centroids_L)       	
				.transferToDevice(DataTransferMode.EVERY_EXECUTION, v_batch, N)
	        	.task("t0", Kmeans::approx_euclidean_distance_tvm_kernel, context,  v_batch, centroids, N, d, Nc, K, centroids_L)
	        	.task("t1", Kmeans::search_min_distance_tvm_kernel, context, d, N, ccentroid, K)
	        	.transferToHost(DataTransferMode.EVERY_EXECUTION, ccentroid);
		
		ImmutableTaskGraph immutableTaskGraph = taskGraph.snapshot();
		TornadoExecutionPlan executor_distance = new TornadoExecutionPlan(immutableTaskGraph);
		
		gridScheduler.setWorkerGrid("s0.t0", gridworker);
		gridScheduler.setWorkerGrid("s0.t1", gridworker);
		
		for(int i = 0; i < 100; i++) {

    	    System.out.println((i+1)+"th exec");
    	   
			// Calc all distances
			gridworker.setGlobalWork(N[0], 1, 1);
			
    	    executor_distance.withGridScheduler(gridScheduler).execute();
    	    
		}
		
		
		
	}
	
	/**
	 * Runs K-means algorithm on datanodes
	 * 
	 * @param table : database table with data
	 * @param cols : Column names ',' separated or name of column with 1D array (size can optionally be indicated with name:int)
	 * @param K : Number of centroids
	 * @param I : Number of iterations
	 * @param batch_percent : Percent of data to be considered for the batch
	 * @param use_tvm : Use TornadoVM for gradient calculation
	 * @param tvm_batch_size : Batch size for device
	 * @param return: Iterator over epoche centroids
	 * @throws SQLException
	 */
	public static Iterator kmeans_control_float(String table, String cols, int K, int I, float batch_percent, boolean use_tvm, int tvm_batch_size) throws SQLException {
		
		// Init db connection
		Connection conn = DriverManager.getConnection(m_url);
				
		// Obtain datanode information
		String query = "select string_agg(node_name,',') from pgxc_node where node_type='D';";
		
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(query);	
		rs.next();
		
		String nodes = rs.getString(1);
		int Nn = nodes.split(",").length;
			
		// Query for Nc
		int Nc = cols.split(",").length;
		String[] parts = cols.split(":");
		boolean array = false;
		if(Nc < 2) {
			array = true;
			// Check if array length is encoded in cols:
			if(parts.length < 2) {
				// Query db for Nc
				stmt = conn.createStatement();
				rs = stmt.executeQuery("select ARRAY_LENGTH("+cols+",1) from "+table+" limit 1;");	
				rs.next();
				Nc = rs.getInt(1);
				
			} else {
				Nc = Integer.valueOf(parts[1]);
			}		
		}
			
		// Query for random datapoints as initial centroids (globally selected)
		float[][] centroids = new float[K][Nc];
		
		if(!array) {
			query = "select "+cols+" from "+table+" TABLESAMPLE SYSTEM(0.25) limit "+K;
		} else {
			query = "select "+parts[0]+" from "+table+" TABLESAMPLE SYSTEM(0.25) where cardinality("+parts[0]+")!=0 limit "+K;
		}
		
		stmt = conn.createStatement();
		rs = stmt.executeQuery(query);	
		
		for(int i = 0; i < K; i++) {
			rs.next();
			
			if(!array) {
				for(int c = 1; c < Nc; c++) {
					centroids[i][c-1] = rs.getFloat(c);
				}
			} else {
				Double[] A = (Double[]) rs.getObject(1); // <- To be changed after change in DB ! ( to Float )
				
				for(int c = 0; c < Nc; c++) {
					centroids[i][c] = A[c].floatValue();	
				}
			}

		}
			
		// Set function
		String func;
		if(use_tvm) {
			func = "execute direct on ("+nodes+") $$select kmeans_gradients_tvm_float";
			func += "('"+table+"','"+cols+"',"+K+","+batch_percent+","+tvm_batch_size+",";
		}
		else {
			func = "execute direct on ("+nodes+") $$select kmeans_gradients_cpu_float";
			func += "('"+table+"','"+cols+"',"+K+","+batch_percent+",";
		}
				
		float[][][] partialGradients = new float[Nn][K][Nc];
		Integer[][] Ccounts = new Integer[Nn][K];
		
		ArrayList<float[][]> receiver = new ArrayList<>();
		
		// Main loop
		for(int i = 0; i < I; i++) {
			// Reset
			int[] counts = new int[K];
			float[][] gradients = new float[K][Nc];
		
			// Set query
			query = func + "'"+getPGarrayFrom2Darray(centroids)+"')$$;";
			
			// Execute
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			
			// Collect
			int c = 0;
			float[] stats = new float[1];
			
			while(rs.next()) { 
				
				ResultSet tmp = (ResultSet) rs.getObject(1);
				// 1. Gradients as 1D Float array <- Autoconvert from plJava. Fix for new versions
				partialGradients[c] = float_1D_to_float_2D( cast1DFloatArray((Float[]) tmp.getObject(1)), K, Nc);
				
				// 2. Counts as 1D Int array
				Ccounts[c] = (Integer[]) tmp.getObject(2);
						
				// 3. Collect statistics
				float[] stats_tmp = cast1DFloatArray( (Float[]) tmp.getObject(3) );
				
				// Add
				vec_add(stats, stats_tmp);
				
				c++;			
			}
		
			vec_mul(stats,(float) 1./c);
			
			// Calc new centroids
			for(int n = 0; n < Nn; n++) {
				for(int k = 0; k < K; k++) {
					if(Ccounts[n][k] > 0) {
						// Add counts
						counts[k] += Ccounts[n][k];
						// Add partial gradients
						for(int j = 0; j < Nc; j++) {
							gradients[k][j] += partialGradients[n][k][j];
						}
					}
				}
			}
				
			// Add
			for(int k=0; k < K; k++) {
				if(counts[k] > 0) {
					for(int j = 0; j < Nc; j++) {
						centroids[k][j] += 1./counts[k]*gradients[k][j];
					}
				}
			}	
		
		// Add to return
		receiver.add( array2DdeepCopy(centroids) );
		}
	
		// Force GC
		System.gc();
		System.runFinalization();
		
		return receiver.iterator();		
	}
	
	/**
	 * CPU based calculation of kmeans stochastic gradients
	 * (Distances are efficiently calculated via decomposition of the norm)
	 * (Return to db: Centroid gradients as float[][] and # update counter as int[])
	 * 
	 * @param table : database table with data
	 * @param cols : Column names ',' separated or name of column with 1D array (size can optionally be indicated with name:int)
	 * @param K : Number of centroids
	 * @param batch_percent : Percent of data to be considered for the batch
	 * @param in_centroids : Initial centroid positions encoded in 1D array
	 * @param receiver
	 * @return
	 * @throws SQLException
	 */
	public static boolean kmeans_gradients_cpu_float(String table, String cols, int K, float batch_percent, float[] in_centroids, ResultSet receiver) throws SQLException {
				
		// Prepare statistics collection
		float[] runstats = new float[2];
		long tic_global = System.currentTimeMillis();
		
		// Prepare data ResultSet
		db_object rs = prepare_db_data(table,cols,batch_percent);
		
		// # columns
		int Nc = rs.Nc;
		
		// Unpack initial centroids;
		float[][] centroids = float_1D_to_float_2D(in_centroids, K, Nc);
		float[] centroids_L = approx_eucld_centroid_length(centroids, K, Nc);
		
		// Cluster member count
		int[] ncount = new int[K];
		// Gradients
		float[][] gradients = new float[K][Nc];
			
		// Main loop over data
		while ( rs.R.next() ) {
			
			// Build vec
			long tic_io = System.currentTimeMillis();
			
			float[] v = new float[Nc];
			
			if(rs.array) {
				// 1D array
				Double[] A = (Double[]) rs.R.getObject(1); // <- To be changed after change in DB ! ( to Float )
				
				for(int c = 0; c < Nc; c++) {
					v[c] = A[c].floatValue();	
				}
			} else {
				// True cols
				for(int c = 1; c < Nc+1; c++) {
					v[c-1] = rs.R.getFloat(c);
				}
			}
			long toc_io = System.currentTimeMillis();
			runstats[1] += toc_io-tic_io;
		
			// Find min distance centroid
			int minc = 0;
			float d = approx_euclidean_distance(v,centroids[0],centroids_L[0]);
			for(int k = 1; k < K; k++) {
				
				float dist = approx_euclidean_distance(v,centroids[k],centroids_L[k]);
				
				if(dist < d) {
					minc = k;
					d = dist;
				}
			}
			
			ncount[minc]++;
			
			// Add to gradient
			vec_add(gradients[minc],v);				
		}
		
		// Add centroid contributions
		for(int k = 0; k < K; k++) {
			vec_muladd(gradients[k],-ncount[k],centroids[k]);
		}
		
		long toc_global = System.currentTimeMillis();
		
		runstats[0] = toc_global-tic_global;
		
		receiver.updateObject(1, cast2DFloatArray(gradients));		
		receiver.updateObject(2, ncount);		
		receiver.updateObject(3, runstats);
		
		// Force GC
		System.gc();
		System.runFinalization();

		return true;
	}
	
	/**
	 * Calculate the class membership for vector under given centroids
	 * 
	 * @param v_in : vector
	 * @param in_centroids : centroids
	 * @return class membership as int
	 * @throws SQLException
	 */
	public static int euclidean_distance_classmembership_cpu_float(double[] v_in, float[] in_centroids) throws SQLException {
		int Nc = v_in.length;
		int K = in_centroids.length/Nc;
		
		// <- Remove uppon change of db to double table
		// Convert in
		float[] v = new float[Nc];
		for(int i = 0; i < Nc; i++) {
			v[i] = (float) v_in[i];
		}
		
		// Unpack centroids;
		float[][] centroids = float_1D_to_float_2D(in_centroids, K, Nc);

		// Precompute centroid dists
		float[] nc = new float[K];
		for(int k = 0; k < K; k++) {
			nc[k] = 0;
			for(int i = 0; i < Nc; i++) {
				nc[k] += centroids[k][i]*centroids[k][i];
			}
			nc[k] *= 0.5;
		}
			
		int minc = 0;
		float d = approx_euclidean_distance(v,centroids[0],nc[0]);
		for(int k = 1; k < K; k++) {
			
			float dist = approx_euclidean_distance(v,centroids[k],nc[k]);
			
			if(dist < d) {
				minc = k;
				d = dist;
			}
		}		
		
		return minc;	
	}
	
	
	/**
	 * TornadoVM based calculation of kmeans stochastic gradients
	 * (Distances are efficiently calculated via decomposition of the norm)
	 * (Return to db: Centroid gradients as float[][] and # update counter as int[])
	 * 
	 * @param table : database table with data
	 * @param cols : Column names ',' separated or name of column with 1D array (size can optionally be indicated with name:int)
	 * @param Kin : Number of centroids
	 * @param batch_percent : Percent of data to be considered for the batch
	 * @param tvm_batch_size: Size of batches to process simultaneously
	 * @param in_centroids : Initial centroid positions encoded in 1D array
	 * @param receiver
	 * @return
	 * @throws SQLException
	 */
	public static boolean kmeans_gradients_tvm_float(String table, String cols, int Kin, float batch_percent, int tvm_batch_size, float[] in_centroids, ResultSet receiver) throws SQLException {
		// Prepare stats
		float[] runstats = new float[2];
		
		long tic_global = System.currentTimeMillis();
			
		// Prepare data ResultSet
		db_object rs = prepare_db_data(table,cols,batch_percent);
		
		// Vars for Tornado
		int[] Nc = new int[1];
		Nc[0] = rs.Nc;
		
		int[] N = new int[1];
		float[] centroids = new float[Kin*Nc[0]];
		System.arraycopy(in_centroids, 0, centroids, 0, in_centroids.length);
		
		int[] ccentroid = new int[tvm_batch_size];
		float[] v_batch = new float[tvm_batch_size*Nc[0]];
		float[] d = new float[tvm_batch_size*Kin];
		int[] K = new int[1];
		K[0] = Kin;
			
		// Centroids length
		float[] centroids_L = approx_eucld_centroid_length(float_1D_to_float_2D(in_centroids, K[0], Nc[0]), K[0], Nc[0]);
				
		
		// Init Tornado
		WorkerGrid  gridworker = new WorkerGrid1D(N[0]);
		
		GridScheduler gridScheduler = new GridScheduler();
		
		KernelContext context = new KernelContext();    
		TaskGraph taskGraph = new TaskGraph("s0")
				.transferToDevice(DataTransferMode.FIRST_EXECUTION, centroids, ccentroid, d, Nc, K, centroids_L)       	
				.transferToDevice(DataTransferMode.EVERY_EXECUTION, v_batch, N)
	        	.task("t0", Kmeans::approx_euclidean_distance_tvm_kernel, context,  v_batch, centroids, N, d, Nc, K, centroids_L)
	        	.task("t1", Kmeans::search_min_distance_tvm_kernel, context, d, N, ccentroid, K)
	        	.transferToHost(DataTransferMode.EVERY_EXECUTION, ccentroid);
		
		ImmutableTaskGraph immutableTaskGraph = taskGraph.snapshot();
		TornadoExecutionPlan executor_distance = new TornadoExecutionPlan(immutableTaskGraph);
		
		gridScheduler.setWorkerGrid("s0.t0", gridworker);
		gridScheduler.setWorkerGrid("s0.t1", gridworker);
		
		// Init return vars
		int[] ncount = new int[K[0]];
		float[][] gradients = new float[K[0]][Nc[0]];
		
		// Do batching
		boolean stop = false;
		while(!stop) {
			
			// Build batch
			long tic_io = System.currentTimeMillis();
			N[0] = 0;
			for(int i = 0; i < tvm_batch_size; i++) {
				// Stop if no more data
				if(!rs.R.next()) {
					stop = true;
					break;
				}
				// Set data
				if(rs.array) {
					// 1D array
					Double[] A = (Double[]) rs.R.getObject(1); // <- To be changed after change in DB ! ( to Float )
					
					for(int c = 0; c < Nc[0]; c++) {
						v_batch[i*Nc[0]+c] = A[c].floatValue();	
					}
					
				} else {
					for(int c = 1; c < Nc[0]+1; c++) {
						v_batch[i*Nc[0]+c-1]= rs.R.getFloat(c);
					}
				}
				N[0]++;
			}
			long toc_io = System.currentTimeMillis();
			runstats[1] += toc_io-tic_io;
			
			// Calc
			if(N[0] > 0) {
				
				// Calc all distances
				gridworker.setGlobalWork(N[0], 1, 1);
				
	    	    executor_distance.withGridScheduler(gridScheduler).execute();
	    	    
	    	    // Calc counts
				for(int i = 0; i < N[0]; i++) {
					
					ncount[ ccentroid[i] ]++;
					
					// Add to gradient
					vec_add(gradients[ ccentroid[i] ], getRowFrom2Darray(v_batch,i,Nc[0]));	
				}
			}	
		}
		
		// Add centroid contributions
		for(int k = 0; k < K[0]; k++) {
			vec_muladd(gradients[k],-ncount[k],getRowFrom2Darray(in_centroids,k,Nc[0]));
		}
		
		long toc_global = System.currentTimeMillis();
		runstats[0] = toc_global-tic_global;
		
		receiver.updateObject(1, cast2DFloatArray(gradients));		
		receiver.updateObject(2, ncount);		
		receiver.updateObject(3, runstats);
		
		executor_distance.freeDeviceMemory();
		
		// Force GC
		System.gc();
		System.runFinalization();
		
		return true;
	}
	
	/**
	 * Calculates approximate euclidean distance
	 * @param v1 : vector
	 * @param v2 : centroid
	 * @param nc : approximate centroid length
	 * @return
	 */
	public static float approx_euclidean_distance(float[] v1, float[] v2, float nc) {
		float d = 0;
		for(int i = 0; i < v1.length; i++) {
			d += v1[i]*v2[i];
		}
		return nc-d;
	}
	
	
	
	/**
	 * Calculates approximate euclidean length of centroids
	 * 
	 * @param centroids : Input centroids
	 * @param K : Number of centroids
	 * @param Nc : Number of columns
	 * @return : Approximate euclidean length 
	 */
	private static float[] approx_eucld_centroid_length(float[][] centroids, int K, int Nc) {
		float[] nc = new float[K];
		for(int k = 0; k < K; k++) {
			nc[k] = 0;
			for(int i = 0; i < Nc; i++) {
				nc[k] += centroids[k][i]*centroids[k][i];
			}
			nc[k] *= 0.5;
		}
		
		return nc;
	}
	
	/**
	 * Method to add two vectors: v1 = v1 + v2
	 * (given by 1D arrays)
	 * 
	 * @param v1 : vector 1
	 * @param v2 : vector 2
	 */
	private static void vec_add(float[] v1, float[] v2) {
		for(int i = 0; i < v1.length; i++) {
			v1[i] = v1[i]+v2[i];
		}	
	}
	
	/**
	 * Method to multiply vector by constant: v = v*c
	 * @param v : vector
	 * @param c : constant
	 */
	private static void vec_mul(float[] v, float c) {
		for(int i = 0; i < v.length; i++) {
			v[i] = v[i]*c;
		}	
	}
	
	
	/**
	 * Method to add multiple of vector to vector: v1 = v1 + alpha v2;
	 * @param v1 : vector 1
	 * @param alpha : constant
	 * @param v2 vector 2
	 */
	public static void vec_muladd(float[] v1, float alpha,float[] v2) {
		for(int i = 0; i < v1.length; i++) {
			v1[i] = v1[i]+alpha*v2[i];
		}
	}	
	
	/**
	 * Method to unpack 1D array to 2D array 
	 * (Assumes 1D array corresponds to rectangular matrix)
	 * @param F : 1D input array
	 * @param d1 : dimension 1
	 * @param d2 : dimension 2
	 * @return : Unpacked 2D array
	 */
	private static float[][] float_1D_to_float_2D(float[] F, int d1, int d2) {
		float[][] A = new float[d1][d2];
		
		for(int i = 0; i < d1; i++) {
			System.arraycopy(F, i*d2, A[i], 0, d2);
		}
		
		return A;
	}
	
	/** 
	 * Method to do a deep copy of 2d float array
	 * @param IN : Array to copy
	 * @return Deep copy of IN
	 */
	private static float[][] array2DdeepCopy(float[][] IN) {
		float[][] A = new float[IN.length][];
		
		for(int i = 0; i < IN.length; i++) {
			A[i] = new float[IN[i].length];
			System.arraycopy(IN[i], 0, A[i], 0, IN[i].length);
		}
		
		return A;
	}
	
	/**
	 * Method to convert float[][] to Float[][]
	 * @param A
	 * @return
	 */
	private static Float[][] cast2DFloatArray(float[][] A) {
		Float[][] N = new Float[A.length][];
		
		for(int i = 0; i < A.length; i++) {
			N[i] = new Float[A[i].length];
			for(int j = 0; j < A[i].length; j++) {
				N[i][j] = A[i][j];
			}
		}
		
		return N;
	}
	
	/**
	 * Method to convert Float[] to float[]
	 * @param A
	 * @return
	 */
	private static float[] cast1DFloatArray(Float[] A) {
		float[] N = new float[A.length];
		
		for(int i = 0; i < A.length; i++) {
				N[i] = A[i];
		}
		return N;
	}
	
	
	/**
	 * Tornado kernel to search for min distance in parallel over batch
	 * @param context
	 * @param dist
	 * @param N
	 * @param ccentroid
	 * @param Ncentr
	 */
	public static void search_min_distance_tvm_kernel(KernelContext context, float[] dist, int[] N, int[] ccentroid, int[] Ncentr) {
		int i = context.globalIdx;
		
		float mind = dist[i*Ncentr[0]];
		ccentroid[i] = 0;
		
		for(int j = 1; j < Ncentr[0]; j++) {
			if( dist[i*Ncentr[0]+j] < mind) {
				ccentroid[i] = j;
				mind = dist[i*Ncentr[0]+j];
			}
		}	
	}
	
	
	/**
	 * Tornado kernel to compute approximate euclidean distance for batch of data
	 * 
	 * @param context
	 * @param v1
	 * @param v2
	 * @param N
	 * @param d
	 * @param Nc
	 * @param Ncentr
	 * @param nc
	 */
	public static void approx_euclidean_distance_tvm_kernel(KernelContext context, float[] v1, float[] v2, int[] N, float[] d, int[] Nc, int[] Ncentr, float[] nc) {
		
		// Encoded as flat arrays
		// v1: N[0] x Nc = i*Nc + j
		// v2: Ncentr x Nc = i*Nc + j 
		// d : N[0] x Ncentr = i*Ncentr + j
		
		int i = context.globalIdx;
		for(int j = 0; j < Ncentr[0]; j++)  {
			float tmp = 0;
			for(int k = 0; k < Nc[0]; k++) {
				tmp += v1[i*Nc[0]+k] * v2[j*Nc[0]+k];
			}	
			d[i*Ncentr[0]+j] = nc[j]-tmp;
		}
	}
	
	/**
	 * Euclidean distance of two vectors
	 * @param v1 : vector 1
	 * @param v2 : vector 2
	 * @return distance
	 */
	public static float euclidean_distance_cpu(float[] v1, float[] v2) {
		float d = 0;
		for(int i = 0; i < v1.length; i++) {
			d += (v1[i]-v2[i])*(v1[i]-v2[i]);
		}
		return (float) Math.sqrt(d);
	}
	
	/**
	 * Returns score for class membership under euclidean distance
	 * 
	 * @param v_in : data vector
	 * @param in_centroids : Centroids
	 * @return scores for class membership
	 * @throws SQLException
	 */
	public static float[] euclidean_classmembership_score_cpu(double[] v_in, float[] in_centroids) throws SQLException {
		int Nc = v_in.length;
		int K = in_centroids.length/Nc;
		
		// Convert in
		float[] v = new float[Nc];
		for(int i = 0; i < Nc; i++) {
			v[i] = (float) v_in[i];
		}
		// Unpack current centroids;
		float[][] centroids = float_1D_to_float_2D(in_centroids, K, Nc);
	
		float[] dist = new float[K];
		float total = 0;
		
		for(int k = 0; k < K; k++) {	
			dist[k] = (float) 1./euclidean_distance_cpu(v,centroids[k]);
			total += dist[k];
		}		
		
		for(int k = 0; k < K; k++) {	
			dist[k]= dist[k]/total;
		}	
		
		return dist;
	}
	
	
	/**
	 * Returns row of 2D matrix encoded in 1D array
	 * @param A : 2D matrix encoded in 1D array
	 * @param r : Row number to return
	 * @param Nc: Number of columns
	 * @return Row as 1D array
	 */
	private static float[] getRowFrom2Darray(float[] A, int r, int Nc) {
		float[] row = new float[Nc];
		
		System.arraycopy(A, Nc*r, row, 0, Nc);
				
		return row;
	}
	
	/**
	 * Converts 2D matrix to postgres 1D array string
	 * @param A : 2D matrix 
	 * @return
	 */
	private static String getPGarrayFrom2Darray(float[][] A) {
		String s = "{";
		
		for(int i = 0; i < A.length; i++) {
			for(int j = 0; j < A[0].length; j++) {
				s+=A[i][j]+",";
			}
		}
		
		s = s.substring(0, s.length()-1);
		s+="}";
		
		return s;
	}
	
}
