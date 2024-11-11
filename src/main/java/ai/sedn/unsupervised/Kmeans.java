package ai.sedn.unsupervised;

import java.sql.*;
import java.util.Iterator;
import java.util.ArrayList;

import uk.ac.manchester.tornado.api.GridScheduler;
import uk.ac.manchester.tornado.api.ImmutableTaskGraph;
import uk.ac.manchester.tornado.api.TaskGraph;
import uk.ac.manchester.tornado.api.TornadoExecutionPlan;
import uk.ac.manchester.tornado.api.WorkerGrid;
import uk.ac.manchester.tornado.api.WorkerGrid1D;
import uk.ac.manchester.tornado.api.WorkerGrid2D;
import uk.ac.manchester.tornado.api.enums.DataTransferMode;
import uk.ac.manchester.tornado.api.KernelContext;


import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;

import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE;


import java.lang.foreign.Arena;
import java.lang.foreign.SequenceLayout;
import java.lang.foreign.StructLayout;
import java.lang.foreign.GroupLayout;
import java.lang.invoke.VarHandle;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.ValueLayout;

/**
 * Distributed kmeans via stochastic gradient descent
 * 
 * (c) 2023 sednai sarl
 * 
 * @author krefl
 */
public class Kmeans {
	
	private static String m_url = "jdbc:default:connection";

	//private static String m_url = "jdbc:postgresql://localhost/pljava_new?socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg&socketFactoryArg=/tmp/.s.PGSQL.40004";
	//private static String m_url = "jdbc:postgresql://localhost:40004/pljava_new";
	
	/*
	public static TestReturn atest() throws SQLException {
		System.out.println("ENTRY OK");
		
		System.out.println(A[0].exectime);
		System.out.println(A[0].freq[0]);
		System.out.println(A[0].amp[0]);
		System.out.println(A[0].periods[0]);

		return A[0];
	}
	*/
	public static boolean btest(ResultSet[] B, ResultSet receiver) throws SQLException {
		System.out.println("ENTRY OK");

		// In
		TestReturn A = new TestReturn();
		
		A.exectime = (long) B[0].getObject(1);
		A.freq = (double[]) B[0].getObject(2);
		A.amp = (double[]) B[0].getObject(3);
		A.periods = (double[]) B[0].getObject(4);
		
		// Exec
		System.out.println(A.exectime);
		System.out.println(A.freq[0]);
		System.out.println(A.amp[0]);
		System.out.println(A.periods[0]);

		
		// Return
		receiver.updateObject(1,A.exectime);
		receiver.updateObject(2,A.freq);
		receiver.updateObject(3,A.amp);
		receiver.updateObject(4,A.periods);
					
		return true;
	}
	
	public static boolean ctest(ResultSet receiver) throws SQLException {
		System.out.println("ENTRY OK");

		Integer[][] A = {{1,2,3,4,5,6,7,8,9,10}, {1,2,3,4,5,6,7,8,9,10},{1,2,3,4,5,6,7,8,9,10},{1,2,3,4,5,6,7,8,9,10},{1,2,3,4,5,6,7,8,9,10},{1,2,3,4,5,6,7,8,9,10},{1,2,3,4,5,6,7,8,9,10},{1,2,3,4,5,6,7,8,9,10},{1,2,3,4,5,6,7,8,9,10},{1,2,3,4,5,6,7,8,9,10}};
		Short[][] B = {{0,1},{1,0}};
		Long[][] C = {{0l,999l},{999l,0l}};
		//byte[][] D = {{127,0},{0,127}};
		
		// Return
		receiver.updateObject(1, A);
		receiver.updateObject(2, B);
		receiver.updateObject(3, C);
		//receiver.updateObject(4, D);
				
		return true;
	}
	
	public static boolean dtest(ResultSet IN, ResultSet receiver) throws SQLException {
		System.out.println("ENTRY OK");

		int[][] A = (int[][]) IN.getObject(1);
		short[][] B = (short[][]) IN.getObject(2);
		long[][] C = (long[][]) IN.getObject(3);
		
		long[][] X = new long[1][];
		
		for(int i = 0; i < X.length; i++) {
			for(int j = 0; j < X[i].length; j++) {
				
			}
		}
		
		
		
		B[0][0] = 0;
		C[0][0] = 0;
		
		// Return
		receiver.updateObject(1, A);
		receiver.updateObject(2, B);
		receiver.updateObject(3, X);
				
		return true;
	}
	
	public static TestReturn2 etest(byte[] BIN) throws SQLException {
		System.out.println("ENTRY OK");

		int[][] A = new int[2][2];
		short[][] B = new short[2][2];
		long[][] C = new long[2][2];
		
		byte[] D = new byte[3];
		
		int[] E = null;
		
		A[0][0] = 1;
		A[0][1] = 2;
		A[1][0] = 3;
		A[1][1] = 4;
		
		B[0][0] = -1;
		B[0][1] = -2;
		B[1][0] = -3;
		B[1][1] = -4;
		
		
		C[0][0] = 1000;
		C[0][1] = 2000;
		C[1][0] = 3000;
		C[1][1] = 4000;
		
		D[0] = 1;
		D[1] = 2;
		D[2] = 3;
		
		
		TestReturn2 R = new TestReturn2();
		R.Test1 = null;
		R.Test2 = B;
		R.Test3 = C;
		R.Test4 = BIN;
		R.Test5 = null;
		
		return R;
	}
	
	public static boolean etest_plj(byte[] BIN,ResultSet receiver) throws SQLException {
		System.out.println("ENTRY OK");

		int[][] A = new int[2][2];
		short[][] B = new short[2][2];
		long[][] C = new long[2][2];
		
		byte[] D = new byte[3];
		
		A[0][0] = 1;
		A[0][1] = 2;
		A[1][0] = 3;
		A[1][1] = 4;
		
		B[0][0] = -1;
		B[0][1] = -2;
		B[1][0] = -3;
		B[1][1] = -4;
		
		
		C[0][0] = 1000;
		C[0][1] = 2000;
		C[1][0] = 3000;
		C[1][1] = 4000;
		
		D[0] = 1;
		D[1] = 2;
		D[2] = 3;
		

		// Return
		receiver.updateObject(1, null);
		receiver.updateObject(2, B);
		receiver.updateObject(3, C);
		receiver.updateObject(4, C);
			
		return true;
	}
	
	
	/*
	public static int atest(TestReturn[] A) throws SQLException {
		System.out.println("ENTRY OK");
		
		System.out.println(A[0].exectime);
		System.out.println(A[0].freq[0]);
		
		return 0;
	}
	*/
	
	public static Iterator rtest() throws SQLException {
		ArrayList L = new ArrayList<TestReturn>();
		
		TestReturn T = new TestReturn();
		T.exectime = 1;
		
		L.add(T);
		L.add(T);
		L.add(T);
		
		return L.listIterator();
	}
	
	public static GradientReturn background_test() throws SQLException {
		
		Moonshot moonshot = new Moonshot();
		try {
			moonshot.execute("select attrs from lorenzo_v3 limit 100000;");
			
			
			int c = 0;
			double[] array;
			
			while(moonshot.fetch_next()) {
				c++;
				array = moonshot.getdoublearray(1);
				//System.out.println(c+": "+moonshot.getdoublearray(1).length);
			
			}
			/*
			moonshot.execute("select attrs from lorenzo_v3 limit 5;");
			
			c = 0;
			while(moonshot.fetch_next()) {
				c++;
				System.out.println(c+": "+moonshot.getdoublearray(1).length);
			}
			
			double[] array;
			do {
				array = moonshot.fetch_next_double_array(1);
				//ms = moonshot.fetch_next_double_ms();
			}
			while(array != null);
			*/
		} catch(Throwable t) {
			System.out.println(t);
		}
		
		
		GradientReturn T =  new GradientReturn();
		T.Test1 = new float[2][2];
		T.Test2 = new int[3];
		T.Test3 = new float[5];
		T.Test2[0] = 4;
		T.Test2[2] = 2;
		
		return T;
	
	}
	
	
	public static GradientReturn kmeans_test(String test) throws SQLException {
		
		Moonshot moonshot = new Moonshot();
		
		try {
			moonshot.connect();
			
			moonshot.execute("select attrs from lorenzo_v3 limit 100000");
			
			long tic = System.nanoTime();
			
			double[] array;
			MemorySegment ms;
			int c = 0;
			do {
				array = moonshot.fetch_next_double_array(1);
				//ms = moonshot.fetch_next_double_ms();
				c+=1;
			}
			while(array != null);
			//while(ms != null);
			
			long toc = System.nanoTime();
			
			System.out.println("D: "+(toc-tic)/1e6+" ("+c+")");
			
			moonshot.disconnect();
			
		} catch(Throwable t) {
			System.out.println(t);
		}
		
		GradientReturn T =  new GradientReturn();
		//T.Test1 = new float[3];
		//T.Test2 = new float[5];
		//T.Test3 = new float[2];
	
		return T;
		
		
	}
	
	public static GradientReturn kmeans_gradients_cpu_float_ms(String table, String cols, int K, float batch_percent, float[] in_centroids) throws SQLException {
		
		// Prepare statistics collection
		float[] runstats = new float[3];
		long tic_global = System.nanoTime();
		
		
		// Prepare data ResultSet
		db_object rs = prepare_db_data_moonshot(table,cols,batch_percent);
		Moonshot moonshot = rs.M;
		
		long tic = System.nanoTime();
		
		//int Nc = 79;
		//int K = 5;
		
		// # columns
		int Nc = rs.Nc;
		// Unpack initial centroids;
		
		float[][] centroids = float_1D_to_float_2D(in_centroids, K, Nc);	
		float[] centroids_L = approx_eucld_centroid_length(centroids, K, Nc);
		
		// Cluster member count
		int[] ncount = new int[K];
		// Gradients
		float[][] gradients = new float[K][Nc];
			
		float[] v = new float[Nc];
		// Main loop over data
		System.out.println("[DEBUG] Before moonshot");
		
		try {
			while ( moonshot.fetch_next() ) {
				
				long tic_io = System.nanoTime();
				double[] A = moonshot.getdoublearray(1);// <- To be changed after change in DB ! ( to Float )
				
				if(A == null) {	
					System.out.println("[DEBUG]: NULL ARRAY!");
					
					break;
				}
					
				for(int c = 0; c < Nc; c++) {
					//v[c] = A[c].floatValue();
					v[c] = (float) A[c];
					//v[c] = B[c].floatValue();				
				}
				
				runstats[1] += System.nanoTime() - tic_io;
				
				// Find min distance centroid
				long tic_cpu = System.nanoTime();
				int minc = 0;
				float d = approx_euclidean_distance(v,centroids[0],centroids_L[0]);
				for(int k = 1; k < K; k++) {
					
					float dist = approx_euclidean_distance(v,centroids[k],centroids_L[k]);
					
					if(dist < d) {
						minc = k;
						d = dist;
					}
				}
				
				runstats[2] += System.nanoTime() - tic_cpu;
				
				ncount[minc]++;
				
				// Add to gradient
				vec_add(gradients[minc],v);
			}
			
		} catch(Throwable t) {		
			throw new SQLException(t);
		}
		
		
		// Add centroid contributions
		for(int k = 0; k < K; k++) {
			vec_muladd(gradients[k],-ncount[k],centroids[k]);
		}
		
		runstats[0] = (System.nanoTime() - tic_global)/1e6f;
		runstats[1] /= 1e6f;
		runstats[2] /= 1e6f;
		
		GradientReturn T =  new GradientReturn();
		
		T.Test1 = gradients;
		T.Test2 = ncount;
		T.Test3 = runstats;
		
		
		try {
			moonshot.disconnect();
		} catch(Throwable t) {
			throw new SQLException(t);
		}
		
		
		// Force GC
		System.gc();
		System.runFinalization();
		System.out.println("[DEBUG] Java done!");
		
		return T;
	}

	
	public static GradientReturn kmeans_gradients_tvm_float_ms(String table, String cols, int Kin, float batch_percent, int tvm_batch_size, float[] in_centroids) throws SQLException {
		// Prepare stats
		float[] runstats = new float[3];
		
		long tic_global = System.nanoTime();
			
		// Prepare data ResultSet
		db_object rs = prepare_db_data_moonshot(table,cols,batch_percent);
		Moonshot moonshot = rs.M;
		
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
		WorkerGrid  gridworker_2D = new WorkerGrid2D(N[0],K[0]);
		
		GridScheduler gridScheduler = new GridScheduler();
	
		System.out.println("[DEBUG] starting kernel init");
		
		KernelContext context = new KernelContext();    
		TaskGraph taskGraph = new TaskGraph("s0")
				.transferToDevice(DataTransferMode.FIRST_EXECUTION, centroids, ccentroid, d, Nc, K, centroids_L)       	
				.transferToDevice(DataTransferMode.EVERY_EXECUTION, v_batch, N)
	        	.task("t0", Kmeans::approx_euclidean_distance_tvm_kernel, context,  v_batch, centroids, N, d, Nc, K, centroids_L)
	        	.task("t1", Kmeans::search_min_distance_tvm_kernel, context, d, N, ccentroid, K)
	        	.transferToHost(DataTransferMode.EVERY_EXECUTION, ccentroid);
		System.out.println("[DEBUG] graph ready");
		
		ImmutableTaskGraph immutableTaskGraph = taskGraph.snapshot();
		
		System.out.println("[DEBUG] graph snapshot ready");
		

		gridScheduler.setWorkerGrid("s0.t0", gridworker_2D);
		gridScheduler.setWorkerGrid("s0.t1", gridworker);
		
		try(TornadoExecutionPlan executor_distance = new TornadoExecutionPlan(immutableTaskGraph)) {
			
			System.out.println("[DEBUG] Execution plan ready");
			
			
			// Init return vars
			int[] ncount = new int[K[0]];
			float[][] gradients = new float[K[0]][Nc[0]];
			
			System.out.println("[DEBUG] Before main loop");
			
			
			// Do batching
			boolean stop = false;
			try {
				while(!stop) {
					
					// Build batch
					long tic_io = System.nanoTime();
					N[0] = 0;
					for(int i = 0; i < tvm_batch_size; i++) {
						
						if(!moonshot.fetch_next()) {
							stop = true;
							break;
						}	
						double[] A = moonshot.getdoublearray(1);// <- To be changed after change in DB ! ( to Float )
						
						if(A == null) {	
							System.out.println("[DEBUG]: NULL ARRAY!");
							
							break;
						}
							
						for(int c = 0; c < Nc[0]; c++) {
							v_batch[i*Nc[0]+c] = (float) A[c];
						}
				
						N[0]++;
					}
					long toc_io = System.nanoTime();
					runstats[1] += toc_io-tic_io;
					
					// Calc
					if(N[0] > 0) {
						long tic_tvm = System.nanoTime();
						// Calc all distances
						gridworker.setGlobalWork(N[0], 1, 1);
						
			    	    executor_distance.withGridScheduler(gridScheduler).execute();
			    	    
			    	    runstats[2] += System.nanoTime() - tic_tvm;
			    	    
			    	    // Calc counts
						for(int i = 0; i < N[0]; i++) {
							
							ncount[ ccentroid[i] ]++;
							
							// Add to gradient
							vec_add(gradients[ ccentroid[i] ], getRowFrom2Darray(v_batch,i,Nc[0]));	
						}
					}	
				}
				
			} catch(Throwable t) {	
				t.printStackTrace();
				throw new SQLException(t);
			}
			
		// Add centroid contributions
		for(int k = 0; k < K[0]; k++) {
			vec_muladd(gradients[k],-ncount[k],getRowFrom2Darray(in_centroids,k,Nc[0]));
		}
			
		runstats[0] = (System.nanoTime() - tic_global)/1e6f;
		runstats[1] /= 1e6f;
		runstats[2] /= 1e6f;
			
		GradientReturn T =  new GradientReturn();
		
		T.Test1 = gradients;
		T.Test2 = ncount;
		T.Test3 = runstats;
	
		try {
			moonshot.disconnect();
		} catch(Throwable t) {
			throw new SQLException(t);
		}
	
		// Force GC
		System.gc();
		System.runFinalization();
		
		return T;
		
		} catch(Exception e) {
			e.printStackTrace();
		}
				
		return null;
	}
	
	
	
	public static void kmeans_main(String[] test) throws SQLException {
		
		// Prepare stats
		float[] runstats = new float[3];
		
		long tic_global = System.nanoTime();
		
		// Vars for Tornado
		int[] Nc = new int[1];
		Nc[0] = 79;
		int Kin = 5;
		int tvm_batch_size = 10000;
		
		int[] N = new int[1];
		float[] centroids = new float[Kin*Nc[0]];
		
		int[] ccentroid = new int[tvm_batch_size];
		float[] v_batch = new float[tvm_batch_size*Nc[0]];
		float[] d = new float[tvm_batch_size*Kin];
		int[] K = new int[1];
		K[0] = Kin;
			
		// Centroids length
		float[] centroids_L = approx_eucld_centroid_length(float_1D_to_float_2D(centroids, K[0], Nc[0]), K[0], Nc[0]);
				
		
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
		for(int m =0; m < 100; m++) {
		System.out.println("~~~ "+m);
		
			// Build batch
			long tic_io = System.nanoTime();
			N[0] = 0;
			for(int i = 0; i < tvm_batch_size; i++) {
			
				// Set data
				double[] A = new double[Nc[0]];
					
				for(int c = 0; c < Nc[0]; c++) {
					v_batch[i*Nc[0]+c] = (float) A[c];
				}	
				N[0]++;
			}
			long toc_io = System.nanoTime();
			runstats[1] += toc_io-tic_io;
			
			// Calc
			if(N[0] > 0) {
				long tic_tvm = System.nanoTime();
				// Calc all distances
				gridworker.setGlobalWork(N[0], 1, 1);
				
	    	    executor_distance.withGridScheduler(gridScheduler).execute();
	    	    
	    	    runstats[2] += System.nanoTime() - tic_tvm;
	    	    
	    	    // Calc counts
				for(int i = 0; i < N[0]; i++) {
					
					ncount[ ccentroid[i] ]++;
					
					// Add to gradient
					vec_add(gradients[ ccentroid[i] ], getRowFrom2Darray(v_batch,i,Nc[0]));	
				}
			}	
		}
		
		executor_distance.freeDeviceMemory();
		
		// Force GC
		System.gc();
		System.runFinalization();	
	}
	
	private static db_object prepare_db_data_moonshot(String table, String cols, float batch_percent) throws SQLException {
		 
		// Init db connection
		Moonshot moonshot = new Moonshot();
		try {
			moonshot.connect();
			
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
					moonshot.execute("select ARRAY_LENGTH("+cols+",1) from "+table+" limit 1;");
					moonshot.fetch_next();
					Nc = moonshot.getint(1);
				} else {
					Nc = Integer.valueOf(parts[1]);
				}
				
				// Query for 1D array
				query = "select "+parts[0]+" from "+table+" TABLESAMPLE SYSTEM("+batch_percent+") where cardinality("+parts[0]+")!=0;"; 	
				//query = "select "+parts[0]+" from "+table+" LIMIT 1000000;"; 	
				
				array = true;
			}
			
			moonshot.execute(query);
			
			return new db_object(moonshot,Nc,array);
			
		} catch(Throwable t) {
			throw new SQLException(t);	
		}
	}
	
	
	
	private static db_object prepare_db_data(String table, String cols, float batch_percent) throws SQLException {
		 
		// Init db connection
		Connection conn = DriverManager.getConnection(m_url);
		//conn.setAutoCommit(false);
		
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
				stmt.setFetchSize(10000);
				ResultSet rs = stmt.executeQuery();	
				rs.next();
				Nc = rs.getInt(1);
			} else {
				Nc = Integer.valueOf(parts[1]);
			}
			
			// Query for 1D array
			query = "select "+parts[0]+" from "+table+" TABLESAMPLE SYSTEM("+batch_percent+") where cardinality("+parts[0]+")!=0;"; 	
			//query = "select "+parts[0]+" from "+table+" LIMIT 1000000;"; 	
			
			array = true;
		}
		
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.setFetchSize(10000);
	
		ResultSet rs = stmt.executeQuery();	
			
		return new db_object(rs,Nc,array);	
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
	 * @param centroid_sequence : Return the sequence of centroids
	 * @param return: Iterator over epoche centroids
	 * @throws SQLException
	 */
	public static Iterator kmeans_control_float(String table, String cols, int K, int I, float batch_percent, boolean use_tvm, int tvm_batch_size, boolean centroid_sequence) throws SQLException {
		
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
				double[] A = (double[]) rs.getObject(1); // <- To be changed after change in DB ! ( to Float )
				
				for(int c = 0; c < Nc; c++) {
					centroids[i][c] = (float) A[c];
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
			//func = "execute direct on ("+nodes+") $$select worker_test9"; // <- for moonshot
			
			func += "('"+table+"','"+cols+"',"+K+","+batch_percent+",";
		}
				
		float[][][] partialGradients = new float[Nn][K][Nc];
		int[][] Ccounts = new int[Nn][K];
			
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
				partialGradients[c] = (float[][]) tmp.getObject(1);
				
				// 2. Counts as 1D Int array
				Ccounts[c] = (int[]) tmp.getObject(2);
						
				// 3. Collect statistics
				float[] stats_tmp = (float[]) tmp.getObject(3);
				
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
	
			if(centroid_sequence) {
				// Add to return
				receiver.add( array2DdeepCopy(centroids) );
			}
		}
		
		if(!centroid_sequence) {
			// Add to return
			receiver.add( array2DdeepCopy(centroids) );
		}
		
		// Force GC
		System.gc();
		System.runFinalization();
		
		return receiver.iterator();		
	}
	
	
public static Iterator kmeans_control_float_ms(String table, String cols, int K, int I, float batch_percent, boolean use_tvm, int tvm_batch_size, boolean centroid_sequence) throws SQLException {
		
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
				double[] A = (double[]) rs.getObject(1); // <- To be changed after change in DB ! ( to Float )
				
				for(int c = 0; c < Nc; c++) {
					centroids[i][c] = (float) A[c];
				}
			}

		}
			
		// Set function
		String func;
		if(use_tvm) {
			func = "execute direct on ("+nodes+") $$select kmeans_gradients_tvm_float_ms";
			func += "('"+table+"','"+cols+"',"+K+","+batch_percent+","+tvm_batch_size+",";
		}
		else {
			func = "execute direct on ("+nodes+") $$select kmeans_gradients_cpu_float_ms";
			
			func += "('"+table+"','"+cols+"',"+K+","+batch_percent+",";
		}
				
		float[][][] partialGradients = new float[Nn][K][Nc];
		int[][] Ccounts = new int[Nn][K];
			
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
				partialGradients[c] = (float[][]) tmp.getObject(1);
				
				// 2. Counts as 1D Int array
				Ccounts[c] = (int[]) tmp.getObject(2);
						
				// 3. Collect statistics
				float[] stats_tmp = (float[]) tmp.getObject(3);
				
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
	
			if(centroid_sequence) {
				// Add to return
				receiver.add( array2DdeepCopy(centroids) );
			}
		}
		
		if(!centroid_sequence) {
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
		float[] runstats = new float[3];
		long tic_global = System.nanoTime();
		
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
			
		float[] v = new float[Nc];
	
		// Main loop over data
		boolean stop = false;
		while ( !stop ) {
			
			long tic_io = System.nanoTime();
			
			// Get next element
			if(!rs.R.next()) {
				stop = true;
				break;
			}
			
			// Build vec		
			if(rs.array) {
				// 1D array
				double[] A = (double[]) rs.R.getObject(1); // <- To be changed after change in DB ! ( to Float )
				//PgArray A = (PgArray) rs.R.getObject(1); // <- To be changed after change in DB ! ( to Float )
				//Double[] B = (Double[]) A.getArray();
				
				for(int c = 0; c < Nc; c++) {
					//v[c] = A[c].floatValue();
					v[c] = (float) A[c];
					//v[c] = B[c].floatValue();
					
				}
			} else {
				// True cols
				for(int c = 1; c < Nc+1; c++) {
					v[c-1] = rs.R.getFloat(c);
				}
			}
			
			runstats[1] += System.nanoTime() - tic_io;
		
			// Find min distance centroid
			long tic_cpu = System.nanoTime();
			int minc = 0;
			float d = approx_euclidean_distance(v,centroids[0],centroids_L[0]);
			for(int k = 1; k < K; k++) {
				
				float dist = approx_euclidean_distance(v,centroids[k],centroids_L[k]);
				
				if(dist < d) {
					minc = k;
					d = dist;
				}
			}
			
			runstats[2] += System.nanoTime() - tic_cpu;
			
			ncount[minc]++;
			
			// Add to gradient
			vec_add(gradients[minc],v);
		}
		
		// Add centroid contributions
		for(int k = 0; k < K; k++) {
			vec_muladd(gradients[k],-ncount[k],centroids[k]);
		}
		
		runstats[0] = (System.nanoTime() - tic_global)/1e6f;
		runstats[1] /= 1e6f;
		runstats[2] /= 1e6f;
		
		receiver.updateObject(1, gradients);				
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
		float[] runstats = new float[3];
		
		long tic_global = System.nanoTime();
			
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
		WorkerGrid  gridworker_2D = new WorkerGrid2D(N[0],K[0]);
		
		GridScheduler gridScheduler = new GridScheduler();
		
		KernelContext context = new KernelContext();    
		TaskGraph taskGraph = new TaskGraph("s0")
				.transferToDevice(DataTransferMode.FIRST_EXECUTION, centroids, ccentroid, d, Nc, K, centroids_L)       	
				.transferToDevice(DataTransferMode.EVERY_EXECUTION, v_batch, N)
	        	.task("t0", Kmeans::approx_euclidean_distance_tvm_kernel, context,  v_batch, centroids, N, d, Nc, K, centroids_L)
	        	.task("t1", Kmeans::search_min_distance_tvm_kernel, context, d, N, ccentroid, K)
	        	.transferToHost(DataTransferMode.EVERY_EXECUTION, ccentroid);
		
		ImmutableTaskGraph immutableTaskGraph = taskGraph.snapshot();
		
		gridScheduler.setWorkerGrid("s0.t0", gridworker_2D);
		gridScheduler.setWorkerGrid("s0.t1", gridworker);
		
		// Init return vars
		int[] ncount = new int[K[0]];
		float[][] gradients = new float[K[0]][Nc[0]];
					
		try(TornadoExecutionPlan executor_distance = new TornadoExecutionPlan(immutableTaskGraph)) {
			
			
			// Do batching
			boolean stop = false;
			while(!stop) {
				
				// Build batch
				long tic_io = System.nanoTime();
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
						double[] A = (double[]) rs.R.getObject(1); // <- To be changed after change in DB ! ( to Float )
						
						for(int c = 0; c < Nc[0]; c++) {
							v_batch[i*Nc[0]+c] = (float) A[c];
						}
						
					} else {
						for(int c = 1; c < Nc[0]+1; c++) {
							v_batch[i*Nc[0]+c-1]= rs.R.getFloat(c);
						}
					}
					N[0]++;
				}
				long toc_io = System.nanoTime();
				runstats[1] += toc_io-tic_io;
				
				// Calc
				if(N[0] > 0) {
					long tic_tvm = System.nanoTime();
					// Calc all distances
					gridworker.setGlobalWork(N[0], 1, 1);
					
		    	    executor_distance.withGridScheduler(gridScheduler).execute();
		    	    
		    	    runstats[2] += System.nanoTime() - tic_tvm;
		    	    
		    	    // Calc counts
					for(int i = 0; i < N[0]; i++) {
						
						ncount[ ccentroid[i] ]++;
						
						// Add to gradient
						vec_add(gradients[ ccentroid[i] ], getRowFrom2Darray(v_batch,i,Nc[0]));	
					}
				}	
			}
		
				
		} catch(Exception e) {
			e.printStackTrace();
			throw new SQLException(e);
		}
		
		// Add centroid contributions
		for(int k = 0; k < K[0]; k++) {
			vec_muladd(gradients[k],-ncount[k],getRowFrom2Darray(in_centroids,k,Nc[0]));
		}
			
		runstats[0] = (System.nanoTime() - tic_global)/1e6f;
		runstats[1] /= 1e6f;
		runstats[2] /= 1e6f;
		
		receiver.updateObject(1, gradients);
		receiver.updateObject(2, ncount);		
		receiver.updateObject(3, runstats);
	
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
		int j = context.globalIdy;
		//for(int j = 0; j < Ncentr[0]; j++)  {
			float tmp = 0;
			for(int k = 0; k < Nc[0]; k++) {
				tmp += v1[i*Nc[0]+k] * v2[j*Nc[0]+k];
			}	
			d[i*Ncentr[0]+j] = nc[j]-tmp;
		//}
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
	
	/**
	 * Converts 1D matrix to postgres 1D array string
	 * @param A : 1D matrix 
	 * @return
	 */
	private static String getPGarrayFrom1Darray(float[] A) {
		String s;
		if(A.length != 0) {
				
			s = "{";
			
			for(int i = 0; i < A.length; i++) {
					s+=A[i]+",";
			}
			
			s = s.substring(0, s.length()-1);
			s+="}";
		} else {
			s = "Null";
		}
		return s;
	}
	
}
