package ai.sedn.unsupervised;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import uk.ac.manchester.tornado.api.GridScheduler;
import uk.ac.manchester.tornado.api.ImmutableTaskGraph;
import uk.ac.manchester.tornado.api.TaskGraph;
import uk.ac.manchester.tornado.api.TornadoExecutionPlan;
import uk.ac.manchester.tornado.api.WorkerGrid;
import uk.ac.manchester.tornado.api.WorkerGrid1D;
import uk.ac.manchester.tornado.api.WorkerGrid2D;
import uk.ac.manchester.tornado.api.annotations.Parallel;
import uk.ac.manchester.tornado.api.enums.DataTransferMode;
import uk.ac.manchester.tornado.api.collections.types.Matrix2DFloat;
import uk.ac.manchester.tornado.api.KernelContext;
import uk.ac.manchester.tornado.api.collections.math.TornadoMath;

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
			query = "select "+cols+" from "+table+" TABLESAMPLE SYSTEM("+batch_percent+") where cardinality("+cols+")!=0;"; 	
			array = true;
		}
		
		PreparedStatement stmt = conn.prepareStatement(query);
		ResultSet rs = stmt.executeQuery();	
		
		System.out.println(Nc+" "+array);
		
		return new db_object(rs,Nc,array);	
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
				
		receiver.updateObject(1, cast2DFloatArray(gradients));		
		receiver.updateObject(2, ncount);		
		
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
	
}
