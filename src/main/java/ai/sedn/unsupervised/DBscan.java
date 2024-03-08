 /*-----------------------------------------------------------------------------
 *
 *                      Gaia CU7 variability
 *
 *         Copyright (C) 2005-2020 Gaia Data Processing and Analysis Consortium
 *
 *
 * CU7 variability software is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * CU7 variability software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this CU7 variability software; if not, write to the
 * Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301  USA
 *
 *-----------------------------------------------------------------------------
 */


package ai.sedn.unsupervised;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.LinkedList;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

/**
*
* TODO Short description
* @author krefl
* @version $Id$
*
*/

public class DBscan {
	
	private static String m_url = "jdbc:default:connection";

	public static boolean dbscan_batch(String tabname, String colname, String idcolname, String returntabname, int batchsize, float eps, int minPts, long lastID, int C, ResultSet receiver) throws SQLException {
		Connection conn   = DriverManager.getConnection(m_url);
		System.out.println("[DEBUG](batch) start: "+lastID+" C: "+C);
		// Get sources
		String cmd = "select "+idcolname+",class"+" from "+returntabname+" where "+idcolname+" >= "+lastID+" limit "+batchsize;
		PreparedStatement stmt = conn.prepareStatement(cmd);
		ResultSet rs = stmt.executeQuery();	
		
		// Init batch
		Map<Long,dbscan_cache> M = new HashMap<Long,dbscan_cache>();
		String bquery = "select "+idcolname+",vector_to_float8("+colname+",0,false) from "+tabname+" where "+idcolname+" in (";
		while(rs.next()) {
			Long sid = rs.getLong(1);
			Integer cid = rs.getInt(2); // WARNING: NULL -> 0
			if(cid == 0) { // <- UNDEFINED POINT
				// Only consider points undefined yet	
				dbscan_cache D = new dbscan_cache();
				D.sid = sid;
				D.NN = new ArrayList<Long>();
				D.vector = null;
				D.label = 0; // UNDEFINED
				M.put(sid, D);
				bquery += sid+",";
			}		
			lastID = sid;
		}
		bquery = bquery.substring(0, bquery.length()-1);
		bquery+=");";
		
		// Get bquery data
		stmt = conn.prepareStatement(bquery);
		rs = stmt.executeQuery();
		
		// Store vectors
		ArrayList<Long> bpoints = new ArrayList<Long>();
		while(rs.next()) {
			
			Long sid = rs.getLong(1);
			//String v = rs.getString(2);
			double[] v = (double[]) rs.getObject(2); // <- To be changed after change in DB ! ( to Float )
			
			if (v.length > 0) {
				M.get(sid).vector = v;	
				bpoints.add(sid);
			} else {
				M.remove(sid);
			}
		}
		
		// Loop over points and query for neighbors
			
		System.out.println("[DEBUG] Loop over batch points");
		
		for (Long key : bpoints) {
			
			// Only consider ids with data
			double[] v = M.get(key).vector;
			if(v.length == 0) { // IGNORE MISSING DATA ROWS
				continue;
			}
		
			// Query for neighbors
			List<dbscan_cache> NN = loadNN(conn, tabname, idcolname, colname, eps, key, v, M); 
			
			//System.out.println("NN: "+NN.size());
				
			if(NN.size() < minPts) {
				M.get(key).label = 1; // NOISE
				continue;
			}	
			
			// Load labels
			loadClassLabels(conn,returntabname,idcolname,key,M,NN);
			
			// Set neighbors and add to cache
			for(dbscan_cache P : NN) {
				M.get(key).NN.add(P.sid);
				M.put(P.sid, P);
			}
			
			C += 1; // Set new class label
			
			// Expand class
			for(int p = 0; p < M.get(key).NN.size(); p++) {
				dbscan_cache P = M.get( M.get(key).NN.get(p) );
				if(P.label == 1) {
					P.label = C;
				}
				
				if(P.label != 0) {
					continue;
				}
				
				P.label = C;
				
				// Query for neighbors
				NN = loadNN(conn, tabname, idcolname, colname, eps, Long.valueOf(P.sid), P.vector, M); 
				
				// Query for class info
				if( NN.size() > minPts) {
					loadClassLabels(conn,returntabname,idcolname,key,M,NN);
							
					//System.out.println("  eNN: "+NN.size());
					
					// Add points
					for(dbscan_cache Px : NN) {
						M.get(key).NN.add(Px.sid);
						M.put(Px.sid, Px);
					}	
				}
			}
		}			
		
		System.out.println("[DEBUG](Serialize) batch, C: "+C);
		
		// Serialize cache to return table
		serializeCache(conn, returntabname, idcolname, M);

		receiver.updateObject(1, lastID);
		receiver.updateObject(2, C);
		
		conn.close();
		
		System.out.println("[DEBUG](batch) end: "+lastID+" C: "+C);
		
		return true;
	}		
	
	@Deprecated // Replaced with plpgsql function
	public static long dbscan(String tabname, String colname, String idcolname, String returntabname, int batchsize, float eps, int minPts) throws SQLException {
		
		Connection conn   = DriverManager.getConnection(m_url);
		
		// Get size of table
		PreparedStatement stmt = conn.prepareStatement("select count(*) from "+tabname);
		ResultSet rs = stmt.executeQuery();		
		rs.next();
		long Ntotal = rs.getLong(1);
		
		// 0. Prepare return table
		// ... Do later after prototpying complete 
		
			
		conn.close();
		
		// Loop over batches and update
		long Nbatch = Math.ceilDiv(Ntotal,batchsize);
		long lastID = -1;
		int C = 0;
		System.out.println("[DEBUG](dbscan) Nbatch: "+Nbatch);
		
		for(long i = 0; i < Nbatch; i++) {
			//dbscan_batch(tabname,colname,idcolname,returntabname,batchsize,eps,minPts,lastID,C);	
			//C = R.C;
			//lastID = R.lastID;
		}
			
		return Nbatch;
	}

	
	private static void serializeCache(Connection conn, String returntabname, String idcolname, Map<Long, dbscan_cache> M) throws SQLException {
		Statement stmt = conn.createStatement();
		int c = 0;
		for (Long key : M.keySet()) {
			dbscan_cache C = M.get(key);
			
			if(C.label > 0) {
				// Store
				String cmd = "update "+returntabname+" set class="+C.label+" where "+idcolname+"="+C.sid;
				stmt.execute(cmd);
				c++;
			}	
		}
		System.out.println("[DEBUG](serializeCache) -> "+c);
	}
	
	/**
	 * Query for NN
	 * 
	 * @param conn
	 * @param tabname
	 * @param idcolname
	 * @param colname
	 * @param eps
	 * @param key
	 * @param v
	 * @param M
	 * @throws SQLException
	 */
	private static List<dbscan_cache> loadNN(Connection conn, String tabname, String idcolname, String colname, float eps, Long key, double[] v, Map<Long,dbscan_cache> M) throws SQLException {
		String vs  = array_to_vec(v);
		String cmd = "select "+idcolname+",vector_to_float8("+colname+",0,false)"+" from "+tabname+" where "+colname+" <-> '"+vs+"' < "+eps+" ORDER BY "+colname+" <-> '"+vs+"'";
		PreparedStatement stmt = conn.prepareStatement(cmd);
		ResultSet rs = stmt.executeQuery();		
		
		List<dbscan_cache> res = new ArrayList<dbscan_cache>();
		
		// Get neighbors
		while(rs.next()) {
			Long nsid = rs.getLong(1);
			double[] nv = (double[]) rs.getObject(2);
			if(nsid.compareTo(key) != 0) { // Do not keep same
				dbscan_cache D = new dbscan_cache();
				D.sid = nsid;
				D.vector = nv;
				D.NN = new ArrayList<Long>();
				res.add(D);
				
				//System.out.println("NN: "+nsid+" ("+key+")");
			}
		}
		
		return res;
	}
	
	/**
	 * Load class labels from return table if not in cache
	 * 
	 * @param conn
	 * @param returntabname
	 * @param idcolname
	 * @param key
	 * @param M
	 * @throws SQLException
	 */
	private static void loadClassLabels(Connection conn, String returntabname, String idcolname, Long key, Map<Long,dbscan_cache> M, List<dbscan_cache> A) throws SQLException {
		// ToDo: Explicitly supply list
		
		// Load labels
		String cmd;
		cmd = "select sourceid,class from "+returntabname+" where "+idcolname+" in (";
		int c = 0;
		for(dbscan_cache P : A) {
			if(M.containsKey(P.sid)) {
				P.label = M.get(P.sid).label;
			} else {
				cmd += P.sid+",";
				c++;
			}
		}
		
		if(c > 0) {
			cmd = cmd.substring(0, cmd.length()-1);
			cmd+=");";
			
			// Execute
			PreparedStatement stmt = conn.prepareStatement(cmd);
			ResultSet rs = stmt.executeQuery();		
			
			// Set labels
			while(rs.next()) {
				// ToDo: Change neighbors list to hashmap
				long nsid = (long) rs.getLong(1);
				int cl = rs.getInt(2);
				for(dbscan_cache P : A) {
					if(P.sid == nsid) {
						P.label = cl;
						break;
					}
				}
			}
		
		}
	}
	
	/*
	 * Convert double array to string representation to query db
	 * 
	 */
	private static String array_to_vec(double[] v) {
		String ret = "[";
		
		for(int i = 0; i < v.length; i++) {
			ret += v[i]+",";
		}
		
		ret = ret.substring(0,ret.length()-1);
		ret += "]";
		
		return ret;
	}
	
}
