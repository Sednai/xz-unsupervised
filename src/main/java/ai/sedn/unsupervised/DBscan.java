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
import java.util.Set;
import java.util.HashSet;

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
		System.out.println("[DEBUG](batch) ***start: "+lastID+" C: "+C+"(eps: "+eps+", minPts: "+minPts+")");
		// Get sources
		String cmd = "select "+idcolname+",class"+" from "+returntabname+" where "+idcolname+" > "+lastID+" order by "+idcolname+" asc limit "+batchsize;
		PreparedStatement stmt = conn.prepareStatement(cmd);
		ResultSet rs = stmt.executeQuery();	
		
		// Init batch
		Map<Long,dbscan_cache> M = new HashMap<Long,dbscan_cache>();
		String bquery = "select "+idcolname+",vector_to_float8("+colname+",0,false) from "+tabname+" where "+idcolname+" in (";
		int fc = 0;
		int bqc = 0;
		while(rs.next()) {
			long sid = rs.getLong(1);
			int cid = rs.getInt(2); // WARNING: NULL -> 0
			if(cid == 0) { // <- UNDEFINED POINT
				// Only consider points undefined yet	
				dbscan_cache D = new dbscan_cache();
				D.sid = sid;
				D.vector = null;
				D.label = 0; // UNDEFINED
				M.put(sid, D);
				bquery += sid+",";
				bqc++;
			} else {
				fc++;
			}
			lastID = sid;
		}
		
		if(bqc > 0) {
			bquery = bquery.substring(0, bquery.length()-1);
		
			bquery+=");";
			
			// Get bquery data
			stmt = conn.prepareStatement(bquery);
			rs = stmt.executeQuery();
			
			// Store vectors
			int fc2 = 0;
			//ArrayList<Long> bpoints = new ArrayList<Long>();
			Set<Long>  bpoints = new HashSet<Long>();
			while(rs.next()) {
				
				long sid = rs.getLong(1);
				double[] v = (double[]) rs.getObject(2); // <- To be changed after change in DB ! ( to Float )
				
				if (v.length > 0) {
					M.get(sid).vector = v;	
					bpoints.add(sid);
				} else {
					M.remove(sid);
					fc2++;
				}
			}
			
			// Loop over points and query for neighbors
				
			System.out.println("[DEBUG] Loop over batch points ("+bqc+","+bpoints.size()+","+M.size()+","+fc+","+fc2+")");
			int pmax=0;
			for (Long key : bpoints) {
				
				// Skip if already labeled 
				if(M.get(key).label > 0) {
					continue;
				}
				
				// Query for neighbors
				List<dbscan_cache> NN = loadNN(conn, tabname, idcolname, colname, eps, key, M.get(key).vector, M); 
				
				//System.out.println("NN: "+NN.size());
					
				if(NN.size() < minPts) {
					M.get(key).label = 1; // NOISE
					continue;
				}	
				
				// Load labels
				loadClassLabels(conn,returntabname,idcolname,M,NN);
				
				List<Long> NNsid = new ArrayList<Long>();
				Set<Long>  NNsid_set = new HashSet<Long>();
				
				// Set neighbors and add to cache
				for(dbscan_cache P : NN) {
					NNsid.add(P.sid);
					NNsid_set.add(P.sid);
					M.put(P.sid, P);
				}
				
				C++; // Set new class label
				M.get(key).label = C; 
				
				// Expand class
				for(int p = 0; p < NNsid.size(); p++) {
					
					dbscan_cache P = M.get( NNsid.get(p) );
					
					if(P.label == 1) {
						P.label = C;
					}
					
					if(P.label != 0) {
						P.vector = null; // Do not need anymore vec info if label set
						continue;
					}
					
					P.label = C;
					
					// Query for neighbors
					List<dbscan_cache> NN2 = loadNN(conn, tabname, idcolname, colname, eps, P.sid, P.vector, M); 
					
					// Query for class info
					if( NN2.size() >= minPts) {
						loadClassLabels(conn,returntabname,idcolname,M,NN2);
						
						//System.out.println("p: "+p+"  eNN: "+NN2.size()+" before: "+M.get(key).NN.size());
										
						// Add points
						for(dbscan_cache Px : NN2) {
							if(Px.sid != key && Px.label < 2 && !NNsid_set.contains(Px.sid)) { // Do not overwrite existing key and check if already in List (hashmap problematic for dynamic addition of elements while looping)
								NNsid.add(Px.sid);
								NNsid_set.add(Px.sid);
								M.put(Px.sid, Px);
							}
						}	
						
						//System.out.println("  eNN: after:"+M.get(key).NN.size());
						
					}
					
					P.vector = null; // Do not need anymore vec info if label set and NN queried
				}
				
				if(NNsid.size() > pmax) {
					pmax = NNsid.size();
				}
					
				
				// Cleanup after class	
				serializeCacheList(conn, returntabname, idcolname, NNsid, M);
				
				for(long sid : NNsid) {
					if(!bpoints.contains(sid)) {
						M.remove(sid);
					}
				}
			}	
			System.out.println("[DEBUG](Serialize) batch, C: "+C+" pmax: "+pmax);
				
		}
		
		// Serialize remaining cache to return table
		serializeCache(conn, returntabname, idcolname, M);

		receiver.updateObject(1, lastID);
		receiver.updateObject(2, C);
		
		conn.close();
		
		System.out.println("[DEBUG](batch) ***end: "+lastID+" C: "+C);
		
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

	private static int serializeCacheList(Connection conn, String returntabname, String idcolname, List<Long> L,Map<Long, dbscan_cache> M) throws SQLException {
		Statement stmt = conn.createStatement();
		int c = 0;
		for (long sid : L)  {
			dbscan_cache C = M.get(sid);
			if(C.label > 0) {
				// Store
				String cmd = "update "+returntabname+" set class="+C.label+" where "+idcolname+"="+C.sid;
				stmt.execute(cmd);
				c++;
			}	
		}
		System.out.println("[DEBUG](serializeCache) -> "+c+" of "+L.size());
		
		return c;
	}
	
	
	private static int serializeCache(Connection conn, String returntabname, String idcolname, Map<Long, dbscan_cache> M) throws SQLException {
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
		System.out.println("[DEBUG](serializeCache) -> "+c+" of "+M.size());
		
		return c;
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
	private static List<dbscan_cache> loadNN(Connection conn, String tabname, String idcolname, String colname, float eps, long key, double[] v, Map<Long,dbscan_cache> M) throws SQLException {
		String vs  = array_to_vec(v);
		
		
		// For speed: 1. get only sourceids
		String cmd = "select "+idcolname+" from "+tabname+" where "+colname+" <-> '"+vs+"' < "+eps+" ORDER BY "+colname+" <-> '"+vs+"'"; // Note: With < 2 not original dbscan as minPts counted differently
		
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(cmd);		
		
		List<dbscan_cache> res = new ArrayList<dbscan_cache>();
		cmd = "select "+idcolname+",vector_to_float8("+colname+",0,false)"+" from "+tabname+" where "+idcolname+" in (";
	
		// Get neighbors
		int c = 0;
		while(rs.next()) {
			long nsid = rs.getLong(1);
			if(M.containsKey(nsid)) {
				if(nsid != key) { // Do not keep same
					res.add(M.get(nsid));
				}
			} else {
				if(nsid != key) { // Do not keep same
					cmd += nsid+",";
					c++;
				}		
			}
		}
		
		// 2. Get missing vectors
		if(c > 0) {
			cmd = cmd.substring(0, cmd.length()-1);
			cmd += ");";
		
			rs = stmt.executeQuery(cmd);		
			while(rs.next()) {

				dbscan_cache D = new dbscan_cache();
				D.sid = rs.getLong(1);
				D.vector = (double[]) rs.getObject(2);
				res.add(D);	
			}
		}
			
		//System.out.println("NN query: "+key+" => "+res.size());
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
	private static void loadClassLabels(Connection conn, String returntabname, String idcolname, Map<Long,dbscan_cache> M, List<dbscan_cache> A) throws SQLException {
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
				long nsid = rs.getLong(1);
				int cl = rs.getInt(2);
				for(dbscan_cache P : A) {
					if(P.sid == nsid) {
						P.label = cl;		
						break;
					}
				}
			}
		
		}
		
		//System.out.println("[DEBUG](loadLabels) c: "+c+" -> "+fc);
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

