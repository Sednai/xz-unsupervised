 

package ai.sedn.moonshot;

import java.sql.SQLException;;

public class Tests {

	/*
	 * Integer
	 */
	
	public static int test_int1(int in) throws SQLException  {
        return in;
    }
	
	public static int test_int2(int in1, int in2) throws SQLException {
        return in1+in2;
    }

	public static int test_int3(int[] in) throws SQLException {
        int ret = 0;
		for(int i = 0; i < in.length; i++) {
			ret += in[i];
        }
		return ret;
    }
	
	public static int test_int4(int[][] in) throws SQLException {
		int ret = 0;
		for(int i = 0; i < in.length; i++) {
			for(int j = 0; j < in[i].length; j++) {
				ret += in[i][j];
			}
        }
		return ret;
	}
	
	/*
	 * Float
	 */
	
	public static float test_float1(float in) throws SQLException  {
        return in;
    }
	
	public static float test_float2(float in1, float in2) throws SQLException {
        return in1+in2;
    }

	public static float test_float3(float[] in) throws SQLException {
        float ret = 0;
		for(int i = 0; i < in.length; i++) {
			ret += in[i];
        }
		return ret;
    }
	
	public static float test_float4(float[][] in) throws SQLException {
		float ret = 0;
		for(int i = 0; i < in.length; i++) {
			for(int j = 0; j < in[i].length; j++) {
				ret += in[i][j];
			}
        }
		return ret;
	}
	
	
	/*
	 * Double
	 */
	
	public static double test_double1(double in) throws SQLException  {
        return in;
    }
	
	public static double test_double2(double in1, double in2) throws SQLException {
        return in1+in2;
    }

	public static double test_double3(double[] in) throws SQLException {
        float ret = 0;
		for(int i = 0; i < in.length; i++) {
			ret += in[i];
        }
		return ret;
    }
	
	public static double test_double4(double[][] in) throws SQLException {
		float ret = 0;
		for(int i = 0; i < in.length; i++) {
			for(int j = 0; j < in[i].length; j++) {
				ret += in[i][j];
			}
        }
		return ret;
	}
	
	
	
	
}

