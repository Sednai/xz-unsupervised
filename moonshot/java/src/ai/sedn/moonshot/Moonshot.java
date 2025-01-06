
package ai.sedn.moonshot;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT;
import static java.lang.foreign.ValueLayout.JAVA_BOOLEAN;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.GroupLayout;
import java.lang.invoke.VarHandle;
import java.lang.foreign.SequenceLayout;
import java.lang.foreign.SegmentAllocator;

import java.sql.*;


public class Moonshot {
	private Arena arena;
	
	private MethodHandle lib_connect;
	private MethodHandle lib_disconnect;
	private MethodHandle lib_execute;
	private MethodHandle lib_fetch_next_double_array;
	private MethodHandle lib_fetch_next;
	private MethodHandle lib_getdouble;
	private MethodHandle lib_getfloat;
	private MethodHandle lib_getint;
	private MethodHandle lib_getlong;
	private MethodHandle lib_getdoublearray;
	private MethodHandle lib_getvector;
	
	private GroupLayout arrayLayout = MemoryLayout.structLayout(
			ADDRESS.withName("arr"),
			JAVA_INT.withName("size")
	);
	
	private VarHandle resultSize = arrayLayout.varHandle(MemoryLayout.PathElement.groupElement("size"));
	private VarHandle resultArr = arrayLayout.varHandle(MemoryLayout.PathElement.groupElement("arr"));
	
	public Moonshot() {
		Linker linker = Linker.nativeLinker();		
		
		arena = Arena.ofConfined();
		
		SymbolLookup lib = SymbolLookup.libraryLookup("/data/moonshot/moonshot.so", arena);
	
		MemorySegment lib_connect_addr = lib.find("connect_SPI").get();
		FunctionDescriptor lib_connect_sig = FunctionDescriptor.of(JAVA_INT);
		
		lib_connect = linker.downcallHandle(lib_connect_addr, lib_connect_sig); 
			
		MemorySegment lib_disconnect_addr = lib.find("disconnect_SPI").get();
		FunctionDescriptor lib_disconnect_sig = FunctionDescriptor.ofVoid();
		lib_disconnect = linker.downcallHandle(lib_disconnect_addr, lib_disconnect_sig); 
		
		MemorySegment lib_execute_addr = lib.find("execute").get();
		FunctionDescriptor lib_execute_sig = FunctionDescriptor.of(JAVA_INT,ADDRESS,JAVA_BOOLEAN);
		lib_execute = linker.downcallHandle(lib_execute_addr, lib_execute_sig); 
		
		MemorySegment lib_fetch_next_double_array_addr = lib.find("fetch_next_double_array").get();
		FunctionDescriptor lib_fetch_next_double_array_sig = FunctionDescriptor.of(ADDRESS.withTargetLayout(arrayLayout),JAVA_INT);
		lib_fetch_next_double_array = linker.downcallHandle(lib_fetch_next_double_array_addr, lib_fetch_next_double_array_sig); 
		
		MemorySegment lib_fetch_next_addr = lib.find("fetch_next").get();
		FunctionDescriptor lib_fetch_next_sig = FunctionDescriptor.of(JAVA_BOOLEAN);
		lib_fetch_next = linker.downcallHandle(lib_fetch_next_addr, lib_fetch_next_sig); 
	
		MemorySegment lib_getdouble_addr = lib.find("getdouble").get();
		FunctionDescriptor lib_getdouble_sig = FunctionDescriptor.of(JAVA_DOUBLE,JAVA_INT);
		lib_getdouble = linker.downcallHandle(lib_getdouble_addr, lib_getdouble_sig); 
	
		MemorySegment lib_getfloat_addr = lib.find("getfloat").get();
		FunctionDescriptor lib_getfloat_sig = FunctionDescriptor.of(JAVA_FLOAT,JAVA_INT);
		lib_getfloat = linker.downcallHandle(lib_getfloat_addr, lib_getfloat_sig); 
	
		MemorySegment lib_getint_addr = lib.find("getint").get();
		FunctionDescriptor lib_getint_sig = FunctionDescriptor.of(JAVA_INT,JAVA_INT);
		lib_getint = linker.downcallHandle(lib_getint_addr, lib_getint_sig); 
	
		MemorySegment lib_getlong_addr = lib.find("getlong").get();
		FunctionDescriptor lib_getlong_sig = FunctionDescriptor.of(JAVA_LONG,JAVA_INT);
		lib_getlong = linker.downcallHandle(lib_getlong_addr, lib_getlong_sig); 
			
		MemorySegment lib_getdoublearray_addr = lib.find("getdoublearray").get();
		FunctionDescriptor lib_getdoublearray_sig = FunctionDescriptor.of(ADDRESS.withTargetLayout(arrayLayout),JAVA_INT);
		lib_getdoublearray = linker.downcallHandle(lib_getdoublearray_addr, lib_getdoublearray_sig); 
	
		MemorySegment lib_getvector_addr = lib.find("getvector").get();
		FunctionDescriptor lib_getvector_sig = FunctionDescriptor.of(ADDRESS.withTargetLayout(arrayLayout),JAVA_INT);
		lib_getvector = linker.downcallHandle(lib_getvector_addr, lib_getvector_sig); 	
	}
	
	public void connect() throws Throwable {
		int ret = (int) lib_connect.invokeExact();
		
		if(ret != 0) {
			throw new Exception("Connection to db failed!"); 
		}
	}
	
	public void disconnect() throws Throwable {
		lib_disconnect.invokeExact();
	}
	
	public void execute(String query) throws Throwable {
		
		var cString = arena.allocateUtf8String(query);
		
		int ret = (int) lib_execute.invokeExact(cString,true);
		
		if(ret != 0) {
			throw new SQLException("Execution failed! ("+query+")"); 
		}
	}
	
public void execute_nc(String query) throws Throwable {
		
		var cString = arena.allocateUtf8String(query);
		
		int ret = (int) lib_execute.invokeExact(cString,false);
		
		if(ret != 0) {
			throw new SQLException("Execution failed! ("+query+")"); 
		}
	}
	
	
	public double[] fetch_next_double_array(int column) throws Throwable{
		
		MemorySegment next = (MemorySegment) lib_fetch_next_double_array.invokeExact(column);  
	
		int size = (int) resultSize.get(next);
		
		if(size > 0) {
			MemorySegment ARR = (MemorySegment) resultArr.get(next);
			
			SequenceLayout L = MemoryLayout.sequenceLayout(size,JAVA_DOUBLE);
			ARR = ARR.reinterpret(L.byteSize());
			
			double[] ret = ARR.toArray(JAVA_DOUBLE);
			
			return ret;
		}
		
		return null;
	}
	
	public MemorySegment fetch_next_double_array_ms(int column) throws Throwable {
		
		MemorySegment next = (MemorySegment) lib_fetch_next_double_array.invokeExact(column);  
	
		int size = (int) resultSize.get(next);
		
		if(size > 0) {
			MemorySegment ARR = (MemorySegment) resultArr.get(next);
			
			SequenceLayout L = MemoryLayout.sequenceLayout(size,JAVA_DOUBLE);
			ARR = ARR.reinterpret(L.byteSize()).asReadOnly();
			
			return ARR;
		}
		
		
		return null;
	}
	
	
	public boolean fetch_next() throws Throwable {
		return (boolean) lib_fetch_next.invokeExact();		
	}

	public double getdouble(int column) throws Throwable {
		return (double) lib_getdouble.invokeExact(column);		
	}

	public float getfloat(int column) throws Throwable {
		return (float) lib_getfloat.invokeExact(column);		
	}
	
	public int getint(int column) throws Throwable {
		return (int) lib_getint.invokeExact(column);		
	}
	
	public long getlong(int column) throws Throwable {
		return (long) lib_getlong.invokeExact(column);		
	}
	
	
	public double[] getdoublearray(int column) throws Throwable{
		
		MemorySegment next = (MemorySegment) lib_getdoublearray.invokeExact(column);  
	
		int size = (int) resultSize.get(next);
		
		if(size > 0) {
			MemorySegment ARR = (MemorySegment) resultArr.get(next);
			
			SequenceLayout L = MemoryLayout.sequenceLayout(size,JAVA_DOUBLE);
			ARR = ARR.reinterpret(L.byteSize());
			
			double[] ret = ARR.toArray(JAVA_DOUBLE);
		
			return ret;
		}
		
		return null;
	}
	
	public float[] getvector(int column) throws Throwable{
		
		MemorySegment next = (MemorySegment) lib_getvector.invokeExact(column);  
	
		int size = (int) resultSize.get(next);
		
		if(size > 0) {
			MemorySegment ARR = (MemorySegment) resultArr.get(next);
			
			SequenceLayout L = MemoryLayout.sequenceLayout(size,JAVA_FLOAT);
			ARR = ARR.reinterpret(L.byteSize());
			
			float[] ret = ARR.toArray(JAVA_FLOAT);
		
			return ret;
		}
		
		return null;
	}
}

