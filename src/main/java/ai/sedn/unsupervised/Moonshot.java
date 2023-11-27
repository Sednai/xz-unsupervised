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

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE;

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

/**
*
* TODO Short description
* @author krefl
* @version $Id$
*
*/

public class Moonshot {
	private Arena arena;
	
	private MethodHandle lib_connect;
	private MethodHandle lib_disconnect;
	private MethodHandle lib_execute;
	private MethodHandle lib_fetch_next_double_array;
	
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
		FunctionDescriptor lib_connect_sig = FunctionDescriptor.ofVoid();
		lib_connect = linker.downcallHandle(lib_connect_addr, lib_connect_sig); 
			
		MemorySegment lib_disconnect_addr = lib.find("disconnect_SPI").get();
		FunctionDescriptor lib_disconnect_sig = FunctionDescriptor.ofVoid();
		lib_disconnect = linker.downcallHandle(lib_disconnect_addr, lib_disconnect_sig); 
		
		MemorySegment lib_execute_addr = lib.find("execute").get();
		FunctionDescriptor lib_execute_sig = FunctionDescriptor.ofVoid(ADDRESS);
		lib_execute = linker.downcallHandle(lib_execute_addr, lib_execute_sig); 
		
		MemorySegment lib_fetch_next_double_array_addr = lib.find("fetch_next_double_array").get();
		FunctionDescriptor lib_fetch_next_double_array_sig = FunctionDescriptor.of(ADDRESS.withTargetLayout(arrayLayout));
		lib_fetch_next_double_array = linker.downcallHandle(lib_fetch_next_double_array_addr, lib_fetch_next_double_array_sig); 
	
	
	}
	
	public void connect() throws Throwable {
		lib_connect.invokeExact();
	}
	
	public void disconnect() throws Throwable {
		lib_disconnect.invokeExact();
	}
	
	public void execute(String query) throws Throwable {
		
		var cString = arena.allocateUtf8String(query);
		
		lib_execute.invokeExact(cString);
	}
	
	public double[] fetch_next_double_array() throws Throwable{
		
		MemorySegment next = (MemorySegment) lib_fetch_next_double_array.invokeExact();  
	
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
	
	public MemorySegment fetch_next_double_ms() throws Throwable {
		
		MemorySegment next = (MemorySegment) lib_fetch_next_double_array.invokeExact();  
	
		int size = (int) resultSize.get(next);
		
		if(size > 0) {
			MemorySegment ARR = (MemorySegment) resultArr.get(next);
			
			SequenceLayout L = MemoryLayout.sequenceLayout(size,JAVA_DOUBLE);
			ARR = ARR.reinterpret(L.byteSize()).asReadOnly();
			
			return ARR;
		}
		
		
		return null;
	}
	
	
	
	
	
}

