
## Features
- Supports JVM in user session or as background process
- Support for native 2D arrays
- (Experimental) Non-JDBC data querying via SPI using the Java Foreign Function and Memory API (read only)
- Stack trace return for Java exceptions
- Minimalistic code base

## Requirements
- Linux, gcc, make, mvn
- Postgres 10+
- Java 21 (for Non-JDBC API)

## Configuration & Installation

The operation mode as background process is implemented via shared memory and a task queue. The number of background JVM workers, queue size and data capacity is hard-coded in `moonshot_worker.h` via the following defines:

```C
#define MAX_USERS 1+1
#define MAX_WORKERS 1
#define MAX_QUEUE_LENGTH 16
#define MAX_DATA 2097152
```
You should make sure for your use case that `MAX_DATA` (in bytes) is sufficiently large to accommodate the arguments to the Java function call, respectively the returned result. The total shared memory reserved will be `MAX_QUEUE_LENGTH * MAX_DATA * MAX_USERS`. `MAX_QUEUE_LENGTH` should be adapted to your expected work load. Note that a PG error will be thrown under calls in case the queue is full. `MAX_USERS` is the maximum number of users which can start own Java background worker processes. Set to 1 if only a global background worker is needed. (In the future, we plan to switch to the new PG17 DSM API, which should allow for more flexibility.) `MAX_WORKERS` is the number of workers started to process a global or user queue.

For installation, execute
```
make install
```

Java settings can be set in `postgres.conf` via the following options:
```
ms.libjvm = '/jdk-path/lib/server/libjvm.so'
ms.jvmoptions = '-Djava.class.path=' 
```
Note that only JNI compatible Java options are supported. Additional settings can be read from external files by adding `@filename` options to `ms.jvmoptions`. 

In postgres, execute
```SQL
CREATE EXTENSION MOONSHOT;
```

## Usage
The PG language handler is installed as `MSJAVA`. Functions using the handler can be created as follows:
```
create function funcname(arguments) returns returntype as 'type_code|full_class_path_and_name|method_name|jni_signature' LANGUAGE MSJAVA;
```
Here, `type_code` can be either
```
F : Foreground worker
S : Foreground worker with SPI enabled
G : Global background worker (no SPI possible)
B : User background worker with SPI enabled
```
Note that `|jni_signature` corresponds to the full java function signature and is optional if no complex types or arrays are used in the `arguments` and `returntype`. Currently, supported basic `arguments` are 
`bytea, boolean, int, long, float4, float8, text`. Complex types consisting of these basic types are supported as argument and return, and require a corresponding class with public variables matching the PG complex type member types. Java native types and complex types can be returned directly, while arrays have to be wrapped by a complex type. 

**Example**:

Java: 

```Java
public class ComplexReturn {
    public int A;
    public double[] B;
}

public class my_functions {
    public static ComplexReturn func_test(int in_A, double in_B) {

        ComplexReturn R = new ComplexReturn();
        R.A = in_A;
        R.B = new double[1];
        R.B[0] = in_B;

        return R;
    }
}
```

Postgres: 

```SQL
create type complexreturn as (a int, b float8[]);
create function func_test(int, float8) returns complexreturn as 'F|my/classpath/my_functions|func_test' LANGUAGE MSJAVA;
```

Currently, only the foreground worker supports `SETOF` return. For this, the java function has to return an `iterator` of a complex type.  

**Example**:

```Java
public static Iterator iter_test() {
    ArrayList L = new ArrayList<ComplexReturn>();
    
    ComplexReturn R = new ComplexReturn();
    R.A = 1;
    R.B = new double[1];
    R.B[0] = 0.3;
		
    L.add(T);
    L.add(T);
    L.add(T);
		
    return L.listIterator();
}
```

## Java API

The Non-JDBC API can only be invoked in foreground mode or in a user based background worker. Build the jar in the `java/` directory with `mvn` and load onto the classpath. The API requires Java 21 and the JVM flags `--enable-preview --enable-native-access=moonshot`

**Example**

```Java
Moonshot moonshot = new Moonshot();
		
try {

    moonshot.connect();
    
    moonshot.execute("select colname from tablename");
    
    double[] array;
    while(moonshot.fetch_next()) {
        array = moonshot.getdoublearray(1);
	}
		     
    moonshot.disconnect();
    
} catch(Throwable t) {

}
```

For more examples, see `moonshot-test.sql` and `Tests.java`.


## Important remarks

- Currently, all security considerations should be dealt with on PG level as no java security policy is implemented. You should not allow arbitrary users to create java functions, as the code will be run as a postgres process. Do not allow users to modify the GUC settings. Also, we advise against using the global background worker for sensitive data.

- The extension is in active development, not all features may be fully implemented yet for all worker types.

- If your session crashes on calling a java function, verify first that the JVM arguments are correct and try first with a foreground worker before raising an issue !

