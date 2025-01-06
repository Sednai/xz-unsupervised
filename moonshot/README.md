
## Features
- Supports JVM in user session or as background process
- Support for native 2D arrays
- (Experimental) Non-JDBC data querying via SPI using the Java Foreign Function and Memory API
- Minimalistic code base

## Configuration & Installation

The operation mode as background process is implemented via shared memory and a task queue. The number of background JVM workers, queue size and data capacity is hard-coded in `moonshot_worker.h` via the following defines:

```C
#define MAX_WORKERS 1
#define MAX_QUEUE_LENGTH 16
#define MAX_DATA 2097152
```
You should make sure for your use case that `MAX_DATA` (in bytes) is sufficiently large to accommodate the arguments to the Java function call, respectively the returned result. The total shared memory reserved will be `MAX_QUEUE_LENGTH * MAX_DATA`. `MAX_QUEUE_LENGTH` should be adapted to your expected work load. Note that a PG error will be thrown under calls in case the queue is full. 

For installation, execute
```
make install
```

Java settings can be set in `postgres.conf` via the following options:
```
ms.libjvm = '/jdk-path/lib/server/libjvm.so'
ms.jvmoptions = '-Djava.class.path=' 
```
Note that only JNI compatible options are supported. Additional settings can be read from an external file by adding `@filename` options to `ms.jvmoptions`. 

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
`bytea, boolean, int, long, float4, float8, text`. Complex types consisting of these basic types are supported as argument and require a corresponding class with public variables matching the PG complex type member types. Java native types can be returned directly, while complex types and arrays have to be wrapped as public variables of a class. 

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

Currently, only the foreground worker supports `SETOF` return. For this, the java function has to return an `iterator`.  

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
    do {
        array = moonshot.fetch_next_double_array(1);
    }
    while(array != null);
    
    moonshot.disconnect();
    
} catch(Throwable t) {

}
```

## Important remarks

- Currently, all security considerations should be dealt with on PG level as no java security policy is implemented. You should not allow arbitrary users to create java functions, as the code will be run as a postgres process. Do not allow users to modify the GUC settings. Also, we advise against using the global background worker for sensitive data.

- The extension is in active development, not all features may be fully implemented yet for all worker types.

- If your session crashes on calling a java function, verify first that the JVM arguments are correct and try first with a foreground worker before raising an issue !

