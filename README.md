
# Distributed-K-V-consistent-storage

Initial Version.

To run Linearizable, use : 
```
./LinearizableRun.sh 4
```
This launches 4 processes for linearizable read-write servers.

To write a value, use 
```
put [variable name] [value]
```

To read a value, use
```$xslt
get [variable name]
```

To see all values in each replica, use
```$xslt
dump
```