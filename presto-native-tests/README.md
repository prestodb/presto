# Presto Native Tests

This module contains end-to-end tests that run queries from test classes in 
the `presto-tests` module with Presto C++ workers. Please build the module
`presto-native-execution` first. 

The following command can be used to run all tests in this module:
```
mvn test 
    -pl 'presto-native-tests' 
    -Dtest="com.facebook.presto.nativetests.Test*" 
    -Duser.timezone=America/Bahia_Banderas 
    -DPRESTO_SERVER=${PRESTO_HOME}/presto-native-execution/cmake-build-debug/presto_cpp/main/presto_server 
    -DWORKER_COUNT=${WORKER_COUNT} -T1C
```
Please update JVM argument `PRESTO_SERVER` to point to the Presto C++ worker
binary `presto_server`. 

## Adding new tests

Presto C++ currently does not have the same behavior as Presto for certain 
queries. This could be because of missing types, missing function signatures,
among other reasons. Tests with these unsupported queries are therefore 
expected to fail and the test asserts the error message is as expected. 

Issues should also be created for the failing queries, so they are documented
and fixed. Please add the tag `presto-native-tests` for these issues. 
Once all the failures in a testcase are fixed, the overriden test in this 
module should be removed and the testcase in the corresponding base class in
`presto-tests` would be the single source of truth for Presto SQL coverage 
tests.  
