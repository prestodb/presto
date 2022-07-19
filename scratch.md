== PR description ====
Correctness test - 
https://www.internalfb.com/intern/presto/verifier/result/?event_uuid=a19032d2-e732-49ca-a1ed-c1380c0b47b7

Functional test - 

Unit test - 

Regression test- 




=== Memory model

- MergingInMemory* uses system memory because this is our overhead
- Push-pull model is bad because ???
- Yielding memory update ???

===== Testing
                                                 QueryRunner
                                                        |
                            -----------------------------------------------------------------------
                            /               /           |                  \                       \               
                           /               /            |                   \                       \
                    Distributed         StandAlone      Local           PrestoSpark               Thrift
                    /       |                   \           \
                Tpch        TestSpilledAggWith*             *Benchmark      
                            Hive
                            Cassa*
                            Druid*
                            Elastic

t
                                                         AbstractTestQueryFramework
                                                                |   
                                    ----------------------------------------------------------------------------------------------------------------------
                                    /               /          |                  \                       \                     \                        \   
                                   /               /            |                   \                       \                     \                        \
                            AbsTest(allbasic)     /      AbsTestAggregations         AbsTestJoin         AbsTestOrderBy             AbsTestWindow        AbsTestTopN
                            /                    /             |                    \                       \
                           /                    /              |                     \                       \
                          /                    /     HiveDistributed            HiveDistributed         HiveDistributed
                         /                    /      PrestoSpark                PrestoSpark             PrestoSpark
                        /                    /       Spilled                    Spilled                 Spilled
                                            /        *test                      *test                   *test   
                                           /
                                          /
                                         /
                                TestPrestoSparkQueryRunner
                                        |
                                Join, Aggregatino, Limit, Spill,
                                OrderBy