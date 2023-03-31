/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spark;

import org.testng.annotations.Test;

/**
 * Following JVM argument is needed to run Spark native tests.
 * <p>
 * - PRESTO_SERVER
 * - This tells Spark where to find the Presto native binary to launch the process.
 * Example: -DPRESTO_SERVER=/path/to/native/process/bin
 * <p>
 * - DATA_DIR
 * - Optional path to store TPC-H tables used in the test. If this directory is empty, it will be
 * populated. If tables already exists, they will be reused.
 * <p>
 * Tests can be running in Interactive Debugging Mode that allows for easier debugging
 * experience. Instead of launching its own native process, the test will connect to an existing
 * native process. This gives developers flexibility to connect IDEA and debuggers to the native process.
 * Enable this mode by setting NATIVE_PORT JVM argument.
 * <p>
 * - NATIVE_PORT
 * - This is the port your externally launched native process listens to. It is used to tell Spark where to send
 * requests. This port number has to be the same as to which your externally launched process listens.
 * Example: -DNATIVE_PORT=7777.
 * When NATIVE_PORT is specified, PRESTO_SERVER argument is not requires and is ignored if specified.
 * <p>
 * For test queries requiring shuffle, the disk-based local shuffle will be used.
 */
public class TestPrestoSparkNativeExecution
        extends AbstractTestPrestoSparkQueries
{
    @Test
    public void testMapOnlyQueries()
    {
        assertQuery("SELECT * FROM orders");
        assertQuery("SELECT orderkey, custkey FROM orders WHERE orderkey <= 200");
        assertQuery("SELECT nullif(orderkey, custkey) FROM orders");
        assertQuery("SELECT orderkey, custkey FROM orders ORDER BY orderkey LIMIT 4");
    }

    @Test
    public void testAggregations()
    {
        assertQuery("SELECT count(*) c FROM lineitem WHERE partkey % 10 = 1 GROUP BY partkey");
    }

    @Test
    public void testJoins()
    {
        assertQuery("SELECT count(*) FROM orders o, lineitem l WHERE o.orderkey = l.orderkey AND o.orderkey % 2 = 1");
    }

    @Test
    public void testFailures()
    {
        assertQueryFails("SELECT sequence(1, orderkey) FROM orders",
                ".*Scalar function name not registered: presto.default.sequence.*");
        assertQueryFails("SELECT orderkey / 0 FROM orders", ".*division by zero.*");
    }

    /**
     * Test native execution of a cpp function declared via json file, with sample function eq() defined
     * in src/test/resources/eq.json
     */
    @Test
    public void testJsonFileBasedFunction()
    {
        assertQuery("SELECT json.test_schema.eq(1, linenumber) FROM lineitem", "SELECT 1 = linenumber FROM lineitem");
    }
}
