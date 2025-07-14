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
package com.facebook.presto.tests;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestSetWorkerSessionPropertiesIncludingInvalidProperties
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DistributedQueryRunner.builder(testSessionBuilder().build()).build();
    }

    @Test
    public void testSetSessionValidNativeWorkerSessionProperty()
    {
        // SET SESSION on a native-worker session property
        @Language("SQL") String setSession = "SET SESSION native_expression_max_array_size_in_reduce=50000";
        MaterializedResult setSessionResult = computeActual(setSession);
        assertEquals(
                setSessionResult.toString(),
                "MaterializedResult{rows=[[true]], " +
                        "types=[boolean], " +
                        "setSessionProperties={native_expression_max_array_size_in_reduce=50000}, " +
                        "resetSessionProperties=[], updateType=SET SESSION, clearTransactionId=false}");
    }

    @Test
    public void testSetSessionValidJavaWorkerSessionProperty()
    {
        // SET SESSION on a java-worker session property
        @Language("SQL") String setSession = "SET SESSION distinct_aggregation_spill_enabled=false";
        MaterializedResult setSessionResult = computeActual(setSession);
        assertEquals(
                setSessionResult.toString(),
                "MaterializedResult{rows=[[true]], " +
                        "types=[boolean], " +
                        "setSessionProperties={distinct_aggregation_spill_enabled=false}, " +
                        "resetSessionProperties=[], updateType=SET SESSION, clearTransactionId=false}");
    }
}
