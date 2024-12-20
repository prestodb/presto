
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
package com.facebook.presto.nativetests;

import com.facebook.presto.nativeworker.NativeQueryRunnerUtils;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;

public class TestOptimizeMixedDistinctAggregations
        extends AbstractTestAggregationsNative
{
    private static final String storageFormat = "PARQUET";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createNativeQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of("optimizer.optimize-mixed-distinct-aggregations", "true"), storageFormat);
    }

    @Override
    protected void createTables()
    {
        try {
            QueryRunner javaQueryRunner = PrestoNativeQueryRunnerUtils.createJavaQueryRunner(storageFormat);
            NativeQueryRunnerUtils.createAllTables(javaQueryRunner, false);
            javaQueryRunner.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void testCountDistinct()
    {
        assertQuery("SELECT COUNT(DISTINCT custkey + 1) FROM orders", "SELECT COUNT(*) FROM (SELECT DISTINCT custkey + 1 FROM orders) t");
        assertQuery("SELECT COUNT(DISTINCT linenumber), COUNT(*) from lineitem where linenumber < 0");
    }
}
