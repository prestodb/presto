
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

import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.Session.builder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder;
import static java.lang.Boolean.parseBoolean;

public class TestOptimizeMixedDistinctAggregations
        extends AbstractTestAggregationsNative
{
    private String storageFormat;
    private boolean sidecarEnabled;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        super.init(storageFormat, sidecarEnabled);
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return NativeTestsUtils.createNativeQueryRunner(storageFormat, sidecarEnabled, Optional.of(nativeHiveQueryRunnerBuilder().setExtraCoordinatorProperties(
                        ImmutableMap.<String, String>builder().put("optimizer.optimize-mixed-distinct-aggregations", "true").build())));
    }

    @Override
    protected void createTables()
    {
        NativeTestsUtils.createTables(storageFormat);
    }

    @Override
    @Test
    public void testCountDistinct()
    {
        assertQuery("SELECT COUNT(DISTINCT custkey + 1) FROM orders", "SELECT COUNT(*) FROM (SELECT DISTINCT custkey + 1 FROM orders) t");
        if (sidecarEnabled) {
            assertQuery("SELECT COUNT(DISTINCT linenumber), COUNT(*) from lineitem where linenumber < 0", "VALUES (0, NULL)");
        }
        else {
            assertQuery("SELECT COUNT(DISTINCT linenumber), COUNT(*) from lineitem where linenumber < 0");
        }
        assertQuery(" SELECT COUNT(*) FROM (SELECT orderkey, COUNT(DISTINCT partkey) FROM lineitem " +
                        "  GROUP BY orderkey " +
                        "HAVING COUNT(DISTINCT partkey) != CARDINALITY(ARRAY_DISTINCT(ARRAY_AGG(partkey))))",
                "VALUES 0");
        assertQuery(builder(getSession())
                        .setSystemProperty("use_mark_distinct", "false")
                        .build(),
                " SELECT COUNT(*) FROM (SELECT orderkey, COUNT(DISTINCT partkey) FROM lineitem " +
                        "  GROUP BY orderkey " +
                        "HAVING COUNT(DISTINCT partkey) != CARDINALITY(ARRAY_DISTINCT(ARRAY_AGG(partkey))))",
                "VALUES 0");
    }
}
