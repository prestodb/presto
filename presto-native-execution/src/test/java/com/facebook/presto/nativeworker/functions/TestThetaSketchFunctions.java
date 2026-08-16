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
package com.facebook.presto.nativeworker.functions;

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestThetaSketchFunctions
        extends AbstractTestQueryFramework
{
    private String storageFormat;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        storageFormat = "PARQUET";
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .build();
        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
            .setStorageFormat(storageFormat)
            .build();
    }

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        queryRunner.execute("DROP TABLE IF EXISTS test_sketch_theta_functions");
        queryRunner.execute("CREATE TABLE test_sketch_theta_functions (" +
                "nullColumn integer, t tinyint, s smallint, i integer," +
                "l bigint, r real, d double, v varchar, " + "dt date" + ", ts timestamp," +
                "sd decimal(10,4)" +
                ", ld decimal(30,8))");
        queryRunner.execute("INSERT INTO test_sketch_theta_functions VALUES(" +
                "null,cast(25 as tinyint),cast(250 as smallint),40000,2147483650," +
                "214748.3650,2147483650123283628.72323,'sampletesttext'," +
                "date'2025-11-12'" +
                ",timestamp'2025-11-12 03:47:58',cast(214748.3650 as DECIMAL(10,4))," +
                "cast(2147483650123283628123.72323123 as DECIMAL(30,8)))");
    }

    @Test
    public void testSketchThetaSummary()
    {
        assertQuery("SELECT sketch_theta_summary(sketch_theta(nullColumn)) FROM test_sketch_theta_functions");

        assertQuery("SELECT sketch_theta_summary(sketch_theta(i)) FROM test_sketch_theta_functions");
        assertQuery("SELECT sketch_theta_summary(sketch_theta(s)) FROM test_sketch_theta_functions");
        assertQuery("SELECT sketch_theta_summary(sketch_theta(t)) FROM test_sketch_theta_functions");
        assertQuery("SELECT sketch_theta_summary(sketch_theta(l)) FROM test_sketch_theta_functions");

        assertQuery("SELECT sketch_theta_summary(sketch_theta(r)) FROM test_sketch_theta_functions");
        assertQuery("SELECT sketch_theta_summary(sketch_theta(d)) FROM test_sketch_theta_functions");

        assertQuery("SELECT sketch_theta_summary(sketch_theta(v)) FROM test_sketch_theta_functions");

        assertQuery("SELECT sketch_theta_summary(sketch_theta(dt)) FROM test_sketch_theta_functions");
        assertQuery("SELECT sketch_theta_summary(sketch_theta(ts)) FROM test_sketch_theta_functions");

//        assertQuery("SELECT sketch_theta_summary(sketch_theta(sd)) FROM test_sketch_theta_functions");
        assertQuery("SELECT sketch_theta_summary(sketch_theta(ld)) FROM test_sketch_theta_functions");
    }

    @Test
    public void testSketchThetaEstimate()
    {
        assertQuery("SELECT sketch_theta_estimate(sketch_theta(nullColumn)) FROM test_sketch_theta_functions");

        assertQuery("SELECT sketch_theta_estimate(sketch_theta(i)) FROM test_sketch_theta_functions");
        assertQuery("SELECT sketch_theta_estimate(sketch_theta(s)) FROM test_sketch_theta_functions");
        assertQuery("SELECT sketch_theta_estimate(sketch_theta(t)) FROM test_sketch_theta_functions");
        assertQuery("SELECT sketch_theta_estimate(sketch_theta(l)) FROM test_sketch_theta_functions");

        assertQuery("SELECT sketch_theta_estimate(sketch_theta(r)) FROM test_sketch_theta_functions");
        assertQuery("SELECT sketch_theta_estimate(sketch_theta(d)) FROM test_sketch_theta_functions");

        assertQuery("SELECT sketch_theta_estimate(sketch_theta(v)) FROM test_sketch_theta_functions");

        assertQuery("SELECT sketch_theta_estimate(sketch_theta(dt)) FROM test_sketch_theta_functions");
        assertQuery("SELECT sketch_theta_estimate(sketch_theta(ts)) FROM test_sketch_theta_functions");

        assertQuery("SELECT sketch_theta_estimate(sketch_theta(sd)) FROM test_sketch_theta_functions");
        assertQuery("SELECT sketch_theta_estimate(sketch_theta(ld)) FROM test_sketch_theta_functions");
    }
}
