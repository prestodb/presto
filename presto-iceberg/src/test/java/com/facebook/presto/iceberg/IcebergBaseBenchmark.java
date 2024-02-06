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
package com.facebook.presto.iceberg;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

public abstract class IcebergBaseBenchmark
{
    protected DistributedQueryRunner queryRunner;

    protected IcebergBaseBenchmark()
    {
        queryRunner = getQueryRunner();
    }

    protected abstract DistributedQueryRunner getQueryRunner();

    @Setup
    public void setUp()
    {
        queryRunner.execute("CREATE SCHEMA iceberg.test_db");
        queryRunner.execute("CREATE TABLE iceberg.test_db.region AS SELECT * from tpch.sf1.region");
        queryRunner.execute("CREATE TABLE iceberg.test_db.orders AS SELECT * from tpch.sf1.orders");
        queryRunner.execute("CREATE TABLE iceberg.test_db.lineitem AS SELECT * from tpch.sf1.lineitem");
    }

    @TearDown
    public void tearDown()
    {
        queryRunner.execute("DROP TABLE IF EXISTS iceberg.test_db.region");
        queryRunner.execute("DROP TABLE IF EXISTS iceberg.test_db.orders");
        queryRunner.execute("DROP TABLE IF EXISTS iceberg.test_db.lineitem");
        queryRunner.execute("DROP SCHEMA IF EXISTS iceberg.test_db");
        queryRunner.close();
        queryRunner = null;
    }

    @Benchmark
    public MaterializedResult benchmarkIcebergTableSelect()
    {
        return queryRunner
                .execute("SELECT * FROM iceberg.test_db.region");
    }

    @Benchmark
    public MaterializedResult benchmarkIcebergTableTpchQ4()
    {
        return queryRunner
                .execute("SELECT o.orderpriority, " +
                        "    count(*) AS order_count " +
                        "FROM iceberg.test_db.orders o " +
                        "WHERE o.orderdate >= DATE '1993-07-01' " +
                        "    AND o.orderdate < DATE '1993-07-01' + interval '3' month " +
                        "    AND EXISTS (SELECT * " +
                        "                FROM iceberg.test_db.lineitem l " +
                        "                WHERE l.orderkey = o.orderkey " +
                        "                    AND l.commitdate < l.receiptdate) " +
                        "GROUP  BY o.orderpriority " +
                        "ORDER  BY o.orderpriority ");
    }
}
