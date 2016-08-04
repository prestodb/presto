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
package com.facebook.presto.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

import static java.lang.String.format;

@Fork(3)
public class BenchmarkDecimalAggregation
    extends AbstractJmhSqlBenchmark
{
    public static class AggregationContext extends AbstractContext
    {
        @Param({"orderstatus, avg(totalprice)",
                "orderstatus, min(totalprice)",
                "orderstatus, sum(totalprice), avg(totalprice), min(totalprice), max(totalprice)"})
        protected String project;

        @Param({"double", "decimal(14,2)", "decimal(30,10)"})
        protected String type;

        @Setup
        public void setUp()
        {
            runQuery(format(
                    "CREATE TABLE inmemory.default.orders AS SELECT orderstatus, cast(totalprice as %s) totalprice FROM tpch.sf1.orders",
                    type));
        }

        public void run()
        {
            runQuery(format("SELECT %s FROM orders GROUP BY orderstatus", project));
        }
    }

    @Benchmark
    public void benchmarkBuildHash(AggregationContext context)
    {
        context.run();
    }
}
