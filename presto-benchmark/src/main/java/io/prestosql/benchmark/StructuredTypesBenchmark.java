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
package io.prestosql.benchmark;

import io.prestosql.testing.LocalQueryRunner;

import static io.prestosql.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;

public class StructuredTypesBenchmark
        extends AbstractSqlBenchmark
{
    // Benchmark is modeled after TPCH query 1, with array/map creation added in
    public StructuredTypesBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "structured_types", 4, 5, "" +
                "select\n" +
                "    returnflag,\n" +
                "    linestatus,\n" +
                "    sum(array[quantity][1]) as sum_qty,\n" +
                "    sum(array[extendedprice][1]) as sum_base_price,\n" +
                "    sum(array[extendedprice][1] * (1 - map(array['key'], array[discount])['key'])) as sum_disc_price,\n" +
                "    sum(array[extendedprice][1] * (1 - map(array['key'], array[discount])['key']) * (1 + tax)) as sum_charge,\n" +
                "    avg(map(array['key'], array[quantity])['key']) as avg_qty,\n" +
                "    avg(map(array['key'], array[extendedprice])['key']) as avg_price,\n" +
                "    avg(map(array['key'], array[discount])['key']) as avg_disc\n" +
                "from\n" +
                "    lineitem\n" +
                "where\n" +
                "    shipdate <= DATE '1998-09-02'\n" +
                "group by\n" +
                "    returnflag,\n" +
                "    linestatus\n" +
                "order by\n" +
                "    returnflag,\n" +
                "    linestatus");
    }

    public static void main(String[] args)
    {
        new StructuredTypesBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
