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

public class SqlTpchQuery1
        extends AbstractSqlBenchmark
{
    public SqlTpchQuery1(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "sql_tpch_query_1", 1, 5, "" +
                "select\n" +
                "    returnflag,\n" +
                "    linestatus,\n" +
                "    sum(quantity) as sum_qty,\n" +
                "    sum(extendedprice) as sum_base_price,\n" +
                "    sum(extendedprice * (1 - discount)) as sum_disc_price,\n" +
                "    sum(extendedprice * (1 - discount) * (1 + tax)) as sum_charge,\n" +
                "    avg(quantity) as avg_qty,\n" +
                "    avg(extendedprice) as avg_price,\n" +
                "    avg(discount) as avg_disc,\n" +
                "    count(*) as count_order\n" +
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
        new SqlTpchQuery1(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
