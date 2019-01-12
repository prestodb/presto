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

public class SqlTpchQuery6
        extends AbstractSqlBenchmark
{
    public SqlTpchQuery6(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "sql_tpch_query_6", 4, 20, "" +
                "select sum(extendedprice * discount) as revenue \n" +
                "from lineitem \n" +
                "where shipdate >= DATE '1994-01-01' \n" +
                "   and shipdate < DATE '1995-01-01' \n" +
                "   and discount >= 0.05 \n" +
                "   and discount <= 0.07 \n" +
                "   and quantity < 24");
    }

    public static void main(String[] args)
    {
        new SqlTpchQuery6(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
