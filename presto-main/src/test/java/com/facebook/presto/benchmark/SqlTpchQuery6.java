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

import com.facebook.presto.tpch.TpchBlocksProvider;

import java.util.concurrent.ExecutorService;

import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class SqlTpchQuery6
        extends AbstractSqlBenchmark
{
    public SqlTpchQuery6(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        super(executor, tpchBlocksProvider, "sql_tpch_query_6", 4, 20, "" +
                "select sum(extendedprice * discount) as revenue \n" +
                "from lineitem \n" +
                "where shipdate >= '1994-01-01' \n" +
                "   and shipdate < '1995-01-01' \n" +
                "   and discount >= 0.05 \n" +
                "   and discount <= 0.07 \n" +
                "   and quantity < 24");
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new SqlTpchQuery6(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
