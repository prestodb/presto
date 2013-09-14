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

public class Top100SqlBenchmark
        extends AbstractSqlBenchmark
{
    public Top100SqlBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        super(executor, tpchBlocksProvider, "sql_top_100", 5, 50, "select totalprice from orders order by totalprice desc limit 100");
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new Top100SqlBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
