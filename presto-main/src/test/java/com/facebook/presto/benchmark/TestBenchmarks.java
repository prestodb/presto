package com.facebook.presto.benchmark;

import com.facebook.presto.util.InMemoryTpchBlocksProvider;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;

import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestBenchmarks
{
    @Test
    public void smokeTest()
            throws Exception
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        try {
            for (AbstractBenchmark benchmark : BenchmarkSuite.createBenchmarks(executor, new InMemoryTpchBlocksProvider())) {
                try {
                    benchmark.runOnce();
                }
                catch (Exception e) {
                    throw new AssertionError("Error running " + benchmark.getBenchmarkName(), e);
                }
            }
        }
        finally {
            executor.shutdownNow();
        }
    }
}
