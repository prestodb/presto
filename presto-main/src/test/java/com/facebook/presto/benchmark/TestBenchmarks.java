package com.facebook.presto.benchmark;

import com.facebook.presto.util.InMemoryTpchBlocksProvider;
import org.testng.annotations.Test;

public class TestBenchmarks
{
    @Test
    public void smokeTest()
            throws Exception
    {
        for (AbstractBenchmark benchmark : BenchmarkSuite.createBenchmarks(new InMemoryTpchBlocksProvider())) {
            try {
                benchmark.runOnce();
            }
            catch (Exception e) {
                throw new AssertionError("Error running " + benchmark.getBenchmarkName(), e);
            }
        }
    }
}
