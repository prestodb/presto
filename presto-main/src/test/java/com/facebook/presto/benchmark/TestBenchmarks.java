package com.facebook.presto.benchmark;

import org.testng.annotations.Test;

public class TestBenchmarks
{
    // dependencies are broken in maven so this won't run
    @Test(enabled = false)
    public void smokeTest()
            throws Exception
    {
        for (AbstractBenchmark benchmark : BenchmarkSuite.BENCHMARKS) {
            // this query is to slow for the smoke test
            if (benchmark instanceof SqlTpchQuery1) {
                continue;
            }
            benchmark.runOnce();
        }
    }
}
