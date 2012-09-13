package com.facebook.presto.benchmark;

public interface BenchmarkResultHook
{
    BenchmarkResultHook addResult(long result);
    void finished();
}
