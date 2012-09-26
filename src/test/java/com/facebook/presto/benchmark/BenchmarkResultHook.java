package com.facebook.presto.benchmark;

import java.util.Map;

public interface BenchmarkResultHook
{
    BenchmarkResultHook addResults(Map<String, Long> results);
    void finished();
}
