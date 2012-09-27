package com.facebook.presto.benchmark;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class SimpleLineBenchmarkResultWriter
        implements BenchmarkResultHook
{
    private final OutputStream outputStream;

    public SimpleLineBenchmarkResultWriter(OutputStream outputStream)
    {
        this.outputStream = Preconditions.checkNotNull(outputStream, "outputStream is null");
    }

    @Override
    public BenchmarkResultHook addResults(Map<String, Long> results)
    {
        Preconditions.checkNotNull(results, "results is null");
        try {
            boolean first = true;
            for (Map.Entry<String, Long> entry : results.entrySet()) {
                if (first) {
                    first = false;
                }
                else {
                    outputStream.write(",".getBytes(Charsets.UTF_8));
                }
                outputStream.write(String.format("%s:%d", entry.getKey(), entry.getValue()).getBytes(Charsets.UTF_8));
            }
            outputStream.write("\n".getBytes(Charsets.UTF_8));
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        return this;
    }

    @Override
    public void finished()
    {
        // No-op
    }
}
