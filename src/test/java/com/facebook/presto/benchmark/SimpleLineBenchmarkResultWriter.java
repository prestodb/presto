package com.facebook.presto.benchmark;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.OutputStream;

public class SimpleLineBenchmarkResultWriter
    implements BenchmarkResultHook
{
    private final OutputStream outputStream;

    public SimpleLineBenchmarkResultWriter(OutputStream outputStream)
    {
        this.outputStream = Preconditions.checkNotNull(outputStream, "outputStream is null");
    }

    @Override
    public BenchmarkResultHook addResult(long result)
    {
        try {
            outputStream.write(String.format("%d\n", result).getBytes(Charsets.UTF_8));
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
