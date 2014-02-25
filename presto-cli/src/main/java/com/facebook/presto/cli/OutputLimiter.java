package com.facebook.presto.cli;

import com.google.common.io.CountingOutputStream;
import io.airlift.units.DataSize;

import static com.google.common.base.Preconditions.checkNotNull;

public class OutputLimiter
{
    private final CountingOutputStream out;
    private final DataSize outputBufferLimit;

    public OutputLimiter(CountingOutputStream out, DataSize outputBufferLimit)
    {
        this.out = checkNotNull(out, "out is null");
        this.outputBufferLimit = checkNotNull(outputBufferLimit, "outputBufferLimit is null");
    }

    public boolean isBufferFull()
    {
        return out.getCount() >= outputBufferLimit.toBytes();
    }
}
