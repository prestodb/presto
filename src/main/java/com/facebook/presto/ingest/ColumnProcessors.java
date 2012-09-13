package com.facebook.presto.ingest;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ColumnProcessors
{
    private static final int DEFAULT_BATCH_SIZE = 10_000;

    public static void process(List<? extends ColumnProcessor> processors)
            throws IOException
    {
        process(processors, DEFAULT_BATCH_SIZE);
    }

    public static void process(List<? extends ColumnProcessor> processors, int batchSize)
            throws IOException
    {
        checkNotNull(processors, "processors is null");
        long end = 0;
        while (true) {
            end = Math.min(end + batchSize, Integer.MAX_VALUE);
            boolean moreData = false;
            for (ColumnProcessor processor : processors) {
                moreData |= processor.processPositions(end);
            }
            if (!moreData) {
                return;
            }
            checkState(end < Integer.MAX_VALUE, "processor should be complete");
        }
    }
}
