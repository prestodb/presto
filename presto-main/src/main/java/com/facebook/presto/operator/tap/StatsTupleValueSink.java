package com.facebook.presto.operator.tap;

import com.facebook.presto.Tuple;
import com.facebook.presto.block.TupleStreamPosition;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class StatsTupleValueSink
        implements TupleValueSink
{
    private long rowCount;
    private long runsCount;
    private Tuple lastTuple;
    private long minPosition = Long.MAX_VALUE;
    private long maxPosition = -1;
    private boolean finished = false;

    @Override
    public void process(TupleStreamPosition tupleStreamPosition)
    {
        checkNotNull(tupleStreamPosition, "tupleStreamPosition is null");
        checkState(!finished, "already finished");
        if (lastTuple == null) {
            lastTuple = tupleStreamPosition.getTuple();
        }
        else if (!tupleStreamPosition.currentTupleEquals(lastTuple)) {
            runsCount++;
            lastTuple = tupleStreamPosition.getTuple();
        }
        minPosition = Math.min(minPosition, tupleStreamPosition.getPosition());
        maxPosition = Math.max(maxPosition, tupleStreamPosition.getCurrentValueEndPosition());
        rowCount += tupleStreamPosition.getCurrentValueEndPosition() - tupleStreamPosition.getPosition() + 1;
    }

    @Override
    public void finished()
    {
        finished = true;
    }

    public Stats getStats()
    {
        return new Stats(rowCount, runsCount + 1, minPosition, maxPosition, rowCount / (runsCount + 1));
    }

    public static class Stats
    {
        private final long rowCount;
        private final long runsCount;
        private final long minPosition;
        private final long maxPosition;
        private final long avgRunLength;

        public Stats(long rowCount, long runsCount, long minPosition, long maxPosition, long avgRunLength)
        {
            this.rowCount = rowCount;
            this.runsCount = runsCount;
            this.minPosition = minPosition;
            this.maxPosition = maxPosition;
            this.avgRunLength = avgRunLength;
        }
        
        public static void serialize(Stats stats, SliceOutput sliceOutput)
        {
            // TODO: add a better way of serializing the stats that is less fragile
            sliceOutput.appendLong(stats.getRowCount())
                    .appendLong(stats.getRunsCount())
                    .appendLong(stats.getMinPosition())
                    .appendLong(stats.getMaxPosition())
                    .appendLong(stats.getAvgRunLength());
        }
        
        public static Stats deserialize(Slice slice)
        {
            SliceInput input = slice.input();
            long rowCount = input.readLong();
            long runsCount = input.readLong();
            long minPosition = input.readLong();
            long maxPosition = input.readLong();
            long avgRunLength = input.readLong();
            return new Stats(rowCount, runsCount, minPosition, maxPosition, avgRunLength);
        }

        public long getRowCount()
        {
            return rowCount;
        }

        public long getRunsCount()
        {
            return runsCount;
        }

        public long getMinPosition()
        {
            return minPosition;
        }

        public long getMaxPosition()
        {
            return maxPosition;
        }

        public long getAvgRunLength()
        {
            return avgRunLength;
        }
    }
}
