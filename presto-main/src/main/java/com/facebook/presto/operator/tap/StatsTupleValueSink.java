package com.facebook.presto.operator.tap;

import com.facebook.presto.Tuple;
import com.facebook.presto.block.TupleStreamPosition;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;

import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class StatsTupleValueSink
        implements TupleValueSink
{
    private static final int MAX_UNIQUE_COUNT = 1000;

    private long rowCount;
    private long runsCount;
    private Tuple lastTuple;
    private long minPosition = Long.MAX_VALUE;
    private long maxPosition = -1;
    private final Set<Tuple> set = new HashSet<>(MAX_UNIQUE_COUNT);
    private boolean finished = false;

    @Override
    public void process(TupleStreamPosition tupleStreamPosition)
    {
        checkNotNull(tupleStreamPosition, "tupleStreamPosition is null");
        checkState(!finished, "already finished");
        if (lastTuple == null) {
            lastTuple = tupleStreamPosition.getTuple();
            if (set.size() < MAX_UNIQUE_COUNT) {
                set.add(lastTuple);
            }
        }
        else if (!tupleStreamPosition.currentTupleEquals(lastTuple)) {
            runsCount++;
            lastTuple = tupleStreamPosition.getTuple();
            if (set.size() < MAX_UNIQUE_COUNT) {
                set.add(lastTuple);
            }
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
        // TODO: expose a way to indicate whether the unique count is EXACT or APPROXIMATE
        return new Stats(rowCount, runsCount + 1, minPosition, maxPosition, rowCount / (runsCount + 1), (set.size() == MAX_UNIQUE_COUNT) ? Integer.MAX_VALUE : set.size());
    }

    public static class Stats
    {
        private final long rowCount;
        private final long runsCount;
        private final long minPosition;
        private final long maxPosition;
        private final long avgRunLength;
        private final int uniqueCount;

        public Stats(long rowCount, long runsCount, long minPosition, long maxPosition, long avgRunLength, int uniqueCount)
        {
            this.rowCount = rowCount;
            this.runsCount = runsCount;
            this.minPosition = minPosition;
            this.maxPosition = maxPosition;
            this.avgRunLength = avgRunLength;
            this.uniqueCount = uniqueCount;
        }

        public static void serialize(Stats stats, SliceOutput sliceOutput)
        {
            // TODO: add a better way of serializing the stats that is less fragile
            sliceOutput.appendLong(stats.getRowCount())
                    .appendLong(stats.getRunsCount())
                    .appendLong(stats.getMinPosition())
                    .appendLong(stats.getMaxPosition())
                    .appendLong(stats.getAvgRunLength())
                    .appendInt(stats.getUniqueCount());
        }

        public static Stats deserialize(Slice slice)
        {
            SliceInput input = slice.input();
            long rowCount = input.readLong();
            long runsCount = input.readLong();
            long minPosition = input.readLong();
            long maxPosition = input.readLong();
            long avgRunLength = input.readLong();
            int uniqueCount = input.readInt();
            return new Stats(rowCount, runsCount, minPosition, maxPosition, avgRunLength, uniqueCount);
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

        public int getUniqueCount()
        {
            return uniqueCount;
        }
    }
}
