package com.facebook.presto.operator.inlined;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

// TODO: how to expose the schema coming out of this?
public class StatsInlinedOperator
        implements InlinedOperator
{
    public enum Fields
    {
        ROW_COUNT(0),
        RUNS_COUNT(1),
        MIN_POSITION(2),
        MAX_POSITION(3),
        AVG_RUN_LENGTH(4),;

        private final int columnIndex;

        private Fields(int columnIndex)
        {
            this.columnIndex = columnIndex;
        }

        public int getFieldIndex()
        {
            return columnIndex;
        }
    }

    private static final TupleInfo RESULT_TUPLE_INFO = new TupleInfo(
            TupleInfo.Type.FIXED_INT_64, // Row count
            TupleInfo.Type.FIXED_INT_64, // Runs count
            TupleInfo.Type.FIXED_INT_64, // Min position
            TupleInfo.Type.FIXED_INT_64, // Max position
            TupleInfo.Type.FIXED_INT_64  // Avg run length
    );

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

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return RESULT_TUPLE_INFO;
    }

    @Override
    public Range getRange()
    {
        return Range.create(0, 0);
    }

    @Override
    public TupleStream getResult()
    {
        checkState(lastTuple != null, "nothing processed yet");
        return new BlockBuilder(0, RESULT_TUPLE_INFO)
                .append(rowCount)
                .append(runsCount + 1)
                .append(minPosition)
                .append(maxPosition)
                .append(rowCount / (runsCount + 1)) // Average run length
                .build();
    }

    public static Stats resultsAsStats(TupleStream tupleStream)
    {
        Cursor cursor = tupleStream.cursor(new QuerySession());
        cursor.advanceNextPosition();
        return new Stats(
                cursor.getLong(Fields.ROW_COUNT.getFieldIndex()),
                cursor.getLong(Fields.RUNS_COUNT.getFieldIndex()),
                cursor.getLong(Fields.MIN_POSITION.getFieldIndex()),
                cursor.getLong(Fields.MAX_POSITION.getFieldIndex()),
                cursor.getLong(Fields.AVG_RUN_LENGTH.getFieldIndex())
        );
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
