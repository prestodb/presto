package com.facebook.presto.operator.tap;

import com.facebook.presto.Tuple;
import com.facebook.presto.block.TupleStreamPosition;
import com.facebook.presto.serde.StatsCollectingBlocksSerde.StatsCollector.Stats;

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
}
