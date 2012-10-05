package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.SizeOf;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Stage;
import io.airlift.json.JsonModule;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

import static com.google.common.base.Preconditions.*;

public class StatsCollectingTupleStreamSerde
        implements TupleStreamSerde
{
    private static final ObjectMapper OBJECT_MAPPER = Guice.createInjector(Stage.PRODUCTION, new JsonModule()).getInstance(ObjectMapper.class);

    private final TupleStreamSerde tupleStreamSerde;

    public StatsCollectingTupleStreamSerde(TupleStreamSerde tupleStreamSerde)
    {
        this.tupleStreamSerde = checkNotNull(tupleStreamSerde, "tupleStreamSerde is null");
    }

    @Override
    public TupleStreamSerializer createSerializer()
    {
        return new TupleStreamSerializer() {
            @Override
            public TupleStreamWriter createTupleStreamWriter(SliceOutput sliceOutput)
            {
                checkNotNull(sliceOutput, "sliceOutput is null");
                return new StatsCollectingTupleStreamWriter(sliceOutput, tupleStreamSerde.createSerializer().createTupleStreamWriter(sliceOutput));
            }
        };
    }

    @Override
    public StatsAnnotatedTupleStreamDeserializer createDeserializer()
    {
        return new StatsAnnotatedTupleStreamDeserializer(tupleStreamSerde.createDeserializer());
    }

    public static class StatsAnnotatedTupleStreamDeserializer
            implements TupleStreamDeserializer
    {
        private final TupleStreamDeserializer tupleStreamDeserializer;

        public StatsAnnotatedTupleStreamDeserializer(TupleStreamDeserializer tupleStreamDeserializer)
        {
            this.tupleStreamDeserializer = checkNotNull(tupleStreamDeserializer, "tupleStreamDeserializer is null");
        }

        @Override
        public StatsAnnotatedTupleStream deserialize(Slice slice)
        {
            checkNotNull(slice, "slice is null");
            int footerLength = slice.getInt(slice.length() - SizeOf.SIZE_OF_INT);
            int footerOffset = slice.length() - footerLength - SizeOf.SIZE_OF_INT;
            try {
                Stats stats = OBJECT_MAPPER.readValue(slice.slice(footerOffset, footerLength).input(), Stats.class);
                return new StatsAnnotatedTupleStream(tupleStreamDeserializer.deserialize(slice.slice(0, footerOffset)), stats);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public static class StatsAnnotatedTupleStream
            implements TupleStream
    {
        private final TupleStream tupleStream;
        private final Stats stats;

        private StatsAnnotatedTupleStream(TupleStream tupleStream, Stats stats)
        {
            this.tupleStream = checkNotNull(tupleStream, "tupleStream is null");
            this.stats = checkNotNull(stats, "stats is null");
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return tupleStream.getTupleInfo();
        }

        @Override
        public Range getRange()
        {
            return stats.getPositionRange();
        }

        @Override
        public Cursor cursor()
        {
            return tupleStream.cursor();
        }

        public Stats getStats()
        {
            return stats;
        }
    }

    private static class StatsCollectingTupleStreamWriter
            implements TupleStreamWriter
    {
        private final StatsMerger statsMerger = new StatsMerger();
        private final SliceOutput sliceOutput;
        private final TupleStreamWriter delegate;

        private StatsCollectingTupleStreamWriter(SliceOutput sliceOutput, TupleStreamWriter delegate)
        {
            this.sliceOutput = checkNotNull(sliceOutput, "sliceOutput is null");
            this.delegate = checkNotNull(delegate, "delegate is null");
        }

        @Override
        public StatsCollectingTupleStreamWriter append(TupleStream tupleStream)
        {
            checkNotNull(tupleStream, "tupleStream is null");
            StatsCollectingTupleStream statsCollectingTupleStream = new StatsCollectingTupleStream(tupleStream);
            delegate.append(statsCollectingTupleStream);
            statsMerger.merge(statsCollectingTupleStream.getStats());
            return this;
        }

        @Override
        public void finish()
        {
            delegate.finish();
            try {
                int startingIndex = sliceOutput.size();
                OBJECT_MAPPER.writeValue(sliceOutput, statsMerger.build());
                int endingIndex = sliceOutput.size();
                checkState(endingIndex > startingIndex);
                sliceOutput.writeInt(endingIndex - startingIndex);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    /**
     * Stats can ONLY be retrieved from this stream when one of its cursors has been run to completion
     */
    private static class StatsCollectingTupleStream
            implements TupleStream
    {
        private final TupleStream tupleStream;
        private StatsBuilder statsBuilder;

        private StatsCollectingTupleStream(TupleStream tupleStream)
        {
            this.tupleStream = checkNotNull(tupleStream, "tupleStream is null");
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return tupleStream.getTupleInfo();
        }

        @Override
        public Range getRange()
        {
            return tupleStream.getRange();
        }

        @Override
        public Cursor cursor()
        {
            if (statsBuilder == null) {
                statsBuilder = new StatsBuilder();
                return new StatsCollectingCursor(tupleStream.cursor(), statsBuilder);
            }
            else {
                // Stats should already have been collected
                return tupleStream.cursor();
            }
        }

        public Stats getStats()
        {
            checkState(statsBuilder != null, "no cursor was ever used");
            checkState(statsBuilder.isFinished(), "did not collect all stats");
            return statsBuilder.build();
        }
    }

    private static class StatsCollectingCursor
            extends ForwardingCursor
    {
        private final StatsBuilder statsBuilder;
        private long measuredPosition = -1;

        private StatsCollectingCursor(Cursor cursor, StatsBuilder statsBuilder)
        {
            super(checkNotNull(cursor, "cursor is null"));
            this.statsBuilder = checkNotNull(statsBuilder, "statsBuilder is null");
        }

        @Override
        public boolean advanceNextValue()
        {
            boolean result = getDelegate().advanceNextValue();
            processCurrentValueIfNecessary(result);
            return result;
        }

        @Override
        public boolean advanceNextPosition()
        {
            boolean result = getDelegate().advanceNextPosition();
            processCurrentValueIfNecessary(result);
            return result;
        }

        @Override
        public boolean advanceToPosition(long position)
        {
            // Skipping over too many positions will cause values to be omitted, which is not valid for a serializer
            Preconditions.checkArgument(position <= getDelegate().getCurrentValueEndPosition() + 1, "Serializer should not be skipping over values");
            boolean result = getDelegate().advanceToPosition(position);
            processCurrentValueIfNecessary(result);
            return result;
        }

        private void processCurrentValueIfNecessary(boolean successfulAdvance)
        {
            if (successfulAdvance) {
                if (getDelegate().getPosition() > measuredPosition) {
                    statsBuilder.process(getDelegate().getTuple(), Range.create(getDelegate().getPosition(), getDelegate().getCurrentValueEndPosition()));
                    measuredPosition = getDelegate().getCurrentValueEndPosition();
                }
            }
            else {
                statsBuilder.markFinished();
            }
        }
    }

    private static class StatsMerger
    {
        private long rowCount;
        private long runsCount;
        private Range range;

        public StatsMerger merge(Stats stats)
        {
            checkNotNull(stats, "stats is null");
            rowCount += stats.getRowCount();
            runsCount += stats.getRunsCount();
            range = (range == null) ? stats.getPositionRange() : range.outerBound(stats.getPositionRange());
            return this;
        }

        public Stats build()
        {
            return new Stats(rowCount, runsCount, range.getStart(), range.getEnd());
        }
    }

    private static class StatsBuilder
    {
        private long rowCount;
        private long runsCount;
        private Range range;
        private Tuple lastTuple;
        private boolean finished;

        public StatsBuilder process(Tuple tuple, Range tupleRange)
        {
            checkNotNull(tuple, "tuple is null");
            checkNotNull(tupleRange, "tupleRange is null");
            if (lastTuple == null) {
                lastTuple = tuple;
            }
            else if (!lastTuple.equals(tuple)) {
                runsCount++;
                lastTuple = tuple;
            }
            range = (range == null) ? tupleRange : range.outerBound(tupleRange);
            rowCount += tupleRange.length();
            return this;
        }

        public StatsBuilder markFinished()
        {
            finished = true;
            return this;
        }

        public boolean isFinished()
        {
            return finished;
        }

        public Stats build()
        {
            return new Stats(rowCount, runsCount + 1, range.getStart(), range.getEnd());
        }
    }

    /**
     * Serializable and deserializable with Jackson JSON Processor
     */
    public static class Stats
    {
        private final long rowCount;
        private final long runsCount;
        private final Range positionRange;

        @JsonCreator
        private Stats(
                @JsonProperty("rowCount") long rowCount,
                @JsonProperty("runsCount") long runsCount,
                @JsonProperty("startPosition") long startPosition,
                @JsonProperty("endPosition") long endPosition
        )
        {
            checkArgument(rowCount >= 0, "row count cannot be negative");
            checkArgument(runsCount >= 0, "runs count cannot be negative");
            checkArgument(startPosition >= 0, "start position cannot be negative");
            checkArgument(endPosition >= 0, "end position cannot be negative");
            this.rowCount = rowCount;
            this.runsCount = runsCount;
            this.positionRange = Range.create(startPosition, endPosition);
        }

        @JsonProperty
        public long getRowCount()
        {
            return rowCount;
        }

        @JsonProperty
        public long getRunsCount()
        {
            return runsCount;
        }

        @JsonProperty
        public long getStartPosition()
        {
            return positionRange.getStart();
        }

        @JsonProperty
        public long getEndPosition()
        {
            return positionRange.getEnd();
        }

        public long getAverageRunLength()
        {
            return rowCount / Math.max(runsCount, 1);
        }


        public Range getPositionRange()
        {
            return positionRange;
        }
    }
}
