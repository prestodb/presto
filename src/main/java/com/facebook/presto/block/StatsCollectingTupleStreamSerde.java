package com.facebook.presto.block;

import com.facebook.presto.SizeOf;
import com.facebook.presto.operator.inlined.InlinedOperator;
import com.facebook.presto.operator.inlined.InlinedOperatorHook;
import com.facebook.presto.operator.inlined.StatsInlinedOperator;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class StatsCollectingTupleStreamSerde
        implements TupleStreamSerde
{
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
        public TupleStream deserialize(Slice slice)
        {
            checkNotNull(slice, "slice is null");
            int footerLength = slice.getInt(slice.length() - SizeOf.SIZE_OF_INT);
            int footerOffset = slice.length() - footerLength - SizeOf.SIZE_OF_INT;
            return tupleStreamDeserializer.deserialize(slice.slice(0, footerOffset));
        }

        // TODO: how do we expose the stats data to other components?
        public StatsInlinedOperator.Stats deserializeStats(Slice slice)
        {
            checkNotNull(slice, "slice is null");
            int footerLength = slice.getInt(slice.length() - SizeOf.SIZE_OF_INT);
            int footerOffset = slice.length() - footerLength - SizeOf.SIZE_OF_INT;
            return StatsInlinedOperator.resultsAsStats(TupleStreamSerdes.deserialize(TupleStreamSerdes.Encoding.RAW.createSerde(), slice.slice(footerOffset, footerLength)));
        }
    }

    private static class StatsCollectingTupleStreamWriter
            implements TupleStreamWriter
    {
        private final InlinedOperator statsInlinedOperator = new StatsInlinedOperator() {
            @Override
            public void finished()
            {
                // Do nothing. We don't want this operator to ever be marked as finished
            }
        };
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
            delegate.append(new InlinedOperatorHook(tupleStream, statsInlinedOperator));
            return this;
        }

        @Override
        public void finish()
        {
            delegate.finish();
            int startingIndex = sliceOutput.size();
            // TODO: add a better way of serializing the stats that is less fragile
            TupleStreamSerdes.serialize(TupleStreamSerdes.Encoding.RAW.createSerde(), statsInlinedOperator.getResult(), sliceOutput);
            int endingIndex = sliceOutput.size();
            checkState(endingIndex > startingIndex);
            sliceOutput.writeInt(endingIndex - startingIndex);
        }
    }
}
