package com.facebook.presto.ingest;

import com.facebook.presto.block.ProjectionTupleStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.MaterializingTupleStream;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.operator.MergeOperator;
import com.facebook.presto.operator.Splitter;
import com.facebook.presto.operator.tap.Tap;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Imports the specified TupleStream using provided StreamWriterTupleValueSinks
 */
public class TupleStreamImporter
{
    private static final DataSize DEFAULT_MAX_SPLIT_SIZE = new DataSize(64, DataSize.Unit.KILOBYTE);

    public static long importFrom(TupleStream tupleStream, List<StreamWriterTupleValueSink> tupleValueSinks) {
        checkNotNull(tupleStream, "tupleStream is null");
        checkNotNull(tupleValueSinks, "tupleValueSinks is null");
        checkArgument(tupleStream.getTupleInfo().getFieldCount() == tupleValueSinks.size(), "TupleStream does not match the provided sinks");

        MaterializingTupleStream materializingTupleStream = new MaterializingTupleStream(tupleStream);
        Splitter<TupleStream> splitter = new Splitter<>(
                materializingTupleStream.getTupleInfo(),
                tupleStream.getTupleInfo().getFieldCount(),
                (int) DEFAULT_MAX_SPLIT_SIZE.toBytes(),
                materializingTupleStream
        );
        ImmutableList.Builder<TupleStream> splits = ImmutableList.builder();
        for (int field = 0; field < tupleStream.getTupleInfo().getFieldCount(); field++) {
            TupleStream splitTupleStream = splitter.getSplit(field);
            Tap writerTupleStream = new Tap(
                    ProjectionTupleStream.project(splitTupleStream, field),
                    tupleValueSinks.get(field)
            );
            splits.add(writerTupleStream);
        }
        MergeOperator mergeOperator = new MergeOperator(splits.build());

        // Drive the import
        Cursor cursor = mergeOperator.cursor(new QuerySession());
        long rowCount = 0;
        while (Cursors.advanceNextValueNoYield(cursor)) {
            rowCount += cursor.getCurrentValueEndPosition() - cursor.getPosition() + 1;
        }
        return rowCount;
    }
}
