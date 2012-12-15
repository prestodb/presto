package com.facebook.presto.ingest;

import com.facebook.presto.operator.AlignmentOperator;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createLongsBlockIterable;
import static com.facebook.presto.block.BlockAssertions.createStringsBlockIterable;
import static com.facebook.presto.ingest.RecordIterables.asRecordIterable;
import static com.facebook.presto.ingest.RecordProjections.createProjection;
import static com.facebook.presto.operator.OperatorAssertions.assertOperatorEquals;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static io.airlift.units.DataSize.Unit.BYTE;

public class TestRecordProjectionOperator
{
    @Test
    public void testSingleColumn()
            throws Exception
    {
        RecordIterable records = asRecordIterable(ImmutableList.of(
                new StringRecord("abc"),
                new StringRecord("def"),
                new StringRecord("g"))
        );

        RecordProjectOperator recordProjectOperator = new RecordProjectOperator(records, new DataSize(10, BYTE), createProjection(0, VARIABLE_BINARY));
        assertOperatorEquals(recordProjectOperator, new AlignmentOperator(createStringsBlockIterable("abc", "def", "g")));
    }

    @Test
    public void testMultiColumn()
            throws Exception
    {
        RecordIterable records = asRecordIterable(ImmutableList.of(
                new StringRecord("abc", "1"),
                new StringRecord("def", "2"),
                new StringRecord("g", "0"))
        );

        RecordProjectOperator recordProjectOperator = new RecordProjectOperator(records,
                new DataSize(10, BYTE),
                createProjection(0, VARIABLE_BINARY), createProjection(1, FIXED_INT_64));
        assertOperatorEquals(recordProjectOperator, new AlignmentOperator(
                createStringsBlockIterable("abc", "def", "g"),
                createLongsBlockIterable(1, 2, 0)
        ));
    }
}
