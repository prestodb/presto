package com.facebook.presto.ingest;

import com.facebook.presto.operator.AlignmentOperator;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createLongsBlockIterable;
import static com.facebook.presto.block.BlockAssertions.createStringsBlockIterable;
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
        RecordSet records = new InMemoryRecordSet(ImmutableList.copyOf(new List<?>[]{ImmutableList.of("abc"), ImmutableList.of("def"), ImmutableList.of("g")}));

        RecordProjectOperator recordProjectOperator = new RecordProjectOperator(records, new DataSize(10, BYTE), VARIABLE_BINARY);
        assertOperatorEquals(recordProjectOperator, new AlignmentOperator(createStringsBlockIterable("abc", "def", "g")));
    }

    @Test
    public void testMultiColumn()
            throws Exception
    {
        RecordSet records = new InMemoryRecordSet(ImmutableList.copyOf(new List<?>[]{ImmutableList.of("abc", 1L), ImmutableList.of("def", 2L), ImmutableList.of("g", 0L)}));

        RecordProjectOperator recordProjectOperator = new RecordProjectOperator(records, new DataSize(10, BYTE), VARIABLE_BINARY, FIXED_INT_64);
        assertOperatorEquals(recordProjectOperator, new AlignmentOperator(
                createStringsBlockIterable("abc", "def", "g"),
                createLongsBlockIterable(1, 2, 0)
        ));
    }
}
