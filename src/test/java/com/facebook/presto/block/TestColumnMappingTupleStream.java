package com.facebook.presto.block;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.slice.Slices;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.Charset;

import static com.facebook.presto.TupleInfo.Type.*;

public class TestColumnMappingTupleStream
{
    private TupleInfo tupleInfo;
    private TupleStream baseTupleStream;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception
    {
        tupleInfo = new TupleInfo(DOUBLE, FIXED_INT_64, VARIABLE_BINARY);
        baseTupleStream = new GenericTupleStream<>(tupleInfo, new BlockBuilder(0, tupleInfo)
                .append(0.1).append(15).append(Slices.copiedBuffer("test", Charset.defaultCharset()))
                .append(1.2).append(0).append(Slices.copiedBuffer("fuu", Charset.defaultCharset()))
                .append(100.99).append(-1).append(Slices.copiedBuffer("t", Charset.defaultCharset()))
                .build());
    }

    @Test
    public void testSingleColumnExtraction() throws Exception
    {
        Blocks.assertTupleStreamEquals(
                ColumnMappingTupleStream.map(baseTupleStream, 0),
                Blocks.createDoublesTupleStream(0, 0.1, 1.2, 100.99)
        );

        Blocks.assertTupleStreamEquals(
                ColumnMappingTupleStream.map(baseTupleStream, 1),
                Blocks.createLongsTupleStream(0, 15, 0, -1)
        );

        Blocks.assertTupleStreamEquals(
                ColumnMappingTupleStream.map(baseTupleStream, 2),
                Blocks.createTupleStream(0, "test", "fuu", "t")
        );
    }

    @Test
    public void testRemap() throws Exception
    {
        Blocks.assertTupleStreamEquals(
                ColumnMappingTupleStream.map(ColumnMappingTupleStream.map(baseTupleStream, 2, 1, 0), 0),
                Blocks.createTupleStream(0, "test", "fuu", "t")
        );

        Blocks.assertTupleStreamEquals(
                ColumnMappingTupleStream.map(ColumnMappingTupleStream.map(baseTupleStream, 1, 1), 1),
                Blocks.createLongsTupleStream(0, 15, 0, -1)
        );
    }

    @Test
    public void testMultiColumnExtraction() throws Exception
    {
        TupleInfo expectedTupleInfo = new TupleInfo(DOUBLE, FIXED_INT_64);
        TupleStream expectedTupleStream = new GenericTupleStream<>(expectedTupleInfo, new BlockBuilder(0, expectedTupleInfo)
                .append(0.1).append(15)
                .append(1.2).append(0)
                .append(100.99).append(-1)
                .build());
        Blocks.assertTupleStreamEquals(
                ColumnMappingTupleStream.map(baseTupleStream, 0, 1),
                expectedTupleStream
        );
    }
}
