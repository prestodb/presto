package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.block.uncompressed.UncompressedSerde;
import com.facebook.presto.slice.DynamicSliceOutput;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.block.StatsCollectingTupleStreamSerde.*;

public class TestStatsCollectingTupleStreamSerde
{
    private DynamicSliceOutput sliceOutput;
    private TupleStream tupleStream;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception
    {
        sliceOutput = new DynamicSliceOutput(1024);
        tupleStream = Blocks.createBlock(0, "a", "b", "b", "c", "c", "c", "c", "d");
    }

    @Test
    public void testSanity() throws Exception
    {
        StatsCollectingTupleStreamSerde serde = new StatsCollectingTupleStreamSerde(new UncompressedSerde());
        serde.createSerializer()
                .createTupleStreamWriter(sliceOutput)
                .append(tupleStream)
                .finish();
        StatsAnnotatedTupleStream statsAnnotatedTupleStream = serde.createDeserializer().deserialize(sliceOutput.slice());
        Blocks.assertTupleStreamEquals(statsAnnotatedTupleStream, tupleStream);
        Assert.assertEquals(statsAnnotatedTupleStream.getStats().getPositionRange(), Range.create(0, 7));
        Assert.assertEquals(statsAnnotatedTupleStream.getStats().getRowCount(), 8);
        Assert.assertEquals(statsAnnotatedTupleStream.getStats().getRunsCount(), 4);
        Assert.assertEquals(statsAnnotatedTupleStream.getStats().getAverageRunLength(), 2);
    }
}
