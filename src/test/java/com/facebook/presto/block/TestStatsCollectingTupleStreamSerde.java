package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.block.uncompressed.UncompressedSerde;
import com.facebook.presto.operator.inlined.StatsInlinedOperator;
import com.facebook.presto.slice.DynamicSliceOutput;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
        TupleStream resultTupleStream = serde.createDeserializer().deserialize(sliceOutput.slice());
        StatsInlinedOperator.Stats stats = serde.createDeserializer().deserializeStats(sliceOutput.slice());
        Blocks.assertTupleStreamEquals(resultTupleStream, tupleStream);
        
        Assert.assertEquals(stats.getAvgRunLength() , 2);
        Assert.assertEquals(stats.getMinPosition() , 0);
        Assert.assertEquals(stats.getMaxPosition() , 7);
        Assert.assertEquals(stats.getRowCount() , 8);
        Assert.assertEquals(stats.getRunsCount() , 4);
    }
}
