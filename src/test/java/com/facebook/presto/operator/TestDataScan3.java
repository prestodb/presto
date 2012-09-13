package com.facebook.presto.operator;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.ValueBlockStream;
import com.facebook.presto.block.uncompressed.UncompressedBlockStream;
import com.facebook.presto.block.position.UncompressedPositionBlock;
import com.facebook.presto.block.uncompressed.UncompressedValueBlock;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.Blocks.assertBlockStreamEquals;
import static com.facebook.presto.block.Blocks.createBlock;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;

public class TestDataScan3
{

    private static final TupleInfo INFO = new TupleInfo(VARIABLE_BINARY);

    @Test
    public void test()
    {
        List<UncompressedValueBlock> values = ImmutableList.<UncompressedValueBlock>builder()
                .add(createBlock(0, "a", "b", "c", "d", "e", "f"))
                .add(createBlock(20, "h", "i", "j", "k", "l", "m"))
                .add(createBlock(30, "n", "o", "p", "q", "r", "s"))
                .build();

        List<UncompressedPositionBlock> positions = ImmutableList.<UncompressedPositionBlock>builder()
                .add(new UncompressedPositionBlock(2L, 4L, 6L, 8L))
                .add(new UncompressedPositionBlock(10L, 11L, 12L, 13L))
                .add(new UncompressedPositionBlock(18L, 19L, 20L, 21L, 22L))
                .add(new UncompressedPositionBlock(31L, 33L, 35L))
                .add(new UncompressedPositionBlock(40L, 41L, 42L))
                .build();

        DataScan3 dataScan3 = new DataScan3(INFO, new UncompressedBlockStream(INFO, values), new ValueBlockStream<>(new TupleInfo(), positions));

        assertBlockStreamEquals(dataScan3,
                new UncompressedBlockStream(INFO,
                        createBlock(2, "c"),
                        createBlock(4, "e"),
                        createBlock(20, "h", "i", "j"),
                        createBlock(31, "o"),
                        createBlock(33, "q"),
                        createBlock(35, "s")));

    }
}
