package com.facebook.presto.block;

import com.facebook.presto.block.Blocks;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.uncompressed.UncompressedCursor;
import com.facebook.presto.block.uncompressed.UncompressedValueBlock;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestCursor
{
    @Test
    public void testUncompressed()
            throws Exception
    {
        List<UncompressedValueBlock> block = ImmutableList.<UncompressedValueBlock>builder()
            .add(Blocks.createLongsBlock(0, 0, 1, 2, 3, 4, 5, 6))
            .add(Blocks.createLongsBlock(10, 10, 11, 12, 13, 14, 15, 16))
            .add(Blocks.createLongsBlock(20, 20, 21, 22, 23, 24, 25, 26))
                .build();

        UncompressedCursor cursor = new UncompressedCursor(new TupleInfo(TupleInfo.Type.FIXED_INT_64), block.iterator());

        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        while (cursor.advanceNextValue()) {
            long value = cursor.getLong(0);
            builder.add(value);
        }

        assertEquals(builder.build(), ImmutableList.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 20L, 21L, 22L, 23L, 24L, 25L, 26L));
    }
}
