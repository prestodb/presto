/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operators;

import com.facebook.presto.BlockBuilder;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.UncompressedBlockStream;
import com.facebook.presto.UncompressedValueBlock;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.Blocks.assertBlockStreamEquals;
import static com.facebook.presto.Blocks.createBlock;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;

public class TestMerge
{
    @Test
    public void test()
    {
        List<UncompressedValueBlock> values = ImmutableList.<UncompressedValueBlock>builder()
                .add(createBlock(0, "a", "b", "c", "d", "e", "f", "g"))
                .add(createBlock(20, "h", "i", "j", "k", "l", "m"))
                .add(createBlock(30, "n", "o", "p", "q", "r", "s"))
                .build();

        UncompressedBlockStream data = new UncompressedBlockStream(new TupleInfo(VARIABLE_BINARY), values);
        Merge merge = new Merge(data, data, data);

        TupleInfo expectedTupleInfo = new TupleInfo(VARIABLE_BINARY, VARIABLE_BINARY, VARIABLE_BINARY);
        BlockBuilder expectedBlock = new BlockBuilder(0, expectedTupleInfo);
        for (int i = 'a'; i <= 's'; i++) {
            expectedBlock.append(new byte[]{(byte) i});
            expectedBlock.append(new byte[]{(byte) i});
            expectedBlock.append(new byte[]{(byte) i});
        }

        assertBlockStreamEquals(merge, new UncompressedBlockStream(expectedTupleInfo, expectedBlock.build()));
    }
}
