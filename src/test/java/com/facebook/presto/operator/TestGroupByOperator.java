/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.block.uncompressed.UncompressedBlockStream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.block.Blocks.createBlock;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Charsets.UTF_8;

public class TestGroupByOperator
{
    @Test
    public void testGroupBy()
    {
        GroupByOperator groupBy = new GroupByOperator(newGroupColumn());

        Map<String, Range> expected = ImmutableMap.<String, Range>of(
                "apple", Range.create(0, 3),
                "banana", Range.create(4, 22),
                "cherry", Range.create(23, 25),
                "date", Range.create(30, 32)
        );

        Map<String, Range> actual = new HashMap<>();
        for (RunLengthEncodedBlock block : groupBy) {
            Tuple tuple = block.getValue();
            String key = tuple.getSlice(0).toString(UTF_8);
            Range range = block.getRange();
            actual.put(key, range);
        }

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testGroupBySimple()
    {
        UncompressedBlockStream data = new UncompressedBlockStream(new TupleInfo(VARIABLE_BINARY),
                ImmutableList.of(createBlock(0, "apple", "banana", "cherry", "date")));

        GroupByOperator groupBy = new GroupByOperator(data);

        Map<String, Range> expected = ImmutableMap.of(
                "apple", Range.create(0, 0),
                "banana", Range.create(1, 1),
                "cherry", Range.create(2, 2),
                "date", Range.create(3, 3)
        );

        Map<String, Range> actual = new HashMap<>();
        for (RunLengthEncodedBlock block : groupBy) {
            Tuple tuple = block.getValue();
            String key = tuple.getSlice(0).toString(UTF_8);
            Range range = block.getRange();
            actual.put(key, range);
        }

        Assert.assertEquals(actual, expected);
    }

    public TupleStream newGroupColumn()
    {
        List<UncompressedBlock> values = ImmutableList.<UncompressedBlock>builder()
                .add(createBlock(0, "apple", "apple", "apple", "apple", "banana", "banana"))
                .add(createBlock(20, "banana", "banana", "banana", "cherry", "cherry", "cherry"))
                .add(createBlock(30, "date"))
                .add(createBlock(31, "date"))
                .add(createBlock(32, "date"))
                .build();

        return new UncompressedBlockStream(new TupleInfo(VARIABLE_BINARY), values);
    }

}
