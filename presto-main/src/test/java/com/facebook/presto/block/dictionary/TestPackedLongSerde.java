package com.facebook.presto.block.dictionary;

import com.google.common.collect.Iterables;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestPackedLongSerde {
    private SliceOutput sliceOutput;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        sliceOutput = new DynamicSliceOutput(128);
    }

    @Test
    public void testFullLong() throws Exception {
        List<Long> list = Arrays.asList(0L, -1L, 2L, Long.MAX_VALUE, Long.MIN_VALUE);
        new PackedLongSerde(Long.SIZE).serialize(list, sliceOutput);
        Assert.assertTrue(
                Iterables.elementsEqual(
                        PackedLongSerde.deserialize(sliceOutput.slice().getInput()),
                        list
                )
        );
    }

    @Test
    public void testLowDensity() throws Exception {
        List<Long> list = Arrays.asList(0L, -1L, 2L, Long.MAX_VALUE/2, Long.MIN_VALUE/2);
        new PackedLongSerde(Long.SIZE - 1).serialize(list, sliceOutput);
        Assert.assertTrue(
                Iterables.elementsEqual(
                        PackedLongSerde.deserialize(sliceOutput.slice().getInput()),
                        list
                )
        );
    }

    @Test
    public void testAligned() throws Exception {
        List<Long> list = Arrays.asList(0L, -1L, 2L,(long) Integer.MAX_VALUE, (long) Integer.MIN_VALUE);
        new PackedLongSerde(Integer.SIZE).serialize(list, sliceOutput);
        Assert.assertTrue(
                Iterables.elementsEqual(
                        PackedLongSerde.deserialize(sliceOutput.slice().getInput()),
                        list
                )
        );
    }

    @Test
    public void testUnaligned() throws Exception {
        List<Long> list = Arrays.asList(0L, -1L, 2L, 65535L, -65536L, 64L, -3L);
        new PackedLongSerde(17).serialize(list, sliceOutput);
        Assert.assertTrue(
                Iterables.elementsEqual(
                        PackedLongSerde.deserialize(sliceOutput.slice().getInput()),
                        list
                )
        );
    }

    @Test
    public void testEmpty() throws Exception {
        List<Long> list = Collections.EMPTY_LIST;
        new PackedLongSerde(1).serialize(list, sliceOutput);
        Assert.assertTrue(
                Iterables.elementsEqual(
                        PackedLongSerde.deserialize(sliceOutput.slice().getInput()),
                        list
                )
        );
    }
}
