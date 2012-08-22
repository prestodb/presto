package com.facebook.presto;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

public class TestDictionarySerde {
    private SliceOutput sliceOutput;
    private DictionarySerde dictionarySerde;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        sliceOutput = new DynamicSliceOutput(1024);
        dictionarySerde = new DictionarySerde();
    }

    @Test
    public void testSanity() throws Exception {
        List<Slice> slices = slicesFromStrings("a", "b", "cde", "fuu", "a", "fuu");
        dictionarySerde.serialize(slices, sliceOutput);
        Assert.assertTrue(
                Iterables.elementsEqual(
                        slices,
                        DictionarySerde.deserialize(sliceOutput.slice().input())
                )
        );
    }

    @Test
    public void testEmpty() throws Exception {
        List<Slice> slices = Collections.EMPTY_LIST;
        dictionarySerde.serialize(slices, sliceOutput);
        Assert.assertTrue(
                Iterables.elementsEqual(
                        slices,
                        DictionarySerde.deserialize(sliceOutput.slice().input())
                )
        );
    }

    @Test
    public void testAllSame() throws Exception {
        List<Slice> slices = slicesFromStrings("a", "a", "a", "a", "a", "a", "a");
        dictionarySerde.serialize(slices, sliceOutput);
        Assert.assertTrue(
                Iterables.elementsEqual(
                        slices,
                        DictionarySerde.deserialize(sliceOutput.slice().input())
                )
        );
    }

    @Test
    public void testAllUnique() throws Exception {
        List<Slice> slices = slicesFromStrings("a", "b", "c", "d", "e", "f", "g");
        dictionarySerde.serialize(slices, sliceOutput);
        Assert.assertTrue(
                Iterables.elementsEqual(
                        slices,
                        DictionarySerde.deserialize(sliceOutput.slice().input())
                )
        );
    }

    @Test
    public void testSmallCardinality() throws Exception {
        List<Slice> slices = slicesFromStrings("a", "b", "c", "a", "c", "d", "c", "b");
        dictionarySerde = new DictionarySerde(4);
        dictionarySerde.serialize(slices, sliceOutput);
        Assert.assertTrue(
                Iterables.elementsEqual(
                        slices,
                        DictionarySerde.deserialize(sliceOutput.slice().input())
                )
        );
    }

    private List<Slice> slicesFromStrings(String... strs) {
        ImmutableList.Builder<Slice> builder = ImmutableList.builder();
        for (String str : strs) {
            builder.add(sliceFromString(str));
        }
        return builder.build();
    }

    private ByteArraySlice sliceFromString(String str) {
        return new ByteArraySlice(str.getBytes());
    }
}
