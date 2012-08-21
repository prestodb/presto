package com.facebook.presto;

import com.google.common.collect.ImmutableList;
import com.google.common.io.InputSupplier;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.InputStreamReader;

import static com.facebook.presto.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.SizeOf.SIZE_OF_SHORT;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.newReaderSupplier;

public class TestCsvFileScanner
{
    private final InputSupplier<InputStreamReader> inputSupplier = newReaderSupplier(getResource("data.csv"), UTF_8);

    @Test
    public void testIterator()
            throws Exception
    {
        CsvFileScanner firstColumn = new CsvFileScanner(inputSupplier, 0, ',', FIXED_INT_64);

        ImmutableList<Pair> actual = ImmutableList.copyOf(new PairsIterator(firstColumn.iterator()));
        Assert.assertEquals(actual,
                ImmutableList.of(
                        new Pair(0, createTuple(0)),
                        new Pair(1, createTuple(1)),
                        new Pair(2, createTuple(2)),
                        new Pair(3, createTuple(3))));

        CsvFileScanner secondColumn = new CsvFileScanner(inputSupplier, 1, ',', VARIABLE_BINARY);
        Assert.assertEquals(ImmutableList.copyOf(new PairsIterator(secondColumn.iterator())),
                ImmutableList.of(
                        new Pair(0, createTuple("apple")),
                        new Pair(1, createTuple("banana")),
                        new Pair(2, createTuple("cherry")),
                        new Pair(3, createTuple("date"))));

        CsvFileScanner thirdColumn = new CsvFileScanner(inputSupplier, 2, ',', VARIABLE_BINARY);
        Assert.assertEquals(ImmutableList.copyOf(new PairsIterator(thirdColumn.iterator())),
                ImmutableList.of(
                        new Pair(0, createTuple("alice")),
                        new Pair(1, createTuple("bob")),
                        new Pair(2, createTuple("charlie")),
                        new Pair(3, createTuple("dave"))));
    }

    private Tuple createTuple(String value)
    {
        TupleInfo tupleInfo = new TupleInfo(VARIABLE_BINARY);
        Tuple tuple = tupleInfo.builder()
                .append(Slices.wrappedBuffer(value.getBytes(UTF_8)))
                .build();

        return tuple;
    }

    private Tuple createTuple(long value)
    {
        TupleInfo tupleInfo = new TupleInfo(FIXED_INT_64);
        Tuple tuple = tupleInfo.builder()
                .append(value)
                .build();

        return tuple;
    }


}
