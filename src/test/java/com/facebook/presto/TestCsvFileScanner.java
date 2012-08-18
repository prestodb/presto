package com.facebook.presto;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.InputSupplier;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.InputStreamReader;

import static com.facebook.presto.SizeOf.SIZE_OF_LONG;
import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.newReaderSupplier;

public class TestCsvFileScanner
{
    private final InputSupplier<InputStreamReader> inputSupplier = newReaderSupplier(getResource("data.csv"), Charsets.UTF_8);

    @Test
    public void testIterator()
            throws Exception
    {
        CsvFileScanner firstColumn = new CsvFileScanner(inputSupplier, 0, ',', new TupleInfo(SIZE_OF_LONG));

        ImmutableList<Pair> actual = ImmutableList.copyOf(new PairsIterator(firstColumn.iterator()));
        Assert.assertEquals(actual,
                ImmutableList.of(
                        new Pair(0, createTuple(0)),
                        new Pair(1, createTuple(1)),
                        new Pair(2, createTuple(2)),
                        new Pair(3, createTuple(3))));

        // todo add support for variable length columns
//        CsvFileScanner secondColumn = new CsvFileScanner(inputSupplier, 1, ',');
//        Assert.assertEquals(ImmutableList.copyOf(new PairsIterator(secondColumn.iterator())),
//                ImmutableList.of(
//                        new Pair(0, new Tuple("apple")),
//                        new Pair(1, new Tuple("banana")),
//                        new Pair(2, new Tuple("cherry")),
//                        new Pair(3, new Tuple("date"))));
//
//        CsvFileScanner thirdColumn = new CsvFileScanner(inputSupplier, 2, ',');
//        Assert.assertEquals(ImmutableList.copyOf(new PairsIterator(thirdColumn.iterator())),
//                ImmutableList.of(
//                        new Pair(0, new Tuple("alice")),
//                        new Pair(1, new Tuple("bob")),
//                        new Pair(2, new Tuple("charlie")),
//                        new Pair(3, new Tuple("dave"))));
    }

    private Tuple createTuple(long value)
    {
        Slice slice = Slices.allocate(SIZE_OF_LONG);
        slice.setLong(0, value);
        return new Tuple(slice, new TupleInfo(SIZE_OF_LONG));
    }
}
