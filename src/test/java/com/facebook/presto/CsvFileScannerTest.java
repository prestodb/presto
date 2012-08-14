package com.facebook.presto;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.InputSupplier;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.InputStreamReader;

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.newReaderSupplier;

public class CsvFileScannerTest
{
    private final InputSupplier<InputStreamReader> inputSupplier = newReaderSupplier(getResource("data.csv"), Charsets.UTF_8);

    @Test
    public void testIterator()
            throws Exception
    {
        CsvFileScanner firstColumn = new CsvFileScanner(inputSupplier, 0, ',');

        Assert.assertEquals(ImmutableList.copyOf(new PairsIterator(firstColumn.iterator())),
                ImmutableList.of(
                        new Pair(0, "0"),
                        new Pair(1, "1"),
                        new Pair(2, "2"),
                        new Pair(3, "3")));

        CsvFileScanner secondColumn = new CsvFileScanner(inputSupplier, 1, ',');
        Assert.assertEquals(ImmutableList.copyOf(new PairsIterator(secondColumn.iterator())),
                ImmutableList.of(
                        new Pair(0, "apple"),
                        new Pair(1, "banana"),
                        new Pair(2, "cherry"),
                        new Pair(3, "date")));

        CsvFileScanner thirdColumn = new CsvFileScanner(inputSupplier, 2, ',');
        Assert.assertEquals(ImmutableList.copyOf(new PairsIterator(thirdColumn.iterator())),
                ImmutableList.of(
                        new Pair(0, "alice"),
                        new Pair(1, "bob"),
                        new Pair(2, "charlie"),
                        new Pair(3, "dave")));
    }
}
