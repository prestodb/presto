package com.facebook.presto;

import com.google.common.collect.ImmutableList;
import com.google.common.io.InputSupplier;
import org.testng.annotations.Test;

import java.io.InputStreamReader;
import java.util.List;

import static com.facebook.presto.CsvReader.csvNumericColumn;
import static com.facebook.presto.CsvReader.csvStringColumn;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.Tuples.createTuple;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.newReaderSupplier;
import static org.testng.Assert.assertEquals;

public class TestCsvFileScanner
{
    private final InputSupplier<InputStreamReader> inputSupplier = newReaderSupplier(getResource("data.csv"), UTF_8);

    @Test
    public void testProcessCsv()
            throws Exception
    {
        TupleInfo tupleInfo = new TupleInfo(FIXED_INT_64, VARIABLE_BINARY, VARIABLE_BINARY);
        CsvReader csvReader = new CsvReader(tupleInfo, inputSupplier, ',',
                ImmutableList.of(csvNumericColumn(), csvStringColumn(), csvStringColumn()));

        try (RowSource rowSource0 = csvReader.getInput();
             RowSource rowSource1 = csvReader.getInput();
             RowSource rowSource2 = csvReader.getInput()) {
            List<CollectingColumnProcessor> processors = ImmutableList.of(
                    new CollectingColumnProcessor(FIXED_INT_64, 0, rowSource0.cursor()),
                    new CollectingColumnProcessor(VARIABLE_BINARY, 1, rowSource1.cursor()),
                    new CollectingColumnProcessor(VARIABLE_BINARY, 2, rowSource2.cursor()));

            ColumnProcessors.process(processors, 2);

            for (CollectingColumnProcessor processor : processors) {
                processor.finish();
            }

            assertEquals(ImmutableList.copyOf(getOnlyElement(processors.get(0).getBlockStream()).pairIterator()),
                    ImmutableList.of(
                            new Pair(0, createTuple(0)),
                            new Pair(1, createTuple(1)),
                            new Pair(2, createTuple(2)),
                            new Pair(3, createTuple(3))));

            assertEquals(ImmutableList.copyOf(getOnlyElement(processors.get(1).getBlockStream()).pairIterator()),
                    ImmutableList.of(
                            new Pair(0, createTuple("apple")),
                            new Pair(1, createTuple("banana")),
                            new Pair(2, createTuple("cherry")),
                            new Pair(3, createTuple("date"))));

            assertEquals(ImmutableList.copyOf(getOnlyElement(processors.get(2).getBlockStream()).pairIterator()),
                    ImmutableList.of(
                            new Pair(0, createTuple("alice")),
                            new Pair(1, createTuple("bob")),
                            new Pair(2, createTuple("charlie")),
                            new Pair(3, createTuple("dave"))));
        }
    }
}
