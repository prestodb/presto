package com.facebook.presto.ingest;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Blocks;
import com.google.common.collect.ImmutableList;
import com.google.common.io.InputSupplier;
import org.testng.annotations.Test;

import java.io.InputStreamReader;
import java.util.List;

import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.block.Blocks.assertBlockStreamEquals;
import static com.facebook.presto.ingest.CsvReader.csvNumericColumn;
import static com.facebook.presto.ingest.CsvReader.csvStringColumn;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.newReaderSupplier;

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

            assertBlockStreamEquals(processors.get(0).getTupleStream(),
                    Blocks.createLongsBlockStream(0, 0, 1, 2, 3));

            assertBlockStreamEquals(processors.get(1).getTupleStream(),
                    Blocks.createBlockStream(0, "apple", "banana", "cherry", "date"));

            assertBlockStreamEquals(processors.get(2).getTupleStream(),
                    Blocks.createBlockStream(0, "alice", "bob", "charlie", "dave"));
        }
    }
}
