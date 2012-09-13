package com.facebook.presto.ingest;

import com.facebook.presto.ingest.RowSource;
import com.facebook.presto.ingest.RowSourceBuilder;
import com.facebook.presto.TupleInfo;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.InputSupplier;
import com.google.common.io.LineReader;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

import static com.facebook.presto.ingest.RowSourceBuilder.RowBuilder;
import static com.facebook.presto.ingest.RowSourceBuilder.RowGenerator;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class CsvReader
        implements InputSupplier<RowSource>
{
    public static interface CsvColumnProcessor
    {
        void process(String value, RowBuilder rowBuilder);
    }

    private final TupleInfo tupleInfo;
    private final InputSupplier<? extends Reader> inputSupplier;
    private final Splitter columnSplitter;
    private final List<CsvColumnProcessor> processors;

    public CsvReader(TupleInfo tupleInfo, InputSupplier<? extends Reader> inputSupplier,
            char columnDelimiter, List<CsvColumnProcessor> processors)
    {
        this.tupleInfo = checkNotNull(tupleInfo, "tupleInfo is null");
        this.inputSupplier = checkNotNull(inputSupplier, "inputSupplier is null");
        this.columnSplitter = Splitter.on(columnDelimiter);
        this.processors = ImmutableList.copyOf(checkNotNull(processors, "processors is null"));
    }

    @Override
    public RowSource getInput()
            throws IOException
    {
        return new RowSourceBuilder(tupleInfo, new CsvRowGenerator());
    }

    private class CsvRowGenerator
            implements RowGenerator
    {
        private final Reader reader;
        private final LineReader lineReader;

        private CsvRowGenerator()
                throws IOException
        {
            this.reader = inputSupplier.getInput();
            this.lineReader = new LineReader(reader);
        }

        @Override
        public boolean generate(RowBuilder rowBuilder)
        {
            String line = nextLine();
            if (line == null) {
                return false;
            }

            Iterable<String> split = columnSplitter.split(line);
            List<String> values = ImmutableList.copyOf(split);

            checkState(values.size() == processors.size(),
                    "line column count (%d) does not match processor count (%d)", values.size(), processors.size());

            for (int i = 0; i < values.size(); i++) {
                String value = values.get(i);
                CsvColumnProcessor processor = processors.get(i);
                processor.process(value, rowBuilder);
            }

            return true;
        }

        private String nextLine()
        {
            try {
                return lineReader.readLine();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void close()
                throws IOException
        {
            reader.close();
        }
    }

    public static CsvColumnProcessor csvNumericColumn()
    {
        return new CsvColumnProcessor()
        {
            @Override
            public void process(String value, RowBuilder rowBuilder)
            {
                rowBuilder.append(Long.valueOf(value));
            }
        };
    }

    public static CsvColumnProcessor csvDoubleColumn()
    {
        return new CsvColumnProcessor()
        {
            @Override
            public void process(String value, RowBuilder rowBuilder)
            {
                rowBuilder.append(Double.valueOf(value));
            }
        };
    }

    public static CsvColumnProcessor csvStringColumn()
    {
        return new CsvColumnProcessor()
        {
            @Override
            public void process(String value, RowBuilder rowBuilder)
            {
                rowBuilder.append(value.getBytes(Charsets.UTF_8));
            }
        };
    }
}
