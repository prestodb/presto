package com.facebook.presto.cli;

import au.com.bytecode.opencsv.CSVWriter;
import com.facebook.presto.operator.OutputProcessor.OutputHandler;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.Writer;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class CSVPrinter
    extends OutputHandler
{
    public static final Function<Object, String> TO_STRING = new Function<Object, String>() {
        @Override
        public String apply(Object value) {
            return value == null ? "" : value.toString();
        }
    };

    private final CSVWriter writer;

    public CSVPrinter(Writer writer, char separator)
    {
        this.writer = new CSVWriter(checkNotNull(writer, "writer is null"), separator);
    }

    @Override
    public void processRow(List<?> values)
    {
        List<String> result = ImmutableList.copyOf(Lists.transform(values, TO_STRING));
        writer.writeNext(result.toArray(new String [result.size()]));
        if (writer.checkError()) {
            throw new RuntimeException("CSV stream busted!");
        }
    }
}
