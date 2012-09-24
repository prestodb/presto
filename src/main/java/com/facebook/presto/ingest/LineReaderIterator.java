package com.facebook.presto.ingest;

import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.LineReader;

import java.io.IOException;

public class LineReaderIterator extends AbstractIterator<String>
{
    private final LineReader lineReader;

    public LineReaderIterator(LineReader lineReader)
    {
        this.lineReader = lineReader;
    }

    @Override
    protected String computeNext()
    {
        try {
            String line = lineReader.readLine();
            if (line == null) {
                return endOfData();
            }
            return line;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
