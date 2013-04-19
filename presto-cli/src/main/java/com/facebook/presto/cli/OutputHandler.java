package com.facebook.presto.cli;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface OutputHandler
        extends Closeable
{
    void processRow(List<?> values)
            throws IOException;
}
