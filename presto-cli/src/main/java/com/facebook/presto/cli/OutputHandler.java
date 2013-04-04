package com.facebook.presto.cli;

import com.facebook.presto.client.StatementClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public abstract class OutputHandler
        implements Closeable
{
    public static void processOutput(StatementClient client, OutputHandler handler)
            throws IOException
    {
        while (client.isValid()) {
            Iterable<List<Object>> data = client.current().getData();
            if (data != null) {
                for (List<Object> tuple : data) {
                    handler.processRow(Collections.unmodifiableList(tuple));
                }
            }
            client.advance();
        }
    }

    public abstract void processRow(List<?> values)
            throws IOException;
}
