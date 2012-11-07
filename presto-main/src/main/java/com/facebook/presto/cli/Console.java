package com.facebook.presto.cli;

import com.facebook.presto.Main;
import io.airlift.command.Command;
import io.airlift.command.Option;
import jline.console.ConsoleReader;
import jline.console.history.MemoryHistory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;

@Command(name = "console", description = "Interactive console")
public class Console
        implements Runnable
{
    @Option(name = "-s", title = "server")
    public URI server = URI.create("http://localhost:8080/v1/presto/query");

    @Override
    public void run()
    {
        MemoryHistory history = new MemoryHistory();
        try (LineReader reader = new LineReader()) {
            reader.setHistory(history);

            while (true) {
                String line = reader.readLine("presto> ");
                if (line == null) {
                    return;
                }

                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                try {
                    if (!process(line)) {
                        return;
                    }
                }
                catch (Exception e) {
                    System.out.println("error running command");
                }
            }
        }
        catch (IOException e) {
            System.err.println("readline error: " + e.getMessage());
        }
    }

    private boolean process(String line)
    {
        if (line.endsWith(";")) {
            line = line.substring(0, line.length() - 1);
        }

        if (line.equalsIgnoreCase("quit")) {
            return false;
        }

        if (line.toLowerCase().startsWith("select ")) {
            line = "sql:" + line;
        }

        Main.Execute query = new Main.Execute();
        query.server = server;
        query.query = line;
        query.run();

        return true;
    }

    private static class LineReader
            extends ConsoleReader
            implements Closeable
    {
        private LineReader()
                throws IOException
        {
            super();
            setExpandEvents(false);
        }

        @Override
        public void close()
        {
            shutdown();
        }
    }
}
