package com.facebook.presto.cli;

import com.facebook.presto.Main;
import io.airlift.command.Command;
import io.airlift.command.Option;
import jline.console.ConsoleReader;
import jline.console.history.FileHistory;
import jline.console.history.History;
import jline.console.history.MemoryHistory;
import org.fusesource.jansi.AnsiConsole;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.net.URI;

import static jline.internal.Configuration.getUserHome;

@Command(name = "console", description = "Interactive console")
public class Console
        implements Runnable
{
    @Option(name = "-s", title = "server")
    public URI server = URI.create("http://localhost:8080");

    @Option(name = "--debug", title = "debug")
    public boolean debug = false;

    @Override
    public void run()
    {
        AnsiConsole.systemInstall();
        Main.initializeLogging(debug);

        try (QueryRunner queryRunner = QueryRunner.create(server, debug)) {
            runConsole(queryRunner);
        }
    }

    private void runConsole(QueryRunner queryRunner)
    {
        try (LineReader reader = new LineReader(getHistory())) {
            while (true) {
                String line = reader.readLine("presto> ");
                if (line == null) {
                    return;
                }

                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                if (!process(queryRunner, line)) {
                    return;
                }
                reader.getHistory().add(line);
            }
        }
        catch (IOException e) {
            System.err.println("Readline error: " + e.getMessage());
        }
    }

    private boolean process(QueryRunner queryRunner, String line)
    {
        if (line.endsWith(";")) {
            line = line.substring(0, line.length() - 1);
        }

        if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
            return false;
        }

        try (Query query = queryRunner.startQuery(line)) {
            query.run(System.out);
        }
        catch (QueryAbortedException e) {
            System.out.println("(query aborted by user)");
            System.out.println();
        }
        catch (Exception e) {
            System.out.println("Error running command: " + e.getMessage());
            if (debug) {
                e.printStackTrace();
            }
        }

        return true;
    }

    private static MemoryHistory getHistory()
    {
        MemoryHistory history;
        File historyFile = new File(getUserHome(), ".presto_history");
        try {
            history = new FileHistory(historyFile);
        }
        catch (IOException e) {
            System.err.printf("WARNING: Failed to load history file (%s): %s. " +
                    "History will not be available during this session.%n",
                    historyFile, e.getMessage());
            history = new MemoryHistory();
        }
        history.setAutoTrim(true);
        return history;
    }

    private static class LineReader
            extends ConsoleReader
            implements Closeable
    {
        private LineReader(History history)
                throws IOException
        {
            setExpandEvents(false);
            setHistory(history);
            setHistoryEnabled(false);
        }

        @Override
        public String readLine(String prompt, Character mask)
                throws IOException
        {
            String line = super.readLine(prompt, mask);
            if (getHistory() instanceof Flushable) {
                ((Flushable) getHistory()).flush();
            }
            return line;
        }

        @Override
        public void close()
        {
            shutdown();
        }
    }
}
