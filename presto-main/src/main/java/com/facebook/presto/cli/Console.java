package com.facebook.presto.cli;

import com.facebook.presto.Main;
import com.facebook.presto.sql.parser.StatementSplitter;
import com.google.common.base.Strings;
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

import static com.facebook.presto.sql.parser.StatementSplitter.squeezeStatement;
import static jline.internal.Configuration.getUserHome;

@Command(name = "console", description = "Interactive console")
public class Console
        implements Runnable
{
    private static final String PROMPT_NAME = "presto";

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
            StringBuilder buffer = new StringBuilder();
            while (true) {
                // read a line of input from user
                String prompt = PROMPT_NAME;
                if (buffer.length() > 0) {
                    prompt = Strings.repeat(" ", prompt.length() - 1) + "-";
                }
                String line = reader.readLine(prompt + "> ");
                if (line == null) {
                    return;
                }

                // check for special commands if this is the first line
                if (buffer.length() == 0) {
                    String command = line.trim();
                    if (command.endsWith(";")) {
                        command = command.substring(0, command.length() - 1).trim();
                    }
                    switch (command.toLowerCase()) {
                        case "exit":
                        case "quit":
                            return;
                    }
                }

                // not a command, add line to buffer
                buffer.append(line).append("\n");

                // execute any complete statements
                StatementSplitter splitter = new StatementSplitter(buffer.toString());
                for (String sql : splitter.getCompleteStatements()) {
                    process(queryRunner, sql);
                    reader.getHistory().add(squeezeStatement(sql) + ";");
                }

                // replace buffer with trailing partial statement
                buffer = new StringBuilder();
                String partial = splitter.getPartialStatement();
                if (!partial.isEmpty()) {
                    buffer.append(partial).append('\n');
                }
            }
        }
        catch (IOException e) {
            System.err.println("Readline error: " + e.getMessage());
        }
    }

    private void process(QueryRunner queryRunner, String sql)
    {
        try (Query query = queryRunner.startQuery(sql)) {
            query.renderOutput(System.out);
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
