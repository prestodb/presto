package com.facebook.presto.cli;

import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.history.History;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

public class LineReader
        extends ConsoleReader
        implements Closeable
{
    private boolean interrupted;

    LineReader(History history,
            TableNameCompleter tableNameCompleter)
            throws IOException
    {
        setExpandEvents(false);
        setBellEnabled(true);
        setHandleUserInterrupt(true);
        setHistory(history);
        setHistoryEnabled(false);
        addCompleter(tableNameCompleter);
    }

    @Override
    public String readLine(String prompt, Character mask)
            throws IOException
    {
        String line;
        interrupted = false;
        try {
            line = super.readLine(prompt, mask);
        }
        catch (UserInterruptException e) {
            interrupted = true;
            return null;
        }

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

    public boolean interrupted()
    {
        return interrupted;
    }
}
