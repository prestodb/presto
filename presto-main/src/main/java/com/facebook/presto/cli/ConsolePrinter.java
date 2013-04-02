package com.facebook.presto.cli;

import jline.TerminalFactory;

import java.io.PrintStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.fusesource.jansi.Ansi.Erase;
import static org.fusesource.jansi.Ansi.ansi;
import static org.fusesource.jansi.internal.CLibrary.STDOUT_FILENO;
import static org.fusesource.jansi.internal.CLibrary.isatty;

public class ConsolePrinter
{
    public static final boolean REAL_TERMINAL = detectRealTerminal();

    private final PrintStream out;
    private int lines;

    public ConsolePrinter(PrintStream out)
    {
        this.out = checkNotNull(out, "out is null");
    }

    public void reprintLine(String line)
    {
        if (isRealTerminal()) {
            out.print(ansi().eraseLine(Erase.ALL).a(line).a('\n').toString());
        }
        else {
            out.print('\r' + line);
        }
        out.flush();
        lines++;
    }

    public void repositionCursor()
    {
        if (lines > 0) {
            if (isRealTerminal()) {
                out.print(ansi().cursorUp(lines).toString());
            }
            else {
                out.print('\r');
            }
            out.flush();
            lines = 0;
        }
    }

    public void resetScreen()
    {
        if (lines > 0) {
            if (isRealTerminal()) {
                out.print(ansi().cursorUp(lines).eraseScreen(Erase.FORWARD).toString());
            }
            else {
                out.print('\r');
            }
            out.flush();
            lines = 0;
        }
    }

    public int getWidth()
    {
        return TerminalFactory.get().getWidth();
    }

    @SuppressWarnings("MethodMayBeStatic")
    public boolean isRealTerminal()
    {
        return REAL_TERMINAL;
    }

    private static boolean detectRealTerminal()
    {
        // If the jansi.passthrough property is set, then don't interpret
        // any of the ansi sequences.
        if (Boolean.parseBoolean(System.getProperty("jansi.passthrough"))) {
            return true;
        }

        // If the jansi.strip property is set, then we just strip the
        // the ansi escapes.
        if (Boolean.parseBoolean(System.getProperty("jansi.strip"))) {
            return false;
        }

        String os = System.getProperty("os.name");
        if (os.startsWith("Windows")) {
            // We could support this, but we'd need a windows box
            return true;
        }

        // We must be on some unix variant..
        try {
            // check if standard out is a terminal
            if (isatty(STDOUT_FILENO) == 0) {
                return false;
            }
        }
        catch (NoClassDefFoundError | UnsatisfiedLinkError ignore) {
            // These errors happen if the JNI lib is not available for your platform.
        }
        return true;
    }
}
