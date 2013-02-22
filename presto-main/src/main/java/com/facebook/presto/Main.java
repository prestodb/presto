/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.cli.Console;
import com.facebook.presto.cli.Execute;
import com.facebook.presto.ingest.RuntimeIOException;
import com.google.common.io.NullOutputStream;
import io.airlift.command.Cli;
import io.airlift.command.Cli.CliBuilder;
import io.airlift.command.Help;
import io.airlift.log.Logging;
import io.airlift.log.LoggingConfiguration;
import io.airlift.log.LoggingMBean;

import java.io.IOException;
import java.io.PrintStream;

public class Main
{
    public static void main(String[] args)
            throws Exception
    {
        CliBuilder<Runnable> builder = Cli.<Runnable>builder("presto")
                .withDefaultCommand(Console.class)
                .withCommand(Execute.class)
                .withCommand(Console.class)
                .withCommand(Help.class);

        Cli<Runnable> cli = builder.build();
        cli.parse(args).run();
    }

    @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
    public static void initializeLogging(boolean debug)
    {
        // unhook out and err while initializing logging or logger will print to them
        PrintStream out = System.out;
        PrintStream err = System.err;
        try {
            if (debug) {
                Logging logging = new Logging();
                logging.initialize(new LoggingConfiguration());
                // TODO: add a proper interface to airlift
                new LoggingMBean().setLevel("com.facebook.presto", "DEBUG");
            }
            else {
                System.setOut(new PrintStream(new NullOutputStream()));
                System.setErr(new PrintStream(new NullOutputStream()));

                Logging logging = new Logging();
                logging.initialize(new LoggingConfiguration());
                logging.disableConsole();
            }
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
        finally {
            System.setOut(out);
            System.setErr(err);
        }
    }
}
