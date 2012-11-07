/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.cli.ConvertCsv;
import com.facebook.presto.cli.DemoQuery2;
import com.facebook.presto.cli.DemoQuery3;
import com.facebook.presto.cli.ExampleSumAggregation;
import com.facebook.presto.cli.Execute;
import com.facebook.presto.cli.LocalQueryCommand;
import com.facebook.presto.cli.Server;
import com.facebook.presto.ingest.RuntimeIOException;
import com.google.common.io.NullOutputStream;
import io.airlift.command.Cli;
import io.airlift.command.Cli.CliBuilder;
import io.airlift.command.Help;
import io.airlift.log.Logging;
import io.airlift.log.LoggingConfiguration;

import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.Callable;

public class Main
{
    public static void main(String[] args)
            throws Exception
    {
        CliBuilder<Callable<Void>> builder = (CliBuilder<Callable<Void>>) (Object) Cli.buildCli("presto", Callable.class)
                .withDefaultCommand(Help.class)
                .withCommand(Server.class)
                .withCommand(ExampleSumAggregation.class)
                .withCommand(Execute.class)
                .withCommand(DemoQuery2.class)
                .withCommand(DemoQuery3.class)
                .withCommand(LocalQueryCommand.class)
                .withCommands(Help.class);

        builder.withGroup("example")
                .withDescription("run example queries")
                .withDefaultCommand(Help.class)
                .withCommand(ExampleSumAggregation.class);

        builder.withGroup("convert")
                .withDescription("convert file formats")
                .withDefaultCommand(Help.class)
                .withCommand(ConvertCsv.class);

        Cli<Callable<Void>> cli = builder.build();

        cli.parse(args).call();
    }

    public static class BaseCommand
            implements Callable<Void>
    {
        @Override
        public Void call()
                throws Exception
        {
            run();
            return null;
        }

        public void run()
                throws Exception
        {
            System.out.println(getClass().getSimpleName());
        }
    }


    public static void initializeLogging(boolean debug)
    {
        // unhook out and err while initializing logging or logger will print to them
        PrintStream out = System.out;
        PrintStream err = System.err;
        try {
            if (debug) {
                Logging logging = new Logging();
                logging.initialize(new LoggingConfiguration());
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
