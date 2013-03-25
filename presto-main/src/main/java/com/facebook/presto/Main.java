/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.cli.Console;
import io.airlift.command.Cli;
import io.airlift.command.Cli.CliBuilder;
import io.airlift.command.Help;

public final class Main
{
    public static void main(String[] args)
            throws Exception
    {
        CliBuilder<Runnable> builder = Cli.<Runnable>builder("presto")
                .withDefaultCommand(Console.class)
                .withCommand(Console.class)
                .withCommand(Help.class);

        Cli<Runnable> cli = builder.build();
        cli.parse(args).run();
    }
}
