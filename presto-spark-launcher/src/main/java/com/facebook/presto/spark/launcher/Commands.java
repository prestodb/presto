/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spark.launcher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.airline.DefaultCommandFactory;
import io.airlift.airline.ParseState;
import io.airlift.airline.Parser;
import io.airlift.airline.SingleCommand;
import io.airlift.airline.model.CommandMetadata;

import static io.airlift.airline.ParserUtil.createInstance;

public class Commands
{
    private Commands() {}

    public static PrestoSparkLauncherCommand parseCommandNoValidate(SingleCommand<PrestoSparkLauncherCommand> command, String[] args)
    {
        CommandMetadata commandMetadata = command.getCommandMetadata();
        Parser parser = new Parser();
        ParseState state = parser.parseCommand(commandMetadata, ImmutableList.copyOf(args));
        return createInstance(
                PrestoSparkLauncherCommand.class,
                commandMetadata.getAllOptions(),
                state.getParsedOptions(),
                commandMetadata.getArguments(),
                state.getParsedArguments(),
                commandMetadata.getMetadataInjections(),
                ImmutableMap.of(CommandMetadata.class, commandMetadata),
                new DefaultCommandFactory<>());
    }
}
