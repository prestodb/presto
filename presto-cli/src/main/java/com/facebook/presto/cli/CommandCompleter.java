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
package com.facebook.presto.cli;

import com.google.common.collect.ImmutableList;
import jline.console.completer.Completer;

import java.io.Closeable;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class CommandCompleter
        implements Completer, Closeable
{
    private SortedSet<String> commands;

    CommandCompleter()
    {
        // Taken from the presto documentation at http://prestodb.io/docs/current/sql.html
        commands = new TreeSet<String>();
        commands.add("SELECT");
        commands.add("SHOW CATALOGS");
        commands.add("SHOW COLUMNS");
        commands.add("SHOW FUNCTIONS");
        commands.add("SHOW PARTITIONS");
        commands.add("SHOW SCHEMAS");
        commands.add("SHOW TABLES");
        commands.add("CREATE TABLE");
        commands.add("DROP TABLE");
        commands.add("EXPLAIN");
        commands.add("DESCRIBE");
        commands.add("USE CATALOG");
        commands.add("USE SCHEMA");
        commands.add("HELP");
        commands.add("QUIT");
        // Add the lowercase values of the same commands
        SortedSet<String> copiedCommands = new TreeSet<String>(commands);
        for (String copiedCommand : copiedCommands) {
            commands.add(copiedCommand.toLowerCase());
        }
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates)
    {
        if (buffer == null) {
            candidates.addAll(commands);
        }
        else {
            for (String match : commands.tailSet(buffer)) {
                if (!match.startsWith(buffer)) {
                    break;
                }

                candidates.add(match);
            }
        }

        if (candidates.size() == 1) {
            candidates.set(0, candidates.get(0) + " ");
        }

        return candidates.isEmpty() ? -1 : 0;
    }

    private static List<String> filterResults(List<String> values, String prefix)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (String value : values) {
            if (value.startsWith(prefix)) {
                builder.add(value);
            }
        }
        return builder.build();
    }

    @Override
    public void close()
    {
    }
}
