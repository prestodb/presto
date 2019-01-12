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
package io.prestosql.cli;

import com.google.common.collect.ImmutableSet;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;

import java.util.Locale;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public final class Completion
{
    private static final Set<String> COMMANDS = ImmutableSet.of(
            "SELECT",
            "SHOW CATALOGS",
            "SHOW COLUMNS",
            "SHOW FUNCTIONS",
            "SHOW SCHEMAS",
            "SHOW SESSION",
            "SHOW TABLES",
            "CREATE TABLE",
            "DROP TABLE",
            "EXPLAIN",
            "DESCRIBE",
            "USE",
            "HELP",
            "QUIT");

    private Completion() {}

    public static Completer commandCompleter()
    {
        return new StringsCompleter(COMMANDS);
    }

    // TODO: create a case-insensitive completer
    public static Completer lowerCaseCommandCompleter()
    {
        return new StringsCompleter(COMMANDS.stream()
                .map(s -> s.toLowerCase(Locale.ENGLISH))
                .collect(toSet()));
    }
}
