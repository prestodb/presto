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

import com.facebook.presto.client.ClientSession;
import com.google.common.io.Files;
import org.fusesource.jansi.AnsiConsole;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.command.SingleCommand.singleCommand;
import static java.lang.String.format;

public final class Presto
{
    private Presto() {}

    public static void main(String[] args)
            throws Exception
    {
        Console console = singleCommand(Console.class).parse(args);

        if (console.helpOption.showHelpIfRequested() ||
                console.versionOption.showVersionIfRequested()) {
            return;
        }

        ClientOptions clientOptions = console.clientOptions;

        boolean hasQuery = !isNullOrEmpty(clientOptions.execute);
        boolean isFromFile = !isNullOrEmpty(clientOptions.file);

        if (!hasQuery || !isFromFile) {
            AnsiConsole.systemInstall();
        }

        String query = clientOptions.execute;
        if (hasQuery) {
            query += ";";
        }

        if (isFromFile) {
            if (hasQuery) {
                throw new RuntimeException("both --execute and --file specified");
            }
            try {
                query = Files.toString(new File(clientOptions.file), UTF_8);
                hasQuery = true;
            }
            catch (IOException e) {
                throw new RuntimeException(format("Error reading from file %s: %s", clientOptions.file, e.getMessage()));
            }
        }

        ClientSession session = clientOptions.toClientSession();
        try (QueryRunner queryRunner = QueryRunner.create(session, Optional.ofNullable(clientOptions.socksProxy))) {
            if (hasQuery) {
                System.exit(console.executeCommand(queryRunner, query, clientOptions.outputFormat));
            }
            else {
                console.run();
            }
        }
    }
}
