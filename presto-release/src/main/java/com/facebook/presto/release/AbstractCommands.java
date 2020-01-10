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
package com.facebook.presto.release;

import com.facebook.airlift.log.Logger;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.Files.asCharSource;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public abstract class AbstractCommands
{
    private static final Logger log = Logger.get(AbstractCommands.class);

    private final String executable;
    private final Map<String, String> environment;
    private final File directory;

    public AbstractCommands(String executable, Map<String, String> environment, File directory)
    {
        this.executable = requireNonNull(executable, "executable is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.directory = requireNonNull(directory, "directory is null");
    }

    protected String command(String... arguments)
    {
        return command(asList(arguments));
    }

    protected String command(List<String> arguments)
    {
        return command(
                ImmutableList.<String>builder()
                        .add(executable)
                        .addAll(arguments)
                        .build(),
                environment,
                directory);
    }

    protected static String command(List<String> command, Map<String, String> environment, File workingDirectory)
    {
        String commandLine = Joiner.on(" ").join(command);

        try {
            File logFile = Files.createTempFile("presto-release-log", "").toFile();
            log.info(format("Running Command: %s; Log: %s", commandLine, logFile.getAbsolutePath()));

            ProcessBuilder processBuilder = new ProcessBuilder(command);
            Map<String, String> processEnvironment = processBuilder.environment();
            environment.forEach(processEnvironment::put);
            Process process = processBuilder.directory(workingDirectory).redirectOutput(logFile).start();

            process.waitFor();

            if (process.exitValue() != 0) {
                readFromStream(process.getErrorStream()).ifPresent(log::error);
                throw new RuntimeException("Command failed");
            }

            process.destroy();
            process.waitFor();

            log.info(format("Finished running command: %s", commandLine));
            return asCharSource(logFile, UTF_8).read();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e) {
            currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static Optional<String> readFromStream(InputStream stream)
    {
        try {
            return Optional.of(CharStreams.toString(new InputStreamReader(stream, UTF_8)).trim());
        }
        catch (IOException t) {
            return Optional.empty();
        }
    }
}
