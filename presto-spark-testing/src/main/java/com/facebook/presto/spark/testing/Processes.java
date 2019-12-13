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
package com.facebook.presto.spark.testing;

import com.facebook.airlift.log.Logger;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.ByteStreams.copy;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Processes
{
    private static final Logger log = Logger.get(Processes.class);

    private Processes() {}

    public static String executeForOutput(String... command)
            throws InterruptedException
    {
        return executeForOutput(ImmutableList.copyOf(command));
    }

    public static String executeForOutput(List<String> command)
            throws InterruptedException
    {
        String commandString = Joiner.on(" ").join(command);
        log.info("Running: %s", commandString);
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Process process = startProcess(processBuilder);
        closeInput(process);
        pipeStderr(commandString, process);
        String result;
        try (InputStream inputStream = process.getInputStream()) {
            result = CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        int exitCode = waitForProcess(process);
        checkState(exitCode == 0, "[%s] process existed with status: %s", commandString, exitCode);
        return result;
    }

    public static int execute(String... command)
            throws InterruptedException
    {
        return execute(ImmutableList.copyOf(command));
    }

    public static int execute(List<String> command)
            throws InterruptedException
    {
        log.info("Running: %s", Joiner.on(" ").join(command));
        return waitForProcess(startProcess(command));
    }

    public static int waitForProcess(Process process)
            throws InterruptedException
    {
        try {
            return process.waitFor();
        }
        catch (InterruptedException e) {
            destroyProcess(process);
            throw e;
        }
    }

    public static Process startProcess(List<String> command)
    {
        String commandString = Joiner.on(" ").join(command);
        log.info("Starting: %s", commandString);
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Process process = startProcess(processBuilder);
        closeInput(process);
        pipeStdout(commandString, process);
        pipeStderr(commandString, process);
        return process;
    }

    public static Process startProcess(ProcessBuilder processBuilder)
    {
        try {
            return processBuilder.start();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void destroyProcess(Process process)
    {
        if (!process.isAlive()) {
            return;
        }

        // stop
        process.destroy();

        // wait for 5 seconds
        for (int i = 0; i < 5; i++) {
            if (!process.isAlive()) {
                break;
            }
            sleepUninterruptibly(1, SECONDS);
        }

        // kill
        process.destroyForcibly();
    }

    private static void pipeStdout(String command, Process process)
    {
        pipe("PIPE STDOUT: " + command, process.getInputStream(), System.out);
    }

    private static void pipeStderr(String command, Process process)
    {
        pipe("PIPE STDERR: " + command, process.getErrorStream(), System.err);
    }

    private static void closeInput(Process process)
    {
        try {
            process.getOutputStream().close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void pipe(String name, InputStream input, OutputStream output)
    {
        Thread thread = new Thread(() -> {
            try {
                copy(input, output);
            }
            catch (IOException ignored) {
            }
        });
        thread.setName(name);
        thread.start();
    }
}
