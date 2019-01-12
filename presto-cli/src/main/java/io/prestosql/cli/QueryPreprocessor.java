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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import io.airlift.units.Duration;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Throwables.propagateIfPossible;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.prestosql.cli.ConsolePrinter.REAL_TERMINAL;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class QueryPreprocessor
{
    public static final String ENV_PREPROCESSOR = "PRESTO_PREPROCESSOR";
    public static final String ENV_PREPROCESSOR_TIMEOUT = "PRESTO_PREPROCESSOR_TIMEOUT";
    public static final String ENV_PRESTO_CATALOG = "PRESTO_CATALOG";
    public static final String ENV_PRESTO_SCHEMA = "PRESTO_SCHEMA";
    private static final Duration DEFAULT_PREPROCESSOR_TIMEOUT = new Duration(10, SECONDS);

    private static final Signal SIGINT = new Signal("INT");
    private static final String PREPROCESSING_QUERY_MESSAGE = "Preprocessing query...";

    private QueryPreprocessor() {}

    public static String preprocessQuery(Optional<String> catalog, Optional<String> schema, String query)
            throws QueryPreprocessorException
    {
        Duration timeout = DEFAULT_PREPROCESSOR_TIMEOUT;
        String timeoutEnvironment = nullToEmpty(System.getenv(ENV_PREPROCESSOR_TIMEOUT)).trim();
        if (!timeoutEnvironment.isEmpty()) {
            timeout = Duration.valueOf(timeoutEnvironment);
        }

        String preprocessorCommand = System.getenv(ENV_PREPROCESSOR);
        if (emptyToNull(preprocessorCommand) == null) {
            return query;
        }
        return preprocessQuery(catalog, schema, query, ImmutableList.of("/bin/sh", "-c", preprocessorCommand), timeout);
    }

    public static String preprocessQuery(Optional<String> catalog, Optional<String> schema, String query, List<String> preprocessorCommand, Duration timeout)
            throws QueryPreprocessorException
    {
        Thread clientThread = Thread.currentThread();
        SignalHandler oldHandler = Signal.handle(SIGINT, signal -> clientThread.interrupt());
        try {
            if (REAL_TERMINAL) {
                System.out.print(PREPROCESSING_QUERY_MESSAGE);
                System.out.flush();
            }
            return preprocessQueryInternal(catalog, schema, query, preprocessorCommand, timeout);
        }
        finally {
            if (REAL_TERMINAL) {
                System.out.print("\r" + Strings.repeat(" ", PREPROCESSING_QUERY_MESSAGE.length()) + "\r");
                System.out.flush();
            }
            Signal.handle(SIGINT, oldHandler);
            Thread.interrupted(); // clear interrupt status
        }
    }

    private static String preprocessQueryInternal(Optional<String> catalog, Optional<String> schema, String query, List<String> preprocessorCommand, Duration timeout)
            throws QueryPreprocessorException
    {
        // execute the process in a child thread so we can better handle interruption and timeouts
        AtomicReference<Process> processReference = new AtomicReference<>();

        Future<String> task = executeInNewThread("Query preprocessor", () -> {
            String result;
            int exitCode;
            Future<String> readStderr;
            try {
                ProcessBuilder processBuilder = new ProcessBuilder(preprocessorCommand);
                processBuilder.environment().put(ENV_PRESTO_CATALOG, catalog.orElse(""));
                processBuilder.environment().put(ENV_PRESTO_SCHEMA, schema.orElse(""));

                Process process = processBuilder.start();
                processReference.set(process);

                Future<?> writeOutput = null;
                try {
                    // write query to process standard out
                    writeOutput = executeInNewThread("Query preprocessor output", () -> {
                        try (OutputStream outputStream = process.getOutputStream()) {
                            outputStream.write(query.getBytes(UTF_8));
                        }
                        return null;
                    });

                    // read stderr
                    readStderr = executeInNewThread("Query preprocessor read stderr", () -> {
                        StringBuilder builder = new StringBuilder();
                        try (InputStream inputStream = process.getErrorStream()) {
                            CharStreams.copy(new InputStreamReader(inputStream, UTF_8), builder);
                        }
                        catch (IOException | RuntimeException ignored) {
                        }
                        return builder.toString();
                    });

                    // read response
                    try (InputStream inputStream = process.getInputStream()) {
                        result = CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
                    }

                    // verify output was written successfully
                    try {
                        writeOutput.get();
                    }
                    catch (ExecutionException e) {
                        throw e.getCause();
                    }

                    // wait for process to finish
                    exitCode = process.waitFor();
                }
                finally {
                    process.destroyForcibly();
                    if (writeOutput != null) {
                        writeOutput.cancel(true);
                    }
                }
            }
            catch (QueryPreprocessorException e) {
                throw e;
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new QueryPreprocessorException("Interrupted while preprocessing query");
            }
            catch (Throwable e) {
                throw new QueryPreprocessorException("Error preprocessing query: " + e.getMessage(), e);
            }

            // check we got a valid exit code
            if (exitCode != 0) {
                Optional<String> errorMessage = tryGetFutureValue(readStderr, 100, MILLISECONDS)
                        .flatMap(value -> Optional.ofNullable(emptyToNull(value.trim())));

                throw new QueryPreprocessorException("Query preprocessor exited " + exitCode +
                        errorMessage.map(message1 -> "\n===\n" + message1 + "\n===").orElse(""));
            }
            return result;
        });

        try {
            return task.get(timeout.toMillis(), MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueryPreprocessorException("Interrupted while preprocessing query");
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            propagateIfPossible(cause, QueryPreprocessorException.class);
            throw new QueryPreprocessorException("Error preprocessing query: " + cause.getMessage(), cause);
        }
        catch (TimeoutException e) {
            throw new QueryPreprocessorException("Timed out waiting for query preprocessor after " + timeout);
        }
        finally {
            Process process = processReference.get();
            if (process != null) {
                process.destroyForcibly();
            }
            task.cancel(true);
        }
    }

    private static <T> Future<T> executeInNewThread(String threadName, Callable<T> callable)
    {
        FutureTask<T> task = new FutureTask<>(callable);
        Thread thread = new Thread(task);
        thread.setName(threadName);
        thread.setDaemon(true);
        thread.start();
        return task;
    }
}
