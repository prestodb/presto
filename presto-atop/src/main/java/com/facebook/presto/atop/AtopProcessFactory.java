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
package com.facebook.presto.atop;

import com.facebook.presto.spi.PrestoException;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.atop.AtopErrorCode.ATOP_CANNOT_START_PROCESS_ERROR;
import static com.facebook.presto.atop.AtopErrorCode.ATOP_READ_TIMEOUT;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class AtopProcessFactory
        implements AtopFactory
{
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("YYYYMMdd");
    private final String executablePath;
    private final DateTimeZone timeZone;
    private final Duration readTimeout;
    private final ExecutorService executor;

    @Inject
    public AtopProcessFactory(AtopConnectorConfig config, AtopConnectorId connectorId)
    {
        this.executablePath = config.getExecutablePath();
        this.timeZone = config.getDateTimeZone();
        this.readTimeout = config.getReadTimeout();
        this.executor = newFixedThreadPool(config.getConcurrentReadersPerNode(), daemonThreadsNamed("atop-" + connectorId + "executable-reader-%s"));
    }

    @Override
    public Atop create(AtopTable table, DateTime date)
    {
        checkArgument(date.getZone().equals(timeZone), "Split date (%s) is not in the local timezone (%s)", date.getZone(), timeZone);

        ProcessBuilder processBuilder = new ProcessBuilder(executablePath);
        processBuilder.command().add("-P");
        processBuilder.command().add(table.getAtopLabel());
        processBuilder.command().add("-r");
        processBuilder.command().add(DATE_FORMATTER.print(date));
        Process process;
        try {
            process = processBuilder.start();
        }
        catch (IOException e) {
            throw new PrestoException(ATOP_CANNOT_START_PROCESS_ERROR, format("Cannot start %s", processBuilder.command()), e);
        }
        return new AtopProcess(process, readTimeout, executor);
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }

    private static final class AtopProcess
            implements Atop
    {
        private final Process process;
        private final BufferedReader underlyingReader;
        private final LineReader reader;
        private String line;

        private AtopProcess(Process process, Duration readTimeout, ExecutorService executor)
        {
            this.process = requireNonNull(process, "process is null");
            underlyingReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            TimeLimiter limiter = new SimpleTimeLimiter(executor);
            this.reader = limiter.newProxy(underlyingReader::readLine, LineReader.class, readTimeout.toMillis(), MILLISECONDS);
            try {
                // Ignore the first two lines, as they are an event since boot (RESET followed by event line)
                this.reader.readLine();
                this.reader.readLine();
                // Read the first real line
                line = this.reader.readLine();
            }
            catch (IOException e) {
                line = null;
            }
            catch (UncheckedTimeoutException e) {
                throw new PrestoException(ATOP_READ_TIMEOUT, "Timeout reading from atop process");
            }
        }

        @Override
        public boolean hasNext()
        {
            return line != null;
        }

        @Override
        public String next()
        {
            if (line == null) {
                throw new NoSuchElementException();
            }
            String currentLine = line;
            try {
                line = reader.readLine();
            }
            catch (IOException e) {
                line = null;
            }
            catch (UncheckedTimeoutException e) {
                throw new PrestoException(ATOP_READ_TIMEOUT, "Timeout reading from atop process");
            }

            return currentLine;
        }

        @Override
        public void close()
        {
            try {
                underlyingReader.close();
            }
            catch (IOException e) {
                // Ignored
            }
            finally {
                process.destroy();
                try {
                    if (!process.waitFor(5, TimeUnit.SECONDS)) {
                        process.destroyForcibly();
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    process.destroyForcibly();
                }
            }
        }
    }

    public interface LineReader
    {
        String readLine()
                throws IOException;
    }
}
