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
package io.prestosql.benchmark.driver;

import com.google.common.collect.ImmutableList;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.log.Level;
import io.airlift.log.Logging;
import io.airlift.log.LoggingConfiguration;
import io.prestosql.client.ClientSession;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.io.ByteStreams.nullOutputStream;
import static io.airlift.airline.SingleCommand.singleCommand;
import static java.util.function.Function.identity;

@Command(name = "presto-benchmark", description = "Presto benchmark driver")
public class PrestoBenchmarkDriver
{
    @Inject
    public HelpOption helpOption;

    @Inject
    public BenchmarkDriverOptions benchmarkDriverOptions = new BenchmarkDriverOptions();

    public static void main(String[] args)
            throws Exception
    {
        new PrestoBenchmarkDriver().run(args);
    }

    protected void run(String[] args)
            throws Exception
    {
        PrestoBenchmarkDriver prestoBenchmarkDriver = singleCommand(PrestoBenchmarkDriver.class).parse(args);

        if (prestoBenchmarkDriver.helpOption.showHelpIfRequested()) {
            return;
        }

        BenchmarkDriverOptions driverOptions = prestoBenchmarkDriver.benchmarkDriverOptions;

        initializeLogging(driverOptions.debug);

        // select suites
        List<Suite> suites = Suite.readSuites(new File(driverOptions.suiteConfigFile));
        if (!driverOptions.suites.isEmpty()) {
            suites = suites.stream()
                    .filter(suite -> driverOptions.suites.contains(suite.getName()))
                    .collect(Collectors.toList());
        }
        suites = ImmutableList.copyOf(suites);

        // load queries
        File queriesDir = new File(driverOptions.sqlTemplateDir);
        List<BenchmarkQuery> allQueries = readQueries(queriesDir);

        // select queries to run
        Set<BenchmarkQuery> queries;
        if (driverOptions.queries.isEmpty()) {
            queries = suites.stream()
                    .map(suite -> suite.selectQueries(allQueries))
                    .flatMap(List::stream)
                    .collect(Collectors.toSet());
        }
        else {
            queries = driverOptions.queries.stream()
                    .map(Pattern::compile)
                    .map(pattern -> allQueries.stream().filter(query -> pattern.matcher(query.getName()).matches()))
                    .flatMap(identity())
                    .collect(Collectors.toSet());
        }

        // create results store
        BenchmarkResultsStore resultsStore = getResultsStore(suites, queries);

        // create session
        ClientSession session = driverOptions.getClientSession();

        try (BenchmarkDriver benchmarkDriver = new BenchmarkDriver(
                resultsStore,
                session,
                queries,
                driverOptions.warm,
                driverOptions.runs,
                driverOptions.debug,
                driverOptions.maxFailures,
                Optional.ofNullable(driverOptions.socksProxy))) {
            for (Suite suite : suites) {
                benchmarkDriver.run(suite);
            }
        }
    }

    protected BenchmarkResultsStore getResultsStore(List<Suite> suites, Set<BenchmarkQuery> queries)
    {
        return new BenchmarkResultsPrinter(suites, queries);
    }

    private static List<BenchmarkQuery> readQueries(File queriesDir)
            throws IOException
    {
        File[] files = queriesDir.listFiles();
        if (files == null) {
            return ImmutableList.of();
        }
        Arrays.sort(files);

        ImmutableList.Builder<BenchmarkQuery> queries = ImmutableList.builder();
        for (File file : files) {
            String fileName = file.getName();
            if (fileName.endsWith(".sql")) {
                queries.add(new BenchmarkQuery(file));
            }
        }
        return queries.build();
    }

    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    public static void initializeLogging(boolean debug)
    {
        // unhook out and err while initializing logging or logger will print to them
        PrintStream out = System.out;
        PrintStream err = System.err;
        try {
            if (debug) {
                Logging logging = Logging.initialize();
                logging.configure(new LoggingConfiguration());
                logging.setLevel("io.prestosql", Level.DEBUG);
            }
            else {
                System.setOut(new PrintStream(nullOutputStream()));
                System.setErr(new PrintStream(nullOutputStream()));

                Logging logging = Logging.initialize();
                logging.configure(new LoggingConfiguration());
                logging.disableConsole();
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        finally {
            System.setOut(out);
            System.setErr(err);
        }
    }
}
