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

import com.facebook.presto.cli.ClientOptions.OutputFormat;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.sql.parser.IdentifierSymbol;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.parser.StatementSplitter;
import com.facebook.presto.sql.tree.Use;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.command.Command;
import io.airlift.command.HelpOption;
import io.airlift.log.Level;
import io.airlift.log.Logging;
import io.airlift.log.LoggingConfiguration;
import jline.console.history.FileHistory;
import jline.console.history.History;
import jline.console.history.MemoryHistory;
import org.fusesource.jansi.AnsiConsole;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.facebook.presto.cli.Completion.commandCompleter;
import static com.facebook.presto.cli.Completion.lowerCaseCommandCompleter;
import static com.facebook.presto.cli.Help.getHelpText;
import static com.facebook.presto.client.ClientSession.withProperties;
import static com.facebook.presto.sql.parser.StatementSplitter.Statement;
import static com.facebook.presto.sql.parser.StatementSplitter.isEmptyStatement;
import static com.facebook.presto.sql.parser.StatementSplitter.squeezeStatement;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static jline.internal.Configuration.getUserHome;

@Command(name = "presto", description = "Presto interactive console")
public class Console
        implements Runnable
{
    private static final String PROMPT_NAME = "presto";

    // create a parser with all identifier options enabled, since this is only used for USE statements
    private static final SqlParser SQL_PARSER = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(EnumSet.allOf(IdentifierSymbol.class)));

    private static final Pattern HISTORY_INDEX_PATTERN = Pattern.compile("!\\d+");

    @Inject
    public HelpOption helpOption;

    @Inject
    public VersionOption versionOption = new VersionOption();

    @Inject
    public ClientOptions clientOptions = new ClientOptions();

    @Override
    public void run()
    {
        ClientSession session = clientOptions.toClientSession();
        boolean hasQuery = !Strings.isNullOrEmpty(clientOptions.execute);
        boolean isFromFile = !Strings.isNullOrEmpty(clientOptions.file);

        if (!hasQuery || !isFromFile) {
            AnsiConsole.systemInstall();
        }

        initializeLogging(session.isDebug());

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

        try (QueryRunner queryRunner = QueryRunner.create(session, Optional.ofNullable(clientOptions.socksProxy))) {
            if (hasQuery) {
                executeCommand(queryRunner, query, clientOptions.outputFormat);
            }
            else {
                runConsole(queryRunner, session);
            }
        }
    }

    private static void runConsole(QueryRunner queryRunner, ClientSession session)
    {
        try (TableNameCompleter tableNameCompleter = new TableNameCompleter(queryRunner);
                LineReader reader = new LineReader(getHistory(), commandCompleter(), lowerCaseCommandCompleter(), tableNameCompleter)) {
            tableNameCompleter.populateCache();
            StringBuilder buffer = new StringBuilder();
            while (true) {
                // read a line of input from user
                String prompt = PROMPT_NAME + ":" + session.getSchema();
                if (buffer.length() > 0) {
                    prompt = Strings.repeat(" ", prompt.length() - 1) + "-";
                }
                String commandPrompt = prompt + "> ";
                String line = reader.readLine(commandPrompt);

                // add buffer to history and clear on user interrupt
                if (reader.interrupted()) {
                    String partial = squeezeStatement(buffer.toString());
                    if (!partial.isEmpty()) {
                        reader.getHistory().add(partial);
                    }
                    buffer = new StringBuilder();
                    continue;
                }

                // exit on EOF
                if (line == null) {
                    System.out.println();
                    return;
                }

                // check for special commands if this is the first line
                if (buffer.length() == 0) {
                    String command = line.trim();

                    if (HISTORY_INDEX_PATTERN.matcher(command).matches()) {
                        int historyIndex = parseInt(command.substring(1));
                        History history = reader.getHistory();
                        if ((historyIndex <= 0) || (historyIndex > history.index())) {
                            System.err.println("Command does not exist");
                            continue;
                        }
                        line = history.get(historyIndex - 1).toString();
                        System.out.println(commandPrompt + line);
                    }

                    if (command.endsWith(";")) {
                        command = command.substring(0, command.length() - 1).trim();
                    }

                    switch (command.toLowerCase(ENGLISH)) {
                        case "exit":
                        case "quit":
                            return;
                        case "history":
                            for (History.Entry entry : reader.getHistory()) {
                                System.out.printf("%5d  %s%n", entry.index() + 1, entry.value());
                            }
                            continue;
                        case "help":
                            System.out.println();
                            System.out.println(getHelpText());
                            continue;
                    }
                }

                // not a command, add line to buffer
                buffer.append(line).append("\n");

                // execute any complete statements
                String sql = buffer.toString();
                StatementSplitter splitter = new StatementSplitter(sql, ImmutableSet.of(";", "\\G"));
                for (Statement split : splitter.getCompleteStatements()) {
                    Optional<Object> statement = getParsedStatement(split.statement());
                    if (statement.isPresent() && isSessionParameterChange(statement.get())) {
                        session = processSessionParameterChange(statement.get(), session);
                        queryRunner.setSession(session);
                        tableNameCompleter.populateCache();
                    }
                    else {
                        OutputFormat outputFormat = OutputFormat.ALIGNED;
                        if (split.terminator().equals("\\G")) {
                            outputFormat = OutputFormat.VERTICAL;
                        }

                        process(queryRunner, split.statement(), outputFormat, true);
                    }
                    reader.getHistory().add(squeezeStatement(split.statement()) + split.terminator());
                }

                // replace buffer with trailing partial statement
                buffer = new StringBuilder();
                String partial = splitter.getPartialStatement();
                if (!partial.isEmpty()) {
                    buffer.append(partial).append('\n');
                }
            }
        }
        catch (IOException e) {
            System.err.println("Readline error: " + e.getMessage());
        }
    }

    private static Optional<Object> getParsedStatement(String statement)
    {
        try {
            return Optional.of((Object) SQL_PARSER.createStatement(statement));
        }
        catch (ParsingException e) {
            return Optional.empty();
        }
    }

    static ClientSession processSessionParameterChange(Object parsedStatement, ClientSession session)
    {
        if (parsedStatement instanceof Use) {
            Use use = (Use) parsedStatement;
            return ClientSession.withCatalogAndSchema(session, use.getCatalog().orElse(session.getCatalog()), use.getSchema());
        }
        return session;
    }

    private static boolean isSessionParameterChange(Object statement)
    {
        return statement instanceof Use;
    }

    private static void executeCommand(QueryRunner queryRunner, String query, OutputFormat outputFormat)
    {
        StatementSplitter splitter = new StatementSplitter(query);
        for (Statement split : splitter.getCompleteStatements()) {
            if (!isEmptyStatement(split.statement())) {
                process(queryRunner, split.statement(), outputFormat, false);
            }
        }
        if (!isEmptyStatement(splitter.getPartialStatement())) {
            System.err.println("Non-terminated statement: " + splitter.getPartialStatement());
        }
    }

    private static void process(QueryRunner queryRunner, String sql, OutputFormat outputFormat, boolean interactive)
    {
        try (Query query = queryRunner.startQuery(sql)) {
            query.renderOutput(System.out, outputFormat, interactive);

            // update session properties if present
            if (!query.getSetSessionProperties().isEmpty() || !query.getResetSessionProperties().isEmpty()) {
                Map<String, String> sessionProperties = new HashMap<>(queryRunner.getSession().getProperties());
                sessionProperties.putAll(query.getSetSessionProperties());
                sessionProperties.keySet().removeAll(query.getResetSessionProperties());
                queryRunner.setSession(withProperties(queryRunner.getSession(), sessionProperties));
            }
        }
        catch (RuntimeException e) {
            System.out.println("Error running command: " + e.getMessage());
            if (queryRunner.getSession().isDebug()) {
                e.printStackTrace();
            }
        }
    }

    private static MemoryHistory getHistory()
    {
        MemoryHistory history;
        File historyFile = new File(getUserHome(), ".presto_history");
        try {
            history = new FileHistory(historyFile);
            history.setMaxSize(10000);
        }
        catch (IOException e) {
            System.err.printf("WARNING: Failed to load history file (%s): %s. " +
                    "History will not be available during this session.%n",
                    historyFile, e.getMessage());
            history = new MemoryHistory();
        }
        history.setAutoTrim(true);
        return history;
    }

    public static void initializeLogging(boolean debug)
    {
        // unhook out and err while initializing logging or logger will print to them
        PrintStream out = System.out;
        PrintStream err = System.err;
        try {
            if (debug) {
                Logging logging = Logging.initialize();
                logging.configure(new LoggingConfiguration());
                logging.setLevel("com.facebook.presto", Level.DEBUG);
            }
            else {
                System.setOut(nullPrintStream());
                System.setErr(nullPrintStream());

                Logging logging = Logging.initialize();
                logging.configure(new LoggingConfiguration());
                logging.disableConsole();
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            System.setOut(out);
            System.setErr(err);
        }
    }

    public static PrintStream nullPrintStream()
    {
        return new PrintStream(nullOutputStream());
    }
}
