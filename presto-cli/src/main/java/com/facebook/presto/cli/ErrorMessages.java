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
import com.facebook.presto.client.ErrorLocation;
import com.facebook.presto.client.PrestoClientException;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.HttpStatus;
import org.fusesource.jansi.Ansi;

import java.io.EOFException;
import java.net.ConnectException;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_STARTING_UP;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getCausalChain;
import static java.lang.String.format;

public class ErrorMessages
{
    private static final String PRESTO_COORDINATOR_NOT_FOUND = "There was a problem with a response from Presto Coordinator.\n";
    private static final String PRESTO_COORDINATOR_404 = "Presto HTTP interface returned 404 (file not found).\n";
    private static final String PRESTO_STARTING_UP  = "Presto is still starting up.\n";
    private static final String PRESTO_SHUTTING_DOWN  = "Presto is shutting down\n";

    private enum Tip {
        VERIFY_PRESTO_RUNNING("Verify that Presto is running on %s."),
        DEFINE_SERVER_AS_CLI_PARAM("Use '--server' argument when starting Presto CLI to define server host and port."),
        CHECK_NETWORK("Check the network conditions between client and server."),
        USE_DEBUG_MODE("Use '--debug' argument to get more technical details."),
        WAIT_FOR_INITIALIZATION("Wait for server to complete initialization."),
        WAIT_FOR_SERVER_RESTART("Wait for server to restart as it may have restart scheduled."),
        START_SERVER_AGAIN("Start server again manually."),
        CHECK_OTHER_HTTP_SERVICE("Make sure that none other HTTP service is running on %s."),
        CLIENT_IS_UP_TO_DATE_WITH_SERVER("Update CLI to match Presto server version.");

        private static final String TIPS_INTRO = "To solve this problem you may try to:\n";

        private final String message;

        private Tip(String message)
        {
            this.message = message;
        }

        @Override
        public String toString()
        {
            return message;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        private static class Builder
        {
            private StringBuilder builder = new StringBuilder();

            public Builder()
            {
                builder.append(TIPS_INTRO);
            }

            public Builder addTip(Tip tip, Object ...toFormat)
            {
                builder.append(format(" * " + tip.toString() + "\n", toFormat));
                return this;
            }

            public String build()
            {
                return builder.toString();
            }
        }
    }

    private ErrorMessages() {}

    public static String createQueryErrorMessage(StatementClient client, boolean useAnsiEscapeCodes)
    {
        StringBuilder builder = new StringBuilder();
        ClientSession session = client.getSession();
        QueryError error = extractQueryError(client);

        // Try to recognize error and provide custom error message
        if (error.getErrorCode() == SERVER_STARTING_UP.toErrorCode().getCode()) {
            serverStartingUpErrorMessage(builder, session);
        }
        else if (error.getErrorCode() == SERVER_SHUTTING_DOWN.toErrorCode().getCode()) {
            serverShuttingDownErrorMessage(builder, session);
        }
        else {
            // Fallback to standard QueryError message
            standardQueryErrorMessage(builder, client);
        }

        // Display common part of all QueryError (position, debug details)
        if (error.getErrorLocation() != null) {
            errorLocationMessage(builder, client.getQuery(), error.getErrorLocation(), useAnsiEscapeCodes);
        }

        if (client.isDebug() && (error.getFailureInfo() != null)) {
            technicalDetailsRuntimeExceptionErrorMessage(builder, error.getFailureInfo().toException(), session);
        }

        return builder.toString();
    }

    public static String createExceptionMessage(Throwable throwable, ClientSession session)
    {
        StringBuilder builder = new StringBuilder();

        // Try to recognize exception type and provide custom error message
        if (throwable instanceof PrestoClientException) {
            createPrestoClientExceptionErrorMessage(builder, (PrestoClientException) throwable, session);
        }
        else {
            runtimeExceptionErrorMessage(builder, throwable, session);
        }

        // Display common part of all Throwable error messages
        if (session.isDebug()) {
            technicalDetailsRuntimeExceptionErrorMessage(builder, throwable, session);
        }

        return builder.toString();
    }

    private static void createPrestoClientExceptionErrorMessage(StringBuilder builder, PrestoClientException exception, ClientSession session)
    {
        if (exception.getResponse().getStatusCode() == HttpStatus.NOT_FOUND.code()) {
            serverFileNotFoundErrorMessage(builder, session);
        }
        else {
            runtimeExceptionErrorMessage(builder, exception, session);
        }
    }

    private static void runtimeExceptionErrorMessage(StringBuilder builder, Throwable throwable, ClientSession session)
    {
        if (getCausalChain(throwable).stream().anyMatch(x -> x instanceof EOFException || x instanceof ConnectException)) {
            serverNotFoundErrorMessage(builder, session);
        }
        else {
            // We have no clue about what went wrong, just display what we obtained.
            builder.append("Error running command:\n" + throwable.getMessage() + "\n");
        }
    }

    //region Messages for given problems
    private static void serverNotFoundErrorMessage(StringBuilder builder, ClientSession session)
    {
        builder.append(PRESTO_COORDINATOR_NOT_FOUND);
        if (session.isQuiet()) {
            return;
        }

        Tip.Builder tipsBuilder = Tip.builder();
        tipsBuilder.addTip(Tip.VERIFY_PRESTO_RUNNING, session.getServer())
                .addTip(Tip.DEFINE_SERVER_AS_CLI_PARAM)
                .addTip(Tip.CHECK_NETWORK).build();
        if (!session.isDebug()) {
            tipsBuilder.addTip(Tip.USE_DEBUG_MODE);
        }
        builder.append(tipsBuilder.build());
    }

    private static void serverStartingUpErrorMessage(StringBuilder builder, ClientSession session)
    {
        builder.append(PRESTO_STARTING_UP);
        if (session.isQuiet()) {
            return;
        }

        Tip.Builder tipsBuilder = Tip.builder();
        tipsBuilder.addTip(Tip.WAIT_FOR_INITIALIZATION);
        if (!session.isDebug()) {
            tipsBuilder.addTip(Tip.USE_DEBUG_MODE);
        }
        builder.append(tipsBuilder.build());
    }

    private static void serverShuttingDownErrorMessage(StringBuilder builder, ClientSession session)
    {
        builder.append(PRESTO_SHUTTING_DOWN);
        if (session.isQuiet()) {
            return;
        }

        Tip.Builder tipsBuilder = Tip.builder();
        tipsBuilder.addTip(Tip.WAIT_FOR_SERVER_RESTART)
                .addTip(Tip.START_SERVER_AGAIN);
        if (!session.isDebug()) {
            tipsBuilder.addTip(Tip.USE_DEBUG_MODE);
        }
        builder.append(tipsBuilder.build());
    }

    private static void serverFileNotFoundErrorMessage(StringBuilder builder, ClientSession session)
    {
        builder.append(PRESTO_COORDINATOR_404);
        if (session.isQuiet()) {
            return;
        }

        Tip.Builder tipsBuilder = Tip.builder();
        tipsBuilder.addTip(Tip.VERIFY_PRESTO_RUNNING, session.getServer())
                .addTip(Tip.DEFINE_SERVER_AS_CLI_PARAM)
                .addTip(Tip.CHECK_NETWORK)
                .addTip(Tip.CHECK_OTHER_HTTP_SERVICE, session.getServer())
                .addTip(Tip.CLIENT_IS_UP_TO_DATE_WITH_SERVER);
        if (!session.isDebug()) {
            tipsBuilder.addTip(Tip.USE_DEBUG_MODE);
        }
        builder.append(tipsBuilder.build());
    }
    //endregion

    private static void technicalDetailsRuntimeExceptionErrorMessage(StringBuilder builder, Throwable throwable, ClientSession session)
    {
        builder.append("\n=========   TECHNICAL DETAILS   =========\n");
        builder.append("[ Error message ]\n");
        builder.append(throwable.getMessage() + "\n\n");
        builder.append("[ Session information ]\n");
        builder.append(session + "\n\n");
        builder.append("[ Stack trace ]\n");
        builder.append(Throwables.getStackTraceAsString(throwable));
        builder.append("========= TECHNICAL DETAILS END =========\n\n");
    }

    //region Standard QueryError format
    private static void standardQueryErrorMessage(StringBuilder builder, StatementClient client)
    {
        QueryError error = extractQueryError(client);
        builder.append(String.format("Query %s failed: %s%n", client.finalResults().getId(), error.getMessage()));
    }

    private static void errorLocationMessage(StringBuilder builder, String query, ErrorLocation location, boolean useAnsiEscapeCodes)
    {
        List<String> lines = ImmutableList.copyOf(Splitter.on('\n').split(query).iterator());

        String errorLine = lines.get(location.getLineNumber() - 1);
        String good = errorLine.substring(0, location.getColumnNumber() - 1);
        String bad = errorLine.substring(location.getColumnNumber() - 1);

        if ((location.getLineNumber() == lines.size()) && bad.trim().isEmpty()) {
            bad = " <EOF>";
        }

        if (useAnsiEscapeCodes) {
            Ansi ansi = Ansi.ansi();

            ansi.fg(Ansi.Color.CYAN);
            for (int i = 1; i < location.getLineNumber(); i++) {
                ansi.a(lines.get(i - 1)).newline();
            }
            ansi.a(good);

            ansi.fg(Ansi.Color.RED);
            ansi.a(bad).newline();
            for (int i = location.getLineNumber(); i < lines.size(); i++) {
                ansi.a(lines.get(i)).newline();
            }

            ansi.reset();
            builder.append(ansi);
        }
        else {
            String prefix = format("LINE %s: ", location.getLineNumber());
            String padding = Strings.repeat(" ", prefix.length() + (location.getColumnNumber() - 1));
            builder.append(prefix + errorLine);
            builder.append(padding + "^");
        }
    }
    //endregion

    //region Helpers
    private static QueryError extractQueryError(StatementClient client)
    {
        QueryResults results = client.finalResults();
        QueryError error = results.getError();
        checkState(error != null);
        return error;
    }
    //endregion
}
