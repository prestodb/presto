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
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.fusesource.jansi.Ansi;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class ErrorMessages
{
    private ErrorMessages() {}


    public static String createQueryErrorMessage(StatementClient client, boolean useAnsiEscapeCodes)
    {
        StringBuilder builder = new StringBuilder();
        QueryError error = extractQueryError(client);

        builder.append(String.format("Query %s failed: %s%n", client.finalResults().getId(), error.getMessage()));
        if (client.isDebug() && (error.getFailureInfo() != null)) {
            technicalDetailsRuntimeExceptionErrorMessage(builder, error.getFailureInfo().toException(), client.getSession());
        }
        if (error.getErrorLocation() != null) {
            errorLocationMessage(builder, client.getQuery(), error.getErrorLocation(), useAnsiEscapeCodes);
        }

        return builder.toString();
    }

    public static String createExceptionMessage(Throwable throwable, ClientSession session)
    {
        StringBuilder builder = new StringBuilder();

        // We have no clue about what went wrong, just display what we obtained.
        builder.append("Error running command:\n" + throwable.getMessage() + "\n");

        if (session.isDebug()) {
            technicalDetailsRuntimeExceptionErrorMessage(builder, throwable, session);
        }

        return builder.toString();
    }

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

    private static QueryError extractQueryError(StatementClient client)
    {
        QueryResults results = client.finalResults();
        QueryError error = results.getError();
        checkState(error != null);
        return error;
    }
}
