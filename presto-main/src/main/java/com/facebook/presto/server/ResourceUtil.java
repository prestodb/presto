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
package com.facebook.presto.server;

import com.facebook.presto.Session;
import com.facebook.presto.Session.SessionBuilder;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.TimeZoneNotSupportedException;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.base.Splitter;
import io.airlift.units.Duration;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_LANGUAGE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

final class ResourceUtil
{
    private ResourceUtil()
    {
    }

    public static Session createSessionForRequest(
            HttpServletRequest servletRequest,
            TransactionManager transactionManager,
            AccessControl accessControl,
            SessionPropertyManager sessionPropertyManager,
            QueryId queryId)
    {
        String catalog = trimEmptyToNull(servletRequest.getHeader(PRESTO_CATALOG));
        String schema = trimEmptyToNull(servletRequest.getHeader(PRESTO_SCHEMA));
        assertRequest((catalog != null) || (schema == null), "Schema is set but catalog is not");

        String user = trimEmptyToNull(servletRequest.getHeader(PRESTO_USER));
        assertRequest(user != null, "User must be set");

        Principal principal = servletRequest.getUserPrincipal();
        try {
            accessControl.checkCanSetUser(principal, user);
        }
        catch (AccessDeniedException e) {
            throw new WebApplicationException(e.getMessage(), Status.FORBIDDEN);
        }

        Identity identity = new Identity(user, Optional.ofNullable(principal));
        SessionBuilder sessionBuilder = Session.builder(sessionPropertyManager)
                .setQueryId(queryId)
                .setIdentity(identity)
                .setSource(servletRequest.getHeader(PRESTO_SOURCE))
                .setCatalog(catalog)
                .setSchema(schema)
                .setRemoteUserAddress(servletRequest.getRemoteAddr())
                .setUserAgent(servletRequest.getHeader(USER_AGENT));

        String timeZoneId = servletRequest.getHeader(PRESTO_TIME_ZONE);
        if (timeZoneId != null) {
            sessionBuilder.setTimeZoneKey(getTimeZoneKey(timeZoneId));
        }

        String language = servletRequest.getHeader(PRESTO_LANGUAGE);
        if (language != null) {
            sessionBuilder.setLocale(Locale.forLanguageTag(language));
        }

        // parse session properties
        for (Entry<String, String> entry : parseSessionHeaders(servletRequest).entrySet()) {
            String fullPropertyName = entry.getKey();
            String propertyValue = entry.getValue();
            List<String> nameParts = Splitter.on('.').splitToList(fullPropertyName);
            if (nameParts.size() == 1) {
                String propertyName = nameParts.get(0);

                assertRequest(!propertyName.isEmpty(), "Invalid %s header", PRESTO_SESSION);

                // catalog session properties can not be validated until the transaction has stated, so we delay system property validation also
                sessionBuilder.setSystemProperty(propertyName, propertyValue);
            }
            else if (nameParts.size() == 2) {
                String catalogName = nameParts.get(0);
                String propertyName = nameParts.get(1);

                assertRequest(!catalogName.isEmpty(), "Invalid %s header", PRESTO_SESSION);
                assertRequest(!propertyName.isEmpty(), "Invalid %s header", PRESTO_SESSION);

                // catalog session properties can not be validated until the transaction has stated
                sessionBuilder.setCatalogSessionProperty(catalogName, propertyName, propertyValue);
            }
            else {
                throw badRequest(format("Invalid %s header", PRESTO_SESSION));
            }
        }

        for (Entry<String, String> preparedStatement : parsePreparedStatementsHeaders(servletRequest).entrySet()) {
            sessionBuilder.addPreparedStatement(preparedStatement.getKey(), preparedStatement.getValue());
        }

        String transactionIdHeader = servletRequest.getHeader(PRESTO_TRANSACTION_ID);
        if (transactionIdHeader != null) {
            sessionBuilder.setClientTransactionSupport();
        }

        Session session = sessionBuilder.build();
        Optional<TransactionId> transactionId = parseTransactionId(transactionIdHeader);
        if (transactionId.isPresent()) {
            session = session.beginTransactionId(transactionId.get(), transactionManager, accessControl);
        }

        return session;
    }

    public static ClientSession createClientSessionForRequest(HttpServletRequest request, URI server, Duration clientRequestTimeout)
    {
        requireNonNull(request, "request is null");
        requireNonNull(server, "server is null");
        requireNonNull(clientRequestTimeout, "clientRequestTimeout is null");

        String catalog = trimEmptyToNull(request.getHeader(PRESTO_CATALOG));
        String schema = trimEmptyToNull(request.getHeader(PRESTO_SCHEMA));
        assertRequest((catalog != null) || (schema == null), "Schema is set but catalog is not");

        String user = trimEmptyToNull(request.getHeader(PRESTO_USER));
        assertRequest(user != null, "User must be set");

        Principal principal = request.getUserPrincipal();

        String source = request.getHeader(PRESTO_SOURCE);

        Identity identity = new Identity(user, Optional.ofNullable(principal));

        String transactionId = trimEmptyToNull(request.getHeader(PRESTO_TRANSACTION_ID));

        String timeZoneId = request.getHeader(PRESTO_TIME_ZONE);

        String language = request.getHeader(PRESTO_LANGUAGE);
        Locale locale = null;
        if (language != null) {
            locale = Locale.forLanguageTag(language);
        }

        Map<String, String> sessionProperties = parseSessionHeaders(request);

        Map<String, String> preparedStatements = parsePreparedStatementsHeaders(request);

        return new ClientSession(
                server,
                identity.getUser(),
                source,
                catalog,
                schema,
                timeZoneId,
                locale,
                sessionProperties,
                preparedStatements,
                transactionId,
                false,
                clientRequestTimeout);
    }

    private static List<String> splitSessionHeader(Enumeration<String> headers)
    {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        return Collections.list(headers).stream()
                .map(splitter::splitToList)
                .flatMap(Collection::stream)
                .collect(toImmutableList());
    }

    private static Map<String, String> parseSessionHeaders(HttpServletRequest servletRequest)
    {
        Map<String, String> sessionProperties = new HashMap<>();
        for (String header : splitSessionHeader(servletRequest.getHeaders(PRESTO_SESSION))) {
            List<String> nameValue = Splitter.on('=').limit(2).trimResults().splitToList(header);
            assertRequest(nameValue.size() == 2, "Invalid %s header", PRESTO_SESSION);
            sessionProperties.put(nameValue.get(0), nameValue.get(1));
        }
        return sessionProperties;
    }

    public static void assertRequest(boolean expression, String format, Object... args)
    {
        if (!expression) {
            throw badRequest(format(format, args));
        }
    }

    private static Map<String, String> parsePreparedStatementsHeaders(HttpServletRequest servletRequest)
    {
        Map<String, String> preparedStatements = new HashMap<>();
        for (String header : splitSessionHeader(servletRequest.getHeaders(PRESTO_PREPARED_STATEMENT))) {
            List<String> nameValue = Splitter.on('=').limit(2).trimResults().splitToList(header);
            assertRequest(nameValue.size() == 2, "Invalid %s header", PRESTO_PREPARED_STATEMENT);

            String statementName;
            String sqlString;
            try {
                statementName = urlDecode(nameValue.get(0));
                sqlString = urlDecode(nameValue.get(1));
            }
            catch (IllegalArgumentException e) {
                throw badRequest(format("Invalid %s header: %s", PRESTO_PREPARED_STATEMENT, e.getMessage()));
            }

            // Validate statement
            SqlParser sqlParser = new SqlParser();
            try {
                sqlParser.createStatement(sqlString);
            }
            catch (ParsingException e) {
                throw badRequest(format("Invalid %s header: %s", PRESTO_PREPARED_STATEMENT, e.getMessage()));
            }

            preparedStatements.put(statementName, sqlString);
        }
        return preparedStatements;
    }

    private static TimeZoneKey getTimeZoneKey(String timeZoneId)
    {
        try {
            return TimeZoneKey.getTimeZoneKey(timeZoneId);
        }
        catch (TimeZoneNotSupportedException e) {
            throw badRequest(e.getMessage());
        }
    }

    private static Optional<TransactionId> parseTransactionId(String transactionId)
    {
        transactionId = trimEmptyToNull(transactionId);
        if (transactionId == null || transactionId.equalsIgnoreCase("none")) {
            return Optional.empty();
        }
        try {
            return Optional.of(TransactionId.valueOf(transactionId));
        }
        catch (Exception e) {
            throw badRequest(e.getMessage());
        }
    }

    private static WebApplicationException badRequest(String message)
    {
        throw new WebApplicationException(Response
                .status(Status.BAD_REQUEST)
                .type(MediaType.TEXT_PLAIN)
                .entity(message)
                .build());
    }

    private static String trimEmptyToNull(String value)
    {
        return emptyToNull(nullToEmpty(value).trim());
    }

    public static String urlEncode(String value)
    {
        try {
            return URLEncoder.encode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    public static String urlDecode(String value)
    {
        try {
            return URLDecoder.decode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }
}
