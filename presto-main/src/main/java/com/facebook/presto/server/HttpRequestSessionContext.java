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

import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_INFO;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_TAGS;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_LANGUAGE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static java.lang.String.format;

public final class HttpRequestSessionContext
        implements SessionContext
{
    private static final Splitter DOT_SPLITTER = Splitter.on('.');

    private final String catalog;
    private final String schema;

    private final Identity identity;

    private final String source;
    private final String userAgent;
    private final String remoteUserAddress;
    private final String timeZoneId;
    private final String language;
    private final Set<String> clientTags;

    private final Map<String, String> systemProperties;
    private final Map<String, Map<String, String>> catalogSessionProperties;

    private final Map<String, String> preparedStatements;

    private final Optional<TransactionId> transactionId;
    private final boolean clientTransactionSupport;
    private final String clientInfo;

    public HttpRequestSessionContext(HttpServletRequest servletRequest)
            throws WebApplicationException
    {
        catalog = trimEmptyToNull(servletRequest.getHeader(PRESTO_CATALOG));
        schema = trimEmptyToNull(servletRequest.getHeader(PRESTO_SCHEMA));
        assertRequest((catalog != null) || (schema == null), "Schema is set but catalog is not");

        String user = trimEmptyToNull(servletRequest.getHeader(PRESTO_USER));
        assertRequest(user != null, "User must be set");
        identity = new Identity(user, Optional.ofNullable(servletRequest.getUserPrincipal()));

        source = servletRequest.getHeader(PRESTO_SOURCE);
        userAgent = servletRequest.getHeader(USER_AGENT);
        remoteUserAddress = servletRequest.getRemoteAddr();
        timeZoneId = servletRequest.getHeader(PRESTO_TIME_ZONE);
        language = servletRequest.getHeader(PRESTO_LANGUAGE);
        clientInfo = servletRequest.getHeader(PRESTO_CLIENT_INFO);
        clientTags = parseClientTags(servletRequest);

        // parse session properties
        ImmutableMap.Builder<String, String> systemProperties = ImmutableMap.builder();
        Map<String, Map<String, String>> catalogSessionProperties = new HashMap<>();
        for (Entry<String, String> entry : parseSessionHeaders(servletRequest).entrySet()) {
            String fullPropertyName = entry.getKey();
            String propertyValue = entry.getValue();
            List<String> nameParts = DOT_SPLITTER.splitToList(fullPropertyName);
            if (nameParts.size() == 1) {
                String propertyName = nameParts.get(0);

                assertRequest(!propertyName.isEmpty(), "Invalid %s header", PRESTO_SESSION);

                // catalog session properties can not be validated until the transaction has stated, so we delay system property validation also
                systemProperties.put(propertyName, propertyValue);
            }
            else if (nameParts.size() == 2) {
                String catalogName = nameParts.get(0);
                String propertyName = nameParts.get(1);

                assertRequest(!catalogName.isEmpty(), "Invalid %s header", PRESTO_SESSION);
                assertRequest(!propertyName.isEmpty(), "Invalid %s header", PRESTO_SESSION);

                // catalog session properties can not be validated until the transaction has stated
                catalogSessionProperties.computeIfAbsent(catalogName, id -> new HashMap<>()).put(propertyName, propertyValue);
            }
            else {
                throw badRequest(format("Invalid %s header", PRESTO_SESSION));
            }
        }
        this.systemProperties = systemProperties.build();
        this.catalogSessionProperties = catalogSessionProperties.entrySet().stream()
                .collect(toImmutableMap(Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue())));

        preparedStatements = parsePreparedStatementsHeaders(servletRequest);

        String transactionIdHeader = servletRequest.getHeader(PRESTO_TRANSACTION_ID);
        clientTransactionSupport = transactionIdHeader != null;
        transactionId = parseTransactionId(transactionIdHeader);
    }

    @Override
    public Identity getIdentity()
    {
        return identity;
    }

    @Override
    public String getCatalog()
    {
        return catalog;
    }

    @Override
    public String getSchema()
    {
        return schema;
    }

    @Override
    public String getSource()
    {
        return source;
    }

    @Override
    public String getRemoteUserAddress()
    {
        return remoteUserAddress;
    }

    @Override
    public String getUserAgent()
    {
        return userAgent;
    }

    @Override
    public String getClientInfo()
    {
        return clientInfo;
    }

    @Override
    public Set<String> getClientTags()
    {
        return clientTags;
    }

    @Override
    public String getTimeZoneId()
    {
        return timeZoneId;
    }

    @Override
    public String getLanguage()
    {
        return language;
    }

    @Override
    public Map<String, String> getSystemProperties()
    {
        return systemProperties;
    }

    @Override
    public Map<String, Map<String, String>> getCatalogSessionProperties()
    {
        return catalogSessionProperties;
    }

    @Override
    public Map<String, String> getPreparedStatements()
    {
        return preparedStatements;
    }

    @Override
    public Optional<TransactionId> getTransactionId()
    {
        return transactionId;
    }

    @Override
    public boolean supportClientTransaction()
    {
        return clientTransactionSupport;
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

    private Set<String> parseClientTags(HttpServletRequest servletRequest)
    {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        return ImmutableSet.copyOf(splitter.split(nullToEmpty(servletRequest.getHeader(PRESTO_CLIENT_TAGS))));
    }

    private static void assertRequest(boolean expression, String format, Object... args)
    {
        if (!expression) {
            throw badRequest(format(format, args));
        }
    }

    private static Map<String, String> parsePreparedStatementsHeaders(HttpServletRequest servletRequest)
    {
        ImmutableMap.Builder<String, String> preparedStatements = ImmutableMap.builder();
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
        return preparedStatements.build();
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

    private static String urlDecode(String value)
    {
        try {
            return URLDecoder.decode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }
}
