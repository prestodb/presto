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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session.ResourceEstimateBuilder;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.facebook.presto.spi.tracing.Tracer;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.tracing.NoopTracerProvider;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

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

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.SystemSessionProperties.ENABLE_DISTRIBUTED_TRACING;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_INFO;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_TAGS;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_EXTRA_CREDENTIAL;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_LANGUAGE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_RESOURCE_ESTIMATE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_ROLE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION_FUNCTION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TRACE_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.google.common.net.HttpHeaders.X_FORWARDED_FOR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class HttpRequestSessionContext
        implements SessionContext
{
    private static final Splitter DOT_SPLITTER = Splitter.on('.');
    private static final JsonCodec<SqlFunctionId> SQL_FUNCTION_ID_JSON_CODEC = jsonCodec(SqlFunctionId.class);
    private static final JsonCodec<SqlInvokedFunction> SQL_INVOKED_FUNCTION_JSON_CODEC = jsonCodec(SqlInvokedFunction.class);

    private final String catalog;
    private final String schema;

    private final Identity identity;

    private final String source;
    private final Optional<String> traceToken;
    private final String userAgent;
    private final String remoteUserAddress;
    private final String timeZoneId;
    private final String language;
    private final Set<String> clientTags;
    private final ResourceEstimates resourceEstimates;

    private final Map<String, String> systemProperties;
    private final Map<String, Map<String, String>> catalogSessionProperties;

    private final Map<String, String> preparedStatements;

    private final Optional<TransactionId> transactionId;
    private final boolean clientTransactionSupport;
    private final String clientInfo;
    private final Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions;

    private final Optional<SessionPropertyManager> sessionPropertyManager;
    private final Optional<Tracer> tracer;

    public HttpRequestSessionContext(HttpServletRequest servletRequest, SqlParserOptions sqlParserOptions)
    {
        this(servletRequest, sqlParserOptions, NoopTracerProvider.NOOP_TRACER, Optional.empty());
    }

    /**
     * @param servletRequest
     * @param sqlParserOptions
     * @param tracer This passed-in {@link Tracer} will only be used when isTracingEabled() returns true.
     * @param sessionPropertyManager is used to provide with some default session values. In some scenarios we need
     * those default values even before session for a query is created. This is how we can get it at this
     * session context creation stage.
     * @throws WebApplicationException
     */
    public HttpRequestSessionContext(HttpServletRequest servletRequest, SqlParserOptions sqlParserOptions, Tracer tracer, Optional<SessionPropertyManager> sessionPropertyManager)
            throws WebApplicationException
    {
        catalog = trimEmptyToNull(servletRequest.getHeader(PRESTO_CATALOG));
        schema = trimEmptyToNull(servletRequest.getHeader(PRESTO_SCHEMA));
        assertRequest((catalog != null) || (schema == null), "Schema is set but catalog is not");

        String user = trimEmptyToNull(servletRequest.getHeader(PRESTO_USER));
        assertRequest(user != null, "User must be set");
        identity = new Identity(
                user,
                Optional.ofNullable(servletRequest.getUserPrincipal()),
                parseRoleHeaders(servletRequest),
                parseExtraCredentials(servletRequest),
                ImmutableMap.of());

        source = servletRequest.getHeader(PRESTO_SOURCE);
        traceToken = Optional.ofNullable(trimEmptyToNull(servletRequest.getHeader(PRESTO_TRACE_TOKEN)));
        userAgent = servletRequest.getHeader(USER_AGENT);
        remoteUserAddress = !isNullOrEmpty(servletRequest.getHeader(X_FORWARDED_FOR)) ? servletRequest.getHeader(X_FORWARDED_FOR) : servletRequest.getRemoteAddr();
        timeZoneId = servletRequest.getHeader(PRESTO_TIME_ZONE);
        language = servletRequest.getHeader(PRESTO_LANGUAGE);
        clientInfo = servletRequest.getHeader(PRESTO_CLIENT_INFO);
        clientTags = parseClientTags(servletRequest);
        resourceEstimates = parseResourceEstimate(servletRequest);

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

        preparedStatements = parsePreparedStatementsHeaders(servletRequest, sqlParserOptions);

        String transactionIdHeader = servletRequest.getHeader(PRESTO_TRANSACTION_ID);
        clientTransactionSupport = transactionIdHeader != null;
        transactionId = parseTransactionId(transactionIdHeader);

        this.sessionFunctions = parseSessionFunctionHeader(servletRequest);
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        if (isTracingEnabled()) {
            this.tracer = Optional.of(requireNonNull(tracer, "tracer is null"));
        }
        else {
            this.tracer = Optional.of(NoopTracerProvider.NOOP_TRACER);
        }
    }

    public static List<String> splitSessionHeader(Enumeration<String> headers)
    {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        return Collections.list(headers).stream()
                .map(splitter::splitToList)
                .flatMap(Collection::stream)
                .collect(toImmutableList());
    }

    private static Map<String, String> parseSessionHeaders(HttpServletRequest servletRequest)
    {
        return parseProperty(servletRequest, PRESTO_SESSION);
    }

    private static Map<String, SelectedRole> parseRoleHeaders(HttpServletRequest servletRequest)
    {
        ImmutableMap.Builder<String, SelectedRole> roles = ImmutableMap.builder();
        for (String header : splitSessionHeader(servletRequest.getHeaders(PRESTO_ROLE))) {
            List<String> nameValue = Splitter.on('=').limit(2).trimResults().splitToList(header);
            assertRequest(nameValue.size() == 2, "Invalid %s header", PRESTO_ROLE);
            roles.put(nameValue.get(0), SelectedRole.valueOf(urlDecode(nameValue.get(1))));
        }
        return roles.build();
    }

    private static Map<String, String> parseExtraCredentials(HttpServletRequest servletRequest)
    {
        return parseCredentialProperty(servletRequest, PRESTO_EXTRA_CREDENTIAL);
    }

    private static Map<String, String> parseProperty(HttpServletRequest servletRequest, String headerName)
    {
        Map<String, String> properties = new HashMap<>();
        for (String header : splitSessionHeader(servletRequest.getHeaders(headerName))) {
            List<String> nameValue = Splitter.on('=').trimResults().splitToList(header);
            assertRequest(nameValue.size() == 2, "Invalid %s header", headerName);
            properties.put(nameValue.get(0), urlDecode(nameValue.get(1)));
        }
        return properties;
    }

    private static Map<String, String> parseCredentialProperty(HttpServletRequest servletRequest, String headerName)
    {
        Map<String, String> properties = new HashMap<>();
        for (String header : splitSessionHeader(servletRequest.getHeaders(headerName))) {
            List<String> nameValue = Splitter.on('=').limit(2).trimResults().splitToList(header);
            assertRequest(nameValue.size() == 2, "Invalid %s header", headerName);
            properties.put(nameValue.get(0), urlDecode(nameValue.get(1)));
        }
        return properties;
    }

    private static void assertRequest(boolean expression, String format, Object... args)
    {
        if (!expression) {
            throw badRequest(format(format, args));
        }
    }

    private static Map<String, String> parsePreparedStatementsHeaders(HttpServletRequest servletRequest, SqlParserOptions sqlParserOptions)
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
            SqlParser sqlParser = new SqlParser(sqlParserOptions);
            try {
                sqlParser.createStatement(sqlString, new ParsingOptions(AS_DOUBLE /* anything */));
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

    private static Map<SqlFunctionId, SqlInvokedFunction> parseSessionFunctionHeader(HttpServletRequest req)
    {
        ImmutableMap.Builder<SqlFunctionId, SqlInvokedFunction> sessionFunctions = ImmutableMap.builder();
        for (String header : splitSessionHeader(req.getHeaders(PRESTO_SESSION_FUNCTION))) {
            List<String> nameValue = Splitter.on('=').limit(2).trimResults().splitToList(header);
            assertRequest(nameValue.size() == 2, "Invalid %s header", PRESTO_SESSION_FUNCTION);

            String serializedFunctionSignature;
            String serializedFunctionDefinition;
            try {
                serializedFunctionSignature = urlDecode(nameValue.get(0));
                serializedFunctionDefinition = urlDecode(nameValue.get(1));
            }
            catch (IllegalArgumentException e) {
                throw badRequest(format("Invalid %s header: %s", PRESTO_SESSION_FUNCTION, e.getMessage()));
            }
            sessionFunctions.put(SQL_FUNCTION_ID_JSON_CODEC.fromJson(serializedFunctionSignature), SQL_INVOKED_FUNCTION_JSON_CODEC.fromJson(serializedFunctionDefinition));
        }
        return sessionFunctions.build();
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
    public ResourceEstimates getResourceEstimates()
    {
        return resourceEstimates;
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

    @Override
    public Map<SqlFunctionId, SqlInvokedFunction> getSessionFunctions()
    {
        return sessionFunctions;
    }

    @Override
    public Optional<String> getTraceToken()
    {
        return traceToken;
    }

    @Override
    public Optional<Tracer> getTracer()
    {
        return tracer;
    }

    /**
     * This helper method tells if tracing is enabled for this query. We take client provided session property
     * as highest priority. If client does not provide any session enabling property, we then take the system
     * default session value for determining if we should trace this query.
     */
    private boolean isTracingEnabled()
    {
        String clientValue = systemProperties.getOrDefault(ENABLE_DISTRIBUTED_TRACING, "");

        // Client session setting overrides everything.
        if (clientValue.equalsIgnoreCase("true")) {
            return true;
        }
        if (clientValue.equalsIgnoreCase("false")) {
            return false;
        }

        // Client not set, we then take system default value. If property manager not provided then false.
        return sessionPropertyManager.map(manager -> manager.decodeSystemPropertyValue(ENABLE_DISTRIBUTED_TRACING, null, Boolean.class)).orElse(false);
    }

    private Set<String> parseClientTags(HttpServletRequest servletRequest)
    {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        return ImmutableSet.copyOf(splitter.split(nullToEmpty(servletRequest.getHeader(PRESTO_CLIENT_TAGS))));
    }

    public static ResourceEstimates parseResourceEstimate(HttpServletRequest servletRequest)
    {
        ResourceEstimateBuilder builder = new ResourceEstimateBuilder();
        for (String header : splitSessionHeader(servletRequest.getHeaders(PRESTO_RESOURCE_ESTIMATE))) {
            List<String> nameValue = Splitter.on('=').limit(2).trimResults().splitToList(header);
            assertRequest(nameValue.size() == 2, "Invalid %s header", PRESTO_RESOURCE_ESTIMATE);
            String name = nameValue.get(0);
            String value = nameValue.get(1);

            try {
                switch (name.toUpperCase()) {
                    case ResourceEstimates.EXECUTION_TIME:
                        builder.setExecutionTime(Duration.valueOf(value));
                        break;
                    case ResourceEstimates.CPU_TIME:
                        builder.setCpuTime(Duration.valueOf(value));
                        break;
                    case ResourceEstimates.PEAK_MEMORY:
                        builder.setPeakMemory(DataSize.valueOf(value));
                        break;
                    case ResourceEstimates.PEAK_TASK_MEMORY:
                        builder.setPeakTaskMemory(DataSize.valueOf(value));
                        break;
                    default:
                        throw badRequest(format("Unsupported resource name %s", name));
                }
            }
            catch (IllegalArgumentException e) {
                throw badRequest(format("Unsupported format for resource estimate '%s': %s", value, e));
            }
        }

        return builder.build();
    }
}
