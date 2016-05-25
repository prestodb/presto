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
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.TimeZoneNotSupportedException;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.io.UnsupportedEncodingException;
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

final class ResourceUtil
{
    private ResourceUtil()
    {
    }

    public static Session createSessionForRequest(HttpServletRequest servletRequest, AccessControl accessControl, SessionPropertyManager sessionPropertyManager, QueryId queryId)
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

        String transactionId = trimEmptyToNull(servletRequest.getHeader(PRESTO_TRANSACTION_ID));
        if (transactionId != null) {
            sessionBuilder.setClientTransactionSupport();
            getTransactionId(transactionId).ifPresent(sessionBuilder::setTransactionId);
        }

        String timeZoneId = servletRequest.getHeader(PRESTO_TIME_ZONE);
        if (timeZoneId != null) {
            sessionBuilder.setTimeZoneKey(getTimeZoneKey(timeZoneId));
        }

        String language = servletRequest.getHeader(PRESTO_LANGUAGE);
        if (language != null) {
            sessionBuilder.setLocale(Locale.forLanguageTag(language));
        }

        // parse session properties
        Multimap<String, Entry<String, String>> sessionPropertiesByCatalog = HashMultimap.create();
        for (String sessionHeader : splitSessionHeader(servletRequest.getHeaders(PRESTO_SESSION))) {
            parseSessionHeader(sessionHeader, sessionPropertiesByCatalog, sessionPropertyManager);
        }

        // verify user can set the session properties
        try {
            for (Entry<String, Entry<String, String>> property : sessionPropertiesByCatalog.entries()) {
                String catalogName = property.getKey();
                String propertyName = property.getValue().getKey();
                if (catalogName == null) {
                    accessControl.checkCanSetSystemSessionProperty(identity, propertyName);
                }
                else {
                    accessControl.checkCanSetCatalogSessionProperty(identity, catalogName, propertyName);
                }
            }
        }
        catch (AccessDeniedException e) {
            throw new WebApplicationException(e.getMessage(), Status.BAD_REQUEST);
        }
        sessionBuilder.setSystemProperties(toMap(sessionPropertiesByCatalog.get(null)));
        for (Entry<String, Collection<Entry<String, String>>> entry : sessionPropertiesByCatalog.asMap().entrySet()) {
            if (entry.getKey() != null) {
                sessionBuilder.setCatalogProperties(entry.getKey(), toMap(entry.getValue()));
            }
        }

        Map<String, String> preparedStatements = new HashMap<>();
        for (String preparedStatementsHeader : splitSessionHeader(servletRequest.getHeaders(PRESTO_PREPARED_STATEMENT))) {
            parsePreparedStatementsHeader(preparedStatementsHeader, preparedStatements);
        }
        sessionBuilder.setPreparedStatements(preparedStatements);
        return sessionBuilder.build();
    }

    private static List<String> splitSessionHeader(Enumeration<String> headers)
    {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        return Collections.list(headers).stream()
                .map(splitter::splitToList)
                .flatMap(Collection::stream)
                .collect(toImmutableList());
    }

    private static void parseSessionHeader(String header, Multimap<String, Entry<String, String>> sessionPropertiesByCatalog, SessionPropertyManager sessionPropertyManager)
    {
        List<String> nameValue = Splitter.on('=').limit(2).trimResults().splitToList(header);
        assertRequest(nameValue.size() == 2, "Invalid %s header", PRESTO_SESSION);
        String fullPropertyName = nameValue.get(0);

        String catalog;
        String name;
        List<String> nameParts = Splitter.on('.').splitToList(fullPropertyName);
        if (nameParts.size() == 1) {
            catalog = null;
            name = nameParts.get(0);
        }
        else if (nameParts.size() == 2) {
            catalog = nameParts.get(0);
            name = nameParts.get(1);
        }
        else {
            throw badRequest(format("Invalid %s header", PRESTO_SESSION));
        }
        assertRequest(catalog == null || !catalog.isEmpty(), "Invalid %s header", PRESTO_SESSION);
        assertRequest(!name.isEmpty(), "Invalid %s header", PRESTO_SESSION);

        String value = nameValue.get(1);

        // validate session property value
        PropertyMetadata<?> metadata = sessionPropertyManager.getSessionPropertyMetadata(fullPropertyName);
        try {
            sessionPropertyManager.decodeProperty(fullPropertyName, value, metadata.getJavaType());
        }
        catch (RuntimeException e) {
            throw badRequest(format("Invalid %s header", PRESTO_SESSION));
        }

        sessionPropertiesByCatalog.put(catalog, Maps.immutableEntry(name, value));
    }

    private static <K, V> Map<K, V> toMap(Iterable<? extends Entry<K, V>> entries)
    {
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        for (Entry<K, V> entry : entries) {
            builder.put(entry);
        }
        return builder.build();
    }

    public static void assertRequest(boolean expression, String format, Object... args)
    {
        if (!expression) {
            throw badRequest(format(format, args));
        }
    }

    private static void parsePreparedStatementsHeader(String header, Map<String, String> preparedStatements)
    {
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

    private static TimeZoneKey getTimeZoneKey(String timeZoneId)
    {
        try {
            return TimeZoneKey.getTimeZoneKey(timeZoneId);
        }
        catch (TimeZoneNotSupportedException e) {
            throw badRequest(e.getMessage());
        }
    }

    private static Optional<TransactionId> getTransactionId(String transactionId)
    {
        if (transactionId.toUpperCase().equals("NONE")) {
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
