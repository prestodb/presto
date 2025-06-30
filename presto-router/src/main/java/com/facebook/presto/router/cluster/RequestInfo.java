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
package com.facebook.presto.router.cluster;

import com.facebook.presto.spi.router.RouterRequestInfo;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import jakarta.servlet.http.HttpServletRequest;

import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_TAGS;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Collections.list;
import static java.util.Objects.requireNonNull;

public class RequestInfo
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private final String user;
    private final Optional<String> source;
    private final List<String> clientTags;
    private final String query;
    private final HttpServletRequest servletRequest;
    private final Map<String, List<String>> headersMap;

    public RequestInfo(HttpServletRequest servletRequest, String query)
    {
        this.servletRequest = requireNonNull(servletRequest, "servletRequest is null");
        this.user = parseHeader(servletRequest, PRESTO_USER);
        this.source = Optional.ofNullable(parseHeader(servletRequest, PRESTO_SOURCE));
        this.clientTags = requireNonNull(parseClientTags(servletRequest), "clientTags is null");
        this.query = query;
        this.headersMap = requireNonNull(parseHeaders(servletRequest), "headersMap is null");
    }

    public String getUser()
    {
        return user;
    }

    public Optional<String> getSource()
    {
        return source;
    }

    public String getQuery()
    {
        return query;
    }

    public List<String> getClientTags()
    {
        return clientTags;
    }

    public Map<String, List<String>> getHeadersMap()
    {
        return headersMap;
    }

    public RouterRequestInfo toRouterRequestInfo()
    {
        return new RouterRequestInfo(user, source, clientTags, query, headersMap, servletRequest.getUserPrincipal());
    }

    private static List<String> parseClientTags(HttpServletRequest servletRequest)
    {
        return ImmutableList.copyOf(SPLITTER.split(nullToEmpty(servletRequest.getHeader(PRESTO_CLIENT_TAGS))));
    }

    private static String parseHeader(HttpServletRequest servletRequest, String header)
    {
        return trimEmptyToNull(servletRequest.getHeader(header));
    }

    private static String trimEmptyToNull(String value)
    {
        return emptyToNull(nullToEmpty(value).trim());
    }

    private static Map<String, List<String>> parseHeaders(HttpServletRequest httpServletRequest)
    {
        ImmutableMap.Builder<String, List<String>> builder = ImmutableMap.builder();
        Enumeration<String> headerNames = httpServletRequest.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();
            Enumeration<String> values = httpServletRequest.getHeaders(headerName);
            builder.put(headerName, list(values));
        }
        return builder.build();
    }
}
