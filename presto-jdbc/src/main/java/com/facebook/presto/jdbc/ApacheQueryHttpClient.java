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
package com.facebook.presto.jdbc;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.PrestoHeaders;
import com.facebook.presto.client.QueryHttpClient;
import com.facebook.presto.client.QueryResults;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;
import static org.apache.http.entity.ContentType.parse;

public class ApacheQueryHttpClient implements QueryHttpClient
{
    private static final RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(10000).setConnectTimeout(10000).build();

    private final CloseableHttpAsyncClient httpAsyncClient;
    private final ObjectMapper mapper;
    private final String userAgent;
    private final AtomicBoolean closed = new AtomicBoolean();

    public ApacheQueryHttpClient(CloseableHttpAsyncClient httpAsyncClient,
                                 ObjectMapper mapper,
                                 String userAgent)
    {
        checkNotNull(httpAsyncClient, "httpAsyncClient is null");
        checkNotNull(mapper, "mapper is null");
        checkNotNull(userAgent, "userAgent is null");

        this.httpAsyncClient = httpAsyncClient;
        this.mapper = mapper;
        this.userAgent = userAgent;
    }

    @Override
    public QueryResults startQuery(ClientSession session, String query)
    {
        HttpPost request = buildQueryRequest(session, query);
        try {
            HttpResponse response = httpAsyncClient.execute(request, null).get();
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw requestFailedStatus("starting query", request, response.getStatusLine().getReasonPhrase());
            }
            return parseResult(request, response);
        }
        catch (InterruptedException e) {
            throw requestFailedException("starting query", request, e);
        }
        catch (ExecutionException e) {
            throw requestFailedException("starting query", request, e);
        }
    }

    @Override
    public void deleteAsync(URI uri)
    {
        HttpDelete request = new HttpDelete(uri);
        request.setHeader(USER_AGENT, userAgent);
        request.setConfig(requestConfig);
        httpAsyncClient.execute(request, null);
    }

    @Override
    public boolean delete(URI uri)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResults execute(URI uri) throws RuntimeException
    {
        HttpGet request = new HttpGet(uri);
        request.setConfig(requestConfig);
        request.setHeader(USER_AGENT, userAgent);

        Exception cause = null;
        long start = System.nanoTime();
        long attempts = 0;

        do {
            // back-off on retry
            if (attempts > 0) {
                sleepUninterruptibly(attempts * 100, MILLISECONDS);
            }
            attempts++;

            HttpResponse response;
            try {
                response = httpAsyncClient.execute(request, null).get();
            }
            catch (InterruptedException e) {
                cause = e;
                continue;
            }
            catch (ExecutionException e) {
                cause = e;
                continue;
            }

            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                return parseResult(request, response);
            }

            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_SERVICE_UNAVAILABLE) {
                throw requestFailedStatus("fetching next", request, response.getStatusLine().getReasonPhrase());
            }
        }
        while ((System.nanoTime() - start) < MINUTES.toNanos(2) && !isClosed());

        throw new RuntimeException("Error fetching next", cause);
    }

    @Override
    public Map<String, String> getSetSessionProperties()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> getResetSessionProperties()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed()
    {
        return closed.get();
    }

    @Override
    public void close()
    {
        closed.set(true);
    }

    private RuntimeException requestFailedException(String task, HttpRequestBase request, Exception exception)
    {
        return new RuntimeException(
                format("Error " + task + " at %s returned an invalid response: %s", request.getURI(), exception.getMessage()),
                exception);
    }

    private RuntimeException requestFailedStatus(String task, HttpRequestBase request, String reason)
    {
        return new RuntimeException(format("Error " + task + " at %s failed with status %s", request.getURI(), reason));
    }

    private HttpPost buildQueryRequest(ClientSession session, String query)
    {
        HttpPost post = new HttpPost(session.getServer());
        post.setEntity(new StringEntity(query, Charset.forName("UTF-8")));

        if (session.getUser() != null) {
            post.setHeader(PrestoHeaders.PRESTO_USER, session.getUser());
        }
        if (session.getSource() != null) {
            post.setHeader(PrestoHeaders.PRESTO_SOURCE, session.getSource());
        }
        if (session.getCatalog() != null) {
            post.setHeader(PrestoHeaders.PRESTO_CATALOG, session.getCatalog());
        }
        if (session.getSchema() != null) {
            post.setHeader(PrestoHeaders.PRESTO_SCHEMA, session.getSchema());
        }
        post.setHeader(PrestoHeaders.PRESTO_TIME_ZONE, session.getTimeZoneId());
        // In Java 6 we don't have toLanguageTag
        //String localeId = session.getLocale().toLanguageTag());
        String localeId = LocaleHelper.buildLanguageTag(session.getLocale());
        post.setHeader(PrestoHeaders.PRESTO_LANGUAGE, localeId);
        post.setHeader(USER_AGENT, userAgent);
        post.setConfig(requestConfig);

        Map<String, String> property = session.getProperties();
        for (Map.Entry<String, String> entry : property.entrySet()) {
            post.addHeader(PrestoHeaders.PRESTO_SESSION, entry.getKey() + "=" + entry.getValue());
        }

        return post;
    }

    private QueryResults parseResult(HttpRequestBase request, HttpResponse response)
    {
        String contentType = response.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue();
        if (!isApplicationJson(contentType)) {
            throw new RuntimeException(
                    format("Error parsing result. Wrong content type. Got %s and expected %s",
                            contentType,
                            APPLICATION_JSON.toString()));
        }

        try {
            return mapper.readValue(response.getEntity().getContent(), QueryResults.class);
        }
        catch (IOException e) {
            throw requestFailedException("parse result", request, e);
        }
    }

    private boolean isApplicationJson(String contentType)
    {
        return parse(contentType).getMimeType().equals(APPLICATION_JSON.getMimeType());
    }
}
