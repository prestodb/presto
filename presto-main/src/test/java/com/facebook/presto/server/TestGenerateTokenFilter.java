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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpRequestFilter;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.StringResponseHandler.StringResponse;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.inject.Qualifier;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.List;

import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static com.facebook.airlift.http.client.TraceTokenRequestFilter.TRACETOKEN_HEADER;
import static com.facebook.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static com.facebook.airlift.testing.Assertions.assertInstanceOf;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestGenerateTokenFilter
{
    private JettyHttpClient httpClient;
    private TestingPrestoServer server;
    private GenerateTraceTokenRequestFilter filter;

    @BeforeClass
    public void setup()
            throws Exception
    {
        server = new TestingPrestoServer(ImmutableList.of(new TestGenerateTokenFilterModule()));
        httpClient = (JettyHttpClient) server.getInstance(Key.get(HttpClient.class, GenerateTokenFilterTest.class));

        // extract the filter
        List<HttpRequestFilter> filters = httpClient.getRequestFilters();
        assertEquals(filters.size(), 3);
        assertInstanceOf(filters.get(2), GenerateTraceTokenRequestFilter.class);
        filter = (GenerateTraceTokenRequestFilter) filters.get(2);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeQuietly(server);
        closeQuietly(httpClient);
    }

    @Test
    public void testTraceToken()
    {
        Request request = prepareGet().setUri(server.getBaseUrl().resolve("/testing/echo_token")).build();
        StringResponse response = httpClient.execute(request, createStringResponseHandler());
        assertEquals(response.getStatusCode(), SC_OK);
        assertEquals(response.getBody(), filter.getLastToken());
    }

    @Retention(RUNTIME)
    @Target(ElementType.PARAMETER)
    @Qualifier
    private @interface GenerateTokenFilterTest {}

    @Path("/testing")
    public static class TestResource
    {
        @GET
        @Path("/echo_token")
        public String echoToken(@HeaderParam(TRACETOKEN_HEADER) String token)
        {
            return token;
        }
    }

    static class TestGenerateTokenFilterModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            jaxrsBinder(binder).bind(TestResource.class);
            httpClientBinder(binder)
                    .bindHttpClient("test", GenerateTokenFilterTest.class)
                    .withTracing()
                    .withFilter(GenerateTraceTokenRequestFilter.class);
        }
    }
}
