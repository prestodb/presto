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
package io.prestosql.tests.querystats;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.io.Resources;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.execution.QueryStats;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestHttpQueryStatsClient
{
    private static final URI BASE_URL = URI.create("http://presto.host");
    private Response httpResponse;
    private HttpQueryStatsClient queryStatsClient;

    private static final String SINGLE_QUERY_INFO = resourceAsString("io/prestosql/tests/querystats/single_query_info_response.json");

    private static String resourceAsString(String resourcePath)
    {
        try {
            URL resourceUrl = Resources.getResource(resourcePath);
            return Resources.toString(resourceUrl, UTF_8);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeMethod
    public void setUp()
    {
        ObjectMapper objectMapper = new ObjectMapperProvider().get();
        TestingHttpClient httpClient = new TestingHttpClient(httpRequest -> httpResponse);
        this.queryStatsClient = new HttpQueryStatsClient(httpClient, objectMapper, BASE_URL);
    }

    @Test
    public void testGetInfoForQuery()
    {
        mockHttpResponse(SINGLE_QUERY_INFO);
        Optional<QueryStats> infoForQuery = queryStatsClient.getQueryStats("20150505_160116_00005_sdzex");
        assertThat(infoForQuery).isPresent();
        assertThat(infoForQuery.get().getTotalCpuTime().getValue()).isEqualTo(1.19);
    }

    @Test
    public void testGetInfoForUnknownQuery()
    {
        mockErrorHttpResponse(HttpStatus.GONE);
        Optional<QueryStats> infoForQuery = queryStatsClient.getQueryStats("20150505_160116_00005_sdzex");
        assertThat(infoForQuery).isEmpty();
    }

    private void mockHttpResponse(String answerJson)
    {
        httpResponse = new TestingResponse(HttpStatus.OK, ImmutableListMultimap.of(), answerJson.getBytes());
    }

    private void mockErrorHttpResponse(HttpStatus statusCode)
    {
        httpResponse = new TestingResponse(statusCode, ImmutableListMultimap.of(), new byte[0]);
    }
}
