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
package com.facebook.presto.elasticsearch;

import com.amazonaws.util.Base64;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.elasticsearch.ElasticsearchClientUtil.buildRequest;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;

public class ElasticsearchClientUtilTest
{
    @Test
    public void testBuildRequest() throws UnsupportedEncodingException
    {
        String method = "POST";
        String endpoint = "/test/_doc?refresh";
        HttpEntity entity = new NStringEntity("");
        Map<String, String> paramMap = new HashMap<>();
        paramMap.put("debug", "true");
        paramMap.put("logLevel", "verbose");
        Header authorizationHeader = new BasicHeader("Authorization", format("Basic %s",
                Base64.encodeAsString(format("%s:%s", "USER", "PASSWORD").getBytes(StandardCharsets.UTF_8))));
        Request request = buildRequest(method, endpoint,
                paramMap,
                entity,
                ImmutableList.of(authorizationHeader));
        assertEquals(request.getMethod(), method, "Wrong method in the request");
        assertEquals(request.getMethod(), method, "Wrong endpoint in the request");
        assertEquals(request.getEntity(), entity, "Wrong entity in the request");
        assertEquals(request.getParameters(), paramMap, "Wrong parameters in the request");
        RequestOptions requestOptions = request.getOptions();
        List<Header> headers = requestOptions.getHeaders();
        assertEquals(headers.size(), 1, "Wrong number of headers in the request");
        assertEquals(headers.get(0).getName(), authorizationHeader.getName(), "Wrong name of the header in the request");
        assertEquals(headers.get(0).getValue(), authorizationHeader.getValue(), "Wrong value for the header in the request");
    }
}
