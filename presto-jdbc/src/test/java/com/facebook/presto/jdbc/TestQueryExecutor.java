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

import com.facebook.presto.client.ServerInfo;
import com.google.common.collect.ImmutableListMultimap;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static com.facebook.presto.client.NodeVersion.UNKNOWN;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.json.JsonCodec.jsonCodec;
import static org.eclipse.jetty.http.HttpHeader.CONTENT_TYPE;
import static org.testng.Assert.assertEquals;

public class TestQueryExecutor
{
    private static final JsonCodec<ServerInfo> SERVER_INFO_CODEC = jsonCodec(ServerInfo.class);

    @Test
    public void testGetServerInfo()
            throws Exception
    {
        ServerInfo serverInfo = new ServerInfo(UNKNOWN, "test", true, Optional.of(Duration.valueOf("2m")));

        QueryExecutor executor = QueryExecutor.create(new TestingHttpClient(input -> new TestingResponse(
                        OK,
                        ImmutableListMultimap.of(CONTENT_TYPE.toString(), "application/json"),
                        SERVER_INFO_CODEC.toJsonBytes(serverInfo))));

        assertEquals(executor.getServerInfo(new URI("http://example.com")).getUptime().get(), Duration.valueOf("2m"));
    }
}
