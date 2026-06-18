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

import com.facebook.presto.spi.QueryId;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static org.testng.Assert.assertTrue;

public class TestCompression
{
    @Test
    public void testCompressionWithJson()
            throws Exception
    {
        // Enable HTTP/2 and compression with minimal threshold to guarantee compression
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("reactor.netty-http-client-enabled", "true")         // Enable HTTP/2 client
                .put("reactor.enable-http2-compression", "true")          // Enable compression
                .put("reactor.payload-compression-threshold", "1kB")      // 1kB (minimum allowed by @Min(1024))
                .put("reactor.compression-ratio-threshold", "0.01")        // 1% compression savings threshold
                .build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(properties)) {
            // Run multiple queries to test different data sizes and patterns
            String[] queries = {
                    "SELECT count(*) FROM tpch.tiny.nation",
                    "SELECT * FROM tpch.tiny.region",
                    "SELECT n.name, r.name FROM tpch.tiny.nation n JOIN tpch.tiny.region r ON n.regionkey = r.regionkey",
                    "SELECT * FROM tpch.tiny.nation ORDER BY nationkey"
            };

            for (String query : queries) {
                QueryId queryId = queryRunner.executeWithQueryId(queryRunner.getDefaultSession(), query).getQueryId();
                assertTrue(queryRunner.getQueryInfo(queryId).getState().isDone());
            }
        }
    }

    @Test
    public void testCompressionWithThrift()
            throws Exception
    {
        // Enable HTTP/2 and compression with minimal threshold to guarantee compression
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("reactor.netty-http-client-enabled", "true")         // Enable HTTP/2 client
                .put("reactor.enable-http2-compression", "true")          // Enable compression
                .put("reactor.payload-compression-threshold", "1kB")      // 1kB (minimum allowed by @Min(1024))
                .put("reactor.compression-ratio-threshold", "0.01")        // 1% compression savings threshold
                .put("experimental.internal-communication.task-info-response-thrift-serde-enabled", "true")
                .put("experimental.internal-communication.task-update-request-thrift-serde-enabled", "true")
                .build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(properties)) {
            // Run multiple queries to test different data sizes and patterns
            String[] queries = {
                    "SELECT count(*) FROM tpch.tiny.nation",
                    "SELECT * FROM tpch.tiny.region",
                    "SELECT n.name, r.name FROM tpch.tiny.nation n JOIN tpch.tiny.region r ON n.regionkey = r.regionkey",
                    "SELECT * FROM tpch.tiny.nation ORDER BY nationkey"
            };

            for (String query : queries) {
                QueryId queryId = queryRunner.executeWithQueryId(queryRunner.getDefaultSession(), query).getQueryId();
                assertTrue(queryRunner.getQueryInfo(queryId).getState().isDone());
            }
        }
    }
}
