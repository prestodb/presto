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
package com.facebook.presto.router;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.router.spec.GroupSpec;
import com.facebook.presto.router.spec.RouterSpec;
import com.facebook.presto.router.spec.SelectorRuleSpec;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.router.scheduler.SchedulerType.ROUND_ROBIN;
import static java.lang.String.format;
import static java.sql.DriverManager.getConnection;

public class TestingRouterUtil
{
    private TestingRouterUtil()
    {
    }

    public static File getConfigFile(List<TestingPrestoServer> servers, File tempFile)
            throws IOException
    {
        List<URI> serverURIs = servers.stream()
                .map(TestingPrestoServer::getBaseUrl)
                .collect(Collectors.toList());
        RouterSpec spec = new RouterSpec(ImmutableList.of(new GroupSpec("all", serverURIs, Optional.empty(), Optional.empty())),
                    ImmutableList.of(new SelectorRuleSpec(Optional.empty(), Optional.empty(), Optional.empty(), "all")),
                    Optional.of(ROUND_ROBIN),
                    Optional.empty(),
                    Optional.empty());
        JsonCodec<RouterSpec> codec = jsonCodec(RouterSpec.class);
        Files.write(tempFile.toPath(), codec.toBytes(spec));
        return tempFile;
    }

    public static TestingPrestoServer createPrestoServer()
            throws Exception
    {
        TestingPrestoServer server = new TestingPrestoServer();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
        server.refreshNodes();

        return server;
    }

    public static Connection createConnection(URI uri)
            throws SQLException
    {
        String url = format("jdbc:presto://%s:%s", uri.getHost(), uri.getPort());
        return getConnection(url, "test", null);
    }
}
