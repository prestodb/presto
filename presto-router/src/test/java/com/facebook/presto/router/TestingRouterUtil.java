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

import com.facebook.presto.server.testing.TestingPrestoServer;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class TestingRouterUtil
{
    private TestingRouterUtil()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static File getConfigFile(List<TestingPrestoServer> servers, File tempFile)
            throws IOException
    {
        FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
        List<String> serverURIs = servers.stream()
                .map(TestingPrestoServer::getBaseUrl)
                .map(URI::toString)
                .collect(Collectors.toList());
        Map<String, List<String>> groups = ImmutableMap.of("all", serverURIs);
        TestingRouterConfig config = new TestingRouterConfig(groups, "all", "ROUND_ROBIN");
        fileOutputStream.write(config.get().toString().getBytes(UTF_8));
        fileOutputStream.close();
        return tempFile;
    }
}
