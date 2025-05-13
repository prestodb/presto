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

import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

public class TestCoordinatorPluginIntegration
{
    private final TestingPrestoServer testingPrestoServer;

    private TestCoordinatorPluginIntegration()
            throws Exception
    {
        this.testingPrestoServer = new TestingPrestoServer(
                true,
                ImmutableMap.<String, String>builder()
                        .put("plugin.bundles", "../presto-tests/target/")
                        .build(),
                null, null, new SqlParserOptions(), ImmutableList.of());
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp =
            "This exception confirms that TestingCoordinatorPlugin was called.")
    public void testCoordinatorPluginIntegration()
            throws Exception
    {
        PluginManager pluginManager = testingPrestoServer.getPluginManager();
        pluginManager.loadPlugins();
    }
}
