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

package com.facebook.presto.hive.functions;

import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.plugin.memory.MemoryPlugin;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.google.inject.Key;

import java.util.Collections;
import java.util.Map;

public final class HiveFunctionsTestUtils
{
    private HiveFunctionsTestUtils() {}

    public static TestingPrestoServer createTestingPrestoServer()
            throws Exception
    {
        TestingPrestoServer server = new TestingPrestoServer();
        server.installPlugin(new MemoryPlugin());
        server.installPlugin(new HiveFunctionNamespacePlugin());
        server.createCatalog("memory", "memory");
        FunctionAndTypeManager functionAndTypeManager = server.getInstance(Key.get(FunctionAndTypeManager.class));
        functionAndTypeManager.loadFunctionNamespaceManager(
                "hive-functions",
                "hive",
                getNamespaceManagerCreationProperties());
        server.refreshNodes();
        return server;
    }

    public static Map<String, String> getNamespaceManagerCreationProperties()
    {
        return Collections.emptyMap();
    }
}
