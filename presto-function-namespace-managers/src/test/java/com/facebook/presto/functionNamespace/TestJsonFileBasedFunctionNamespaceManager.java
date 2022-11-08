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
package com.facebook.presto.functionNamespace;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.functionNamespace.execution.NoopSqlFunctionExecutorsModule;
import com.facebook.presto.functionNamespace.json.JsonFileBasedFunctionNamespaceManager;
import com.facebook.presto.functionNamespace.json.JsonFileBasedFunctionNamespaceManagerModule;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Optional;

import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.TEST_CATALOG;
import static org.testng.Assert.assertEquals;

public class TestJsonFileBasedFunctionNamespaceManager
{
    @Test
    public void testLoadFunctions()
    {
        JsonFileBasedFunctionNamespaceManager jsonFileBasedFunctionNameSpaceManager = createFunctionNamespaceManager();
        Collection<SqlInvokedFunction> functionList = jsonFileBasedFunctionNameSpaceManager.listFunctions(Optional.empty(), Optional.empty());
        assertEquals(functionList.size(), 7);
    }

    private JsonFileBasedFunctionNamespaceManager createFunctionNamespaceManager()
    {
        Bootstrap app = new Bootstrap(
                new JsonFileBasedFunctionNamespaceManagerModule(TEST_CATALOG),
                new NoopSqlFunctionExecutorsModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(ImmutableMap.of("json-based-function-manager.path-to-function-definition", getPath("json_udf_function_definition.json"), "supported-function-languages", "CPP"))
                .initialize();
        return injector.getInstance(JsonFileBasedFunctionNamespaceManager.class);
    }

    private String getPath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }
}
