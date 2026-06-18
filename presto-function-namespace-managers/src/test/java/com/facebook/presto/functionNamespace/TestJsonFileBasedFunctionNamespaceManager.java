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
import com.google.common.io.Resources;
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
        // 1. Test loading of a single json file
        final String jsonFileName = "json_udf_function_definition.json";
        final int fileFunctionCount = 9;

        JsonFileBasedFunctionNamespaceManager jsonFileBasedFunctionNameSpaceManager = createFunctionNamespaceManager(jsonFileName);
        Collection<SqlInvokedFunction> functionList = jsonFileBasedFunctionNameSpaceManager.listFunctions(Optional.empty(), Optional.empty());
        assertEquals(functionList.size(), fileFunctionCount);

        // 2. Test loading of json files in a directory
        final String jsonDirName = "json_udf_function_definition_dir";
        final int dirFunctionCount = 7;

        jsonFileBasedFunctionNameSpaceManager = createFunctionNamespaceManager(jsonDirName);
        functionList = jsonFileBasedFunctionNameSpaceManager.listFunctions(Optional.empty(), Optional.empty());
        assertEquals(functionList.size(), dirFunctionCount);
    }

    private JsonFileBasedFunctionNamespaceManager createFunctionNamespaceManager(String filePath)
    {
        Bootstrap app = new Bootstrap(
                new JsonFileBasedFunctionNamespaceManagerModule(TEST_CATALOG),
                new NoopSqlFunctionExecutorsModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(
                        ImmutableMap.of(
                                "json-based-function-manager.path-to-function-definition", getResourceFilePath(filePath),
                                "supported-function-languages", "CPP"))
                .initialize();
        return injector.getInstance(JsonFileBasedFunctionNamespaceManager.class);
    }

    private static String getResourceFilePath(String resourceName)
    {
        return Resources.getResource(resourceName).getFile();
    }
}
