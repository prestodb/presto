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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.functionNamespace.execution.NoopSqlFunctionExecutorsModule;
import com.facebook.presto.functionNamespace.json.JsonFileBasedFunctionNamespaceManager;
import com.facebook.presto.functionNamespace.json.JsonFileBasedFunctionNamespaceManagerModule;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Injector;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.TEST_CATALOG;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

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

    @Test
    public void testLoadVariableArityAnyFunction()
    {
        JsonFileBasedFunctionNamespaceManager manager = createFunctionNamespaceManager("json_udf_variable_arity.json");
        SqlInvokedFunction function = manager.listFunctions(Optional.empty(), Optional.empty()).stream()
                .filter(candidate -> candidate.getSignature().getName().getObjectName().equals("format_any"))
                .collect(onlyElement());

        assertTrue(function.getVariableArity());
        assertEquals(function.getSignature().getArgumentTypes().get(function.getSignature().getArgumentTypes().size() - 1).getBase(), "__ANY__");
    }

    @Test
    public void testAnyTypeRejectedOutsideVariadicTail()
    {
        try {
            createFunctionNamespaceManager("json_udf_invalid_any.json");
            fail("expected loading a function that misuses the 'any' type to fail");
        }
        catch (RuntimeException e) {
            String trace = getStackTraceAsString(e);
            assertTrue(
                    trace.contains("only allowed as the last argument of a variable-arity function"),
                    "unexpected failure: " + trace);
        }
    }

    @Test
    public void testVariableArityAnyCallResolvesEndToEnd()
    {
        JsonFileBasedFunctionNamespaceManager manager = createFunctionNamespaceManager("json_udf_variable_arity.json");
        QualifiedObjectName name = QualifiedObjectName.valueOf(TEST_CATALOG, "test_schema", "format_any");
        List<TypeSignature> callArgumentTypes = ImmutableList.of(
                parseTypeSignature("varchar"),
                parseTypeSignature("double"),
                parseTypeSignature("varchar"));
        // The bound signature the analyzer produces for format_any(varchar, double, varchar).
        Signature boundSignature = new Signature(name, SCALAR, parseTypeSignature("varchar"), callArgumentTypes);

        FunctionHandle handle = manager.getFunctionHandle(Optional.empty(), boundSignature);
        FunctionMetadata metadata = manager.getFunctionMetadata(handle);

        assertEquals(metadata.getName(), name);
        // Metadata carries the call-arity argument types, so per-argument analysis lines up with the call.
        assertEquals(metadata.getArgumentTypes(), callArgumentTypes);
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
