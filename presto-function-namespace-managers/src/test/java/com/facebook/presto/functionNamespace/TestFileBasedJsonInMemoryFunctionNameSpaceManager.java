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

import com.facebook.presto.functionNamespace.execution.NoopSqlFunctionExecutor;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.functionNamespace.json.FileBasedJsonConfig;
import com.facebook.presto.functionNamespace.json.FileBasedJsonInMemoryFunctionNameSpaceManager;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.Optional;

import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.TEST_CATALOG;

public class TestFileBasedJsonInMemoryFunctionNameSpaceManager
{
    @Test
    public void testCreateFunction()
    {
        FileBasedJsonInMemoryFunctionNameSpaceManager fileBasedJsonInMemoryFunctionNameSpaceManager = createFunctionNamespaceManager();
        Iterator<SqlInvokedFunction> itr = (fileBasedJsonInMemoryFunctionNameSpaceManager.listFunctions(Optional.empty(), Optional.empty())).iterator();

        while(itr.hasNext()) {
            SqlInvokedFunction sqlInvokedFunction = itr.next();
            System.out.println("Function name->" + sqlInvokedFunction.getSignature().toString());
        }
    }

    private FileBasedJsonInMemoryFunctionNameSpaceManager createFunctionNamespaceManager()
    {
        FileBasedJsonConfig fileBasedJsonConfig = new FileBasedJsonConfig().setFunctionDefinitionFile(getPath("functionDefinition.json"));

        return new FileBasedJsonInMemoryFunctionNameSpaceManager(
                TEST_CATALOG,
                new SqlFunctionExecutors(
                        ImmutableMap.of(new RoutineCharacteristics.Language("CPP"), FunctionImplementationType.CPP),
                        new NoopSqlFunctionExecutor()),
                new SqlInvokedFunctionNamespaceManagerConfig(),
                fileBasedJsonConfig);
    }

    private String getPath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }
}
