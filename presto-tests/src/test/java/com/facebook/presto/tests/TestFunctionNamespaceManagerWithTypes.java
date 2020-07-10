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
package com.facebook.presto.tests;

import com.facebook.presto.common.function.QualifiedFunctionName;
import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarcharEnumType.VarcharEnumMap;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.function.AlterRoutineCharacteristics;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionHandleResolver;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceManagerFactory;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.type.VarcharEnumParametricType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.singletonList;

@Test(singleThreaded = true)
public class TestFunctionNamespaceManagerWithTypes
        extends AbstractTestQueryFramework
{
    static class TestEnumsFunctionNamespaceManager
            implements FunctionNamespaceManager<SqlInvokedFunction>
    {
        private List<ParametricType> types = ImmutableList.of(
                new VarcharEnumParametricType("animal", new VarcharEnumMap(ImmutableMap.of(
                        "CAT", "cat",
                        "DOG", "dog"))));

        @Override
        public ParametricType getParametricType(TypeSignature typeSignature)
        {
            return types.stream()
                    .filter(type -> type.getName().equals(typeSignature.getBase()))
                    .findFirst()
                    .orElse(null);
        }

        // None of the methods below are supported:
        // ------------------------------------------

        @Override
        public void dropFunction(QualifiedFunctionName functionName, Optional parameterTypes, boolean exists)
        {
        }

        @Override
        public void alterFunction(QualifiedFunctionName functionName, Optional parameterTypes, AlterRoutineCharacteristics alterRoutineCharacteristics)
        {
        }

        @Override
        public FunctionNamespaceTransactionHandle beginTransaction()
        {
            return null;
        }

        @Override
        public void commit(FunctionNamespaceTransactionHandle transactionHandle)
        {
        }

        @Override
        public void abort(FunctionNamespaceTransactionHandle transactionHandle)
        {
        }

        @Override
        public void createFunction(SqlInvokedFunction function, boolean replace)
        {
        }

        @Override
        public Collection<SqlInvokedFunction> listFunctions()
        {
            return Collections.emptyList();
        }

        @Override
        public Collection<SqlInvokedFunction> getFunctions(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, QualifiedFunctionName functionName)
        {
            return Collections.emptyList();
        }

        @Override
        public FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature)
        {
            return null;
        }

        @Override
        public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
        {
            return null;
        }

        @Override
        public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
        {
            return null;
        }
    }

    static class TestEnumsFunctionNamespaceManagerFactory
            implements FunctionNamespaceManagerFactory
    {
        @Override
        public String getName()
        {
            return "testEnums";
        }

        @Override
        public FunctionHandleResolver getHandleResolver()
        {
            return new SqlFunctionHandle.Resolver();
        }

        @Override
        public FunctionNamespaceManager<?> create(String catalogName, Map<String, String> config)
        {
            return new TestEnumsFunctionNamespaceManager();
        }
    }

    static class EnumTestingPlugin
            implements Plugin
    {
        @Override
        public Iterable<FunctionNamespaceManagerFactory> getFunctionNamespaceManagerFactories()
        {
            return singletonList(new TestEnumsFunctionNamespaceManagerFactory());
        }
    }

    protected TestFunctionNamespaceManagerWithTypes()
    {
        super(TestFunctionNamespaceManagerWithTypes::createQueryRunner);
    }

    private static QueryRunner createQueryRunner()
    {
        QueryRunner queryRunner = new LocalQueryRunner(testSessionBuilder()
                .build());
        queryRunner.installPlugin(new EnumTestingPlugin());
        queryRunner.loadFunctionNamespaceManager("testEnums", "testEnums", ImmutableMap.of());
        return queryRunner;
    }

    @Test
    public void testTypeQuery()
    {
        assertQueryResultUnordered("SELECT animal.dog", singletonList(ImmutableList.of("dog")));
    }
}
