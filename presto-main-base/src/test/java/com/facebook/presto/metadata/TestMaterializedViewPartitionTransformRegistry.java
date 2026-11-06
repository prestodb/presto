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
package com.facebook.presto.metadata;

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionMapping;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransform;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransformArgument;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransformProvider;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.spi.function.FunctionImplementationType.JAVA;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.expectThrows;

public class TestMaterializedViewPartitionTransformRegistry
{
    private static final CatalogSchemaName NATIVE_NAMESPACE = new CatalogSchemaName("native", "default");

    @Test
    public void testFunctionLookupUsesJavaAndConfiguredDefaultNamespaces()
    {
        MaterializedViewPartitionTransformRegistry registry = new MaterializedViewPartitionTransformRegistry(NATIVE_NAMESPACE);
        TestingProvider provider = new TestingProvider();
        registry.addProvider(provider);

        assertEquals(registry.getProvider(functionMetadata(JAVA_BUILTIN_NAMESPACE, true)), Optional.of(provider));
        assertEquals(registry.getProvider(functionMetadata(NATIVE_NAMESPACE, true)), Optional.of(provider));
        assertFalse(registry.getProvider(functionMetadata(new CatalogSchemaName("other", "default"), true)).isPresent());
    }

    @Test
    public void testRequiresDeterministicScalar()
    {
        MaterializedViewPartitionTransformRegistry registry = new MaterializedViewPartitionTransformRegistry(NATIVE_NAMESPACE);
        registry.addProvider(new TestingProvider());

        expectThrows(IllegalArgumentException.class, () -> registry.getProvider(functionMetadata(JAVA_BUILTIN_NAMESPACE, false)));
    }

    @Test
    public void testRequiresNullPropagation()
    {
        MaterializedViewPartitionTransformRegistry registry = new MaterializedViewPartitionTransformRegistry(NATIVE_NAMESPACE);
        registry.addProvider(new TestingProvider());

        expectThrows(IllegalArgumentException.class, () -> registry.getProvider(functionMetadata(JAVA_BUILTIN_NAMESPACE, true, true)));
    }

    @Test
    public void testDecodeByTransformName()
    {
        MaterializedViewPartitionTransformRegistry registry = new MaterializedViewPartitionTransformRegistry(NATIVE_NAMESPACE);
        registry.addProvider(new TestingProvider());

        expectThrows(IllegalArgumentException.class, () -> registry.decode(ImmutableMap.of()));
        assertEquals(registry.decode(ImmutableMap.of("transform", "test_bucket")).encode(), ImmutableMap.of("transform", "test_bucket"));
        expectThrows(IllegalArgumentException.class, () -> registry.decode(ImmutableMap.of("transform", "unknown")));
    }

    @Test
    public void testValidatesEncodedTransformName()
    {
        MaterializedViewPartitionTransformRegistry registry = new MaterializedViewPartitionTransformRegistry(NATIVE_NAMESPACE);
        TestingProvider provider = new TestingProvider();
        registry.addProvider(provider);

        assertEquals(registry.encode(provider, new TestingTransform("test_bucket")), ImmutableMap.of("transform", "test_bucket"));
        expectThrows(IllegalArgumentException.class, () -> registry.encode(provider, new TestingTransform("unknown")));
    }

    @Test
    public void testValidatesDecodedTransformName()
    {
        MaterializedViewPartitionTransformRegistry registry = new MaterializedViewPartitionTransformRegistry(NATIVE_NAMESPACE);
        registry.addProvider(new TestingProvider("other"));

        expectThrows(IllegalArgumentException.class, () -> registry.decode(ImmutableMap.of("transform", "test_bucket")));
    }

    @Test
    public void testRejectsDuplicateOwnership()
    {
        MaterializedViewPartitionTransformRegistry registry = new MaterializedViewPartitionTransformRegistry(NATIVE_NAMESPACE);
        registry.addProvider(new TestingProvider());

        expectThrows(IllegalArgumentException.class, () -> registry.addProvider(new TestingProvider()));
    }

    private static FunctionMetadata functionMetadata(CatalogSchemaName namespace, boolean deterministic)
    {
        return functionMetadata(namespace, deterministic, false);
    }

    private static FunctionMetadata functionMetadata(CatalogSchemaName namespace, boolean deterministic, boolean calledOnNullInput)
    {
        return new FunctionMetadata(
                QualifiedObjectName.valueOf(namespace, "test_bucket"),
                ImmutableList.of(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()),
                VARCHAR.getTypeSignature(),
                SCALAR,
                JAVA,
                deterministic,
                calledOnNullInput);
    }

    private static final class TestingProvider
            implements MaterializedViewPartitionTransformProvider
    {
        private final String encodedName;

        private TestingProvider()
        {
            this("test_bucket");
        }

        private TestingProvider(String encodedName)
        {
            this.encodedName = encodedName;
        }

        @Override
        public Set<String> getFunctionNames()
        {
            return ImmutableSet.of("test_bucket");
        }

        @Override
        public MaterializedViewPartitionTransform analyze(FunctionMetadata function, List<MaterializedViewPartitionTransformArgument> arguments)
        {
            return new TestingTransform(encodedName);
        }

        @Override
        public MaterializedViewPartitionTransform decode(Map<String, String> properties)
        {
            return new TestingTransform(encodedName);
        }
    }

    private static final class TestingTransform
            implements MaterializedViewPartitionTransform
    {
        private final String encodedName;

        private TestingTransform(String encodedName)
        {
            this.encodedName = encodedName;
        }

        @Override
        public Map<String, String> encode()
        {
            return ImmutableMap.of("transform", encodedName);
        }

        @Override
        public MaterializedViewPartitionMapping getSourceMapping(ConstantExpression targetValue)
        {
            return MaterializedViewPartitionMapping.halfOpenRange(
                    targetValue,
                    new ConstantExpression(Optional.empty(), utf8Slice("upper"), VARCHAR));
        }
    }
}
