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
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionRange;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransform;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransformArgument;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransformProperties;
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
    public void testDecodeByTransformId()
    {
        MaterializedViewPartitionTransformRegistry registry = new MaterializedViewPartitionTransformRegistry(NATIVE_NAMESPACE);
        registry.addProvider(new TestingProvider());

        assertFalse(registry.decode(ImmutableMap.of()).isPresent());
        assertEquals(registry.decode(ImmutableMap.of("transform", "test_bucket")).get().getTransformId(), "test_bucket");
        expectThrows(IllegalArgumentException.class, () -> registry.decode(ImmutableMap.of("transform", "unknown")));
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
        @Override
        public Set<String> getTransformIds()
        {
            return ImmutableSet.of("test_bucket");
        }

        @Override
        public Set<String> getFunctionNames()
        {
            return ImmutableSet.of("test_bucket");
        }

        @Override
        public Optional<MaterializedViewPartitionTransform> analyze(FunctionMetadata function, List<MaterializedViewPartitionTransformArgument> arguments)
        {
            return Optional.of(new TestingTransform());
        }

        @Override
        public MaterializedViewPartitionTransform decode(Map<String, String> properties)
        {
            return new TestingTransform();
        }
    }

    private static final class TestingTransform
            implements MaterializedViewPartitionTransform
    {
        @Override
        public String getTransformId()
        {
            return "test_bucket";
        }

        @Override
        public MaterializedViewPartitionTransformProperties getProperties()
        {
            return new MaterializedViewPartitionTransformProperties(
                    0,
                    ImmutableSet.of(1),
                    MaterializedViewPartitionTransformProperties.MappingCardinality.MANY_TO_ONE,
                    1);
        }

        @Override
        public Map<String, String> encode()
        {
            return ImmutableMap.of("transform", "test_bucket");
        }

        @Override
        public boolean isCanonicalTargetValue(ConstantExpression value)
        {
            return true;
        }

        @Override
        public ConstantExpression apply(ConstantExpression sourceValue)
        {
            return sourceValue;
        }

        @Override
        public MaterializedViewPartitionRange getSourceRange(ConstantExpression targetValue)
        {
            return new MaterializedViewPartitionRange(
                    targetValue,
                    new ConstantExpression(Optional.empty(), utf8Slice("upper"), VARCHAR));
        }
    }
}
