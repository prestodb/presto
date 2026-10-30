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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.materializedview.MaterializedViewAggregationMergeStrategy;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransform;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransformArgument;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransformProvider;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
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
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestMaterializedViewPluginRegistration
{
    @Test
    public void testPluginManagerRegistersMaterializedViewExtensions()
    {
        MaterializedViewPartitionTransformProvider provider = new TestingProvider();
        MaterializedViewAggregationMergeStrategy strategy =
                new MaterializedViewAggregationMergeStrategy("test_partial", Optional.of("test_merge"), false);
        Plugin plugin = new Plugin()
        {
            @Override
            public Iterable<MaterializedViewAggregationMergeStrategy> getMaterializedViewAggregationMergeStrategies()
            {
                return ImmutableList.of(strategy);
            }

            @Override
            public Iterable<MaterializedViewPartitionTransformProvider> getMaterializedViewPartitionTransformProviders()
            {
                return ImmutableList.of(provider);
            }
        };

        try (LocalQueryRunner queryRunner = new LocalQueryRunner(testSessionBuilder().build())) {
            queryRunner.installPlugin(plugin);

            FunctionAndTypeManager functionAndTypeManager = queryRunner.getMetadata().getFunctionAndTypeManager();
            assertEquals(
                    functionAndTypeManager.getMaterializedViewAggregationMergeRegistry().getStrategy("test_partial"),
                    Optional.of(strategy));
            assertEquals(
                    functionAndTypeManager.getMaterializedViewPartitionTransformRegistry().getProvider(functionMetadata()),
                    Optional.of(provider));
        }
    }

    private static FunctionMetadata functionMetadata()
    {
        return new FunctionMetadata(
                QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "test_transform"),
                ImmutableList.of(VARCHAR.getTypeSignature()),
                VARCHAR.getTypeSignature(),
                SCALAR,
                JAVA,
                true,
                false);
    }

    private static final class TestingProvider
            implements MaterializedViewPartitionTransformProvider
    {
        @Override
        public Set<String> getTransformIds()
        {
            return ImmutableSet.of("test_transform");
        }

        @Override
        public Set<String> getFunctionNames()
        {
            return ImmutableSet.of("test_transform");
        }

        @Override
        public Optional<MaterializedViewPartitionTransform> analyze(
                FunctionMetadata function,
                List<MaterializedViewPartitionTransformArgument> arguments)
        {
            return Optional.empty();
        }

        @Override
        public MaterializedViewPartitionTransform decode(Map<String, String> properties)
        {
            throw new UnsupportedOperationException();
        }
    }
}
