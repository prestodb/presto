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
package com.facebook.presto.iceberg;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.azure.HiveAzureConfig;
import com.facebook.presto.hive.azure.HiveAzureConfigurationInitializer;
import com.facebook.presto.hive.gcs.HiveGcsConfig;
import com.facebook.presto.hive.gcs.HiveGcsConfigurationInitializer;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.iceberg.statistics.StatisticsFileCache;
import com.facebook.presto.metadata.BuiltInProcedureRegistry;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.hive.HiveTestUtils.FILTER_STATS_CALCULATOR_SERVICE;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_RESOLUTION;
import static com.facebook.presto.hive.HiveTestUtils.ROW_EXPRESSION_SERVICE;
import static com.facebook.presto.iceberg.CatalogType.REST;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

/**
 * A catalog with nested namespaces is walked one level at a time, so a namespace reported by its
 * parent's listing can be dropped before its own children are asked for. Listing the schemas of
 * the catalog has to survive that, because every statement that needs the schema list -- including
 * the check CREATE SCHEMA makes before creating one -- would otherwise fail over an unrelated
 * namespace going away.
 */
public class TestIcebergNestedNamespaceListing
{
    private static final ConnectorSession SESSION = new TestingConnectorSession(ImmutableList.of());

    @Test
    public void testNamespaceThatDisappearsMidWalkIsSkipped()
    {
        // "dropped" has no entry, so asking for its children throws, the way a catalog answers for
        // a namespace it no longer knows.
        ConnectorMetadata metadata = nestedNamespaceMetadata(new TestingNamespaces(ImmutableMap.of(
                Namespace.empty(), ImmutableList.of(Namespace.of("kept"), Namespace.of("dropped")),
                Namespace.of("kept"), ImmutableList.of())));

        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of("kept", "dropped"));
    }

    @Test
    public void testNamespaceThatDisappearsBelowTheTopLevelIsSkipped()
    {
        // The same one level down: what is dropped is a nested namespace, so the namespaces walked
        // before it, and its own parent, still have to come back.
        ConnectorMetadata metadata = nestedNamespaceMetadata(new TestingNamespaces(ImmutableMap.of(
                Namespace.empty(), ImmutableList.of(Namespace.of("a")),
                Namespace.of("a"), ImmutableList.of(Namespace.of("a", "b")))));

        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of("a", "a.b"));
    }

    @Test
    public void testNestedNamespacesAreListed()
    {
        ConnectorMetadata metadata = nestedNamespaceMetadata(new TestingNamespaces(ImmutableMap.of(
                Namespace.empty(), ImmutableList.of(Namespace.of("a")),
                Namespace.of("a"), ImmutableList.of(Namespace.of("a", "b")),
                Namespace.of("a", "b"), ImmutableList.of())));

        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of("a", "a.b"));
    }

    @Test
    public void testFailureOtherThanAMissingNamespaceIsNotSwallowed()
    {
        // Only a namespace that is not there is skipped. Anything else the catalog answers, such as
        // the caller not being allowed to see it, is the answer to the whole listing.
        SupportsNamespaces namespaces = new TestingNamespaces(ImmutableMap.of(
                Namespace.empty(), ImmutableList.of(Namespace.of("a"))))
        {
            @Override
            public List<Namespace> listNamespaces(Namespace namespace)
            {
                if (namespace.equals(Namespace.of("a"))) {
                    throw new ForbiddenException("Forbidden: User not authorized");
                }
                return super.listNamespaces(namespace);
            }
        };
        ConnectorMetadata metadata = nestedNamespaceMetadata(namespaces);

        assertThrows(ForbiddenException.class, () -> metadata.listSchemaNames(SESSION));
    }

    @Test
    public void testOnlyTopLevelNamespacesAreListedWhenNestingIsDisabled()
    {
        // Without nested namespaces the tree is never walked, so only the top level is listed and
        // nothing below it is reached.
        ConnectorMetadata metadata = metadata(new TestingNamespaces(ImmutableMap.of(
                Namespace.empty(), ImmutableList.of(Namespace.of("a")),
                Namespace.of("a"), ImmutableList.of(Namespace.of("a", "b")))), false);

        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of("a"));
    }

    private static ConnectorMetadata nestedNamespaceMetadata(SupportsNamespaces namespaces)
    {
        return metadata(namespaces, true);
    }

    private static ConnectorMetadata metadata(SupportsNamespaces namespaces, boolean nestedNamespaceEnabled)
    {
        IcebergConfig icebergConfig = new IcebergConfig().setCatalogType(REST);
        IcebergNativeCatalogFactory catalogFactory = new TestingCatalogFactory(icebergConfig, namespaces, nestedNamespaceEnabled);

        return new IcebergNativeMetadataFactory(
                icebergConfig,
                catalogFactory,
                FUNCTION_AND_TYPE_MANAGER,
                new BuiltInProcedureRegistry(MetadataManager.createTestMetadataManager().getFunctionAndTypeManager()),
                FUNCTION_RESOLUTION,
                ROW_EXPRESSION_SERVICE,
                jsonCodec(CommitTaskData.class),
                columnMappingsCodec(),
                schemaTableNamesCodec(),
                new NodeVersion("test_version"),
                FILTER_STATS_CALCULATOR_SERVICE,
                new StatisticsFileCache(CacheBuilder.newBuilder().build()),
                new IcebergTableProperties(icebergConfig))
                .create();
    }

    private static JsonCodec<List<MaterializedViewDefinition.ColumnMapping>> columnMappingsCodec()
    {
        return jsonCodec(new TypeToken<List<MaterializedViewDefinition.ColumnMapping>>() {});
    }

    private static JsonCodec<List<SchemaTableName>> schemaTableNamesCodec()
    {
        return jsonCodec(new TypeToken<List<SchemaTableName>>() {});
    }

    /**
     * Hands out {@code namespaces} instead of connecting to a catalog, so the listing can be driven
     * from the namespaces a test declares.
     */
    private static class TestingCatalogFactory
            extends IcebergNativeCatalogFactory
    {
        private final SupportsNamespaces namespaces;
        private final boolean nestedNamespaceEnabled;

        TestingCatalogFactory(IcebergConfig icebergConfig, SupportsNamespaces namespaces, boolean nestedNamespaceEnabled)
        {
            super(icebergConfig,
                    new IcebergCatalogName("iceberg"),
                    new PrestoS3ConfigurationUpdater(new HiveS3Config()),
                    new HiveGcsConfigurationInitializer(new HiveGcsConfig()),
                    new HiveAzureConfigurationInitializer(new HiveAzureConfig()));
            this.namespaces = namespaces;
            this.nestedNamespaceEnabled = nestedNamespaceEnabled;
        }

        @Override
        public SupportsNamespaces getNamespaces(ConnectorSession session)
        {
            return namespaces;
        }

        @Override
        public boolean isNestedNamespaceEnabled()
        {
            return nestedNamespaceEnabled;
        }
    }

    /**
     * A catalog's namespaces, declared as the children of each one. A namespace with no entry is
     * one the catalog does not know, and asking for its children throws, as a catalog does.
     */
    private static class TestingNamespaces
            implements SupportsNamespaces
    {
        private final Map<Namespace, List<Namespace>> children;

        TestingNamespaces(Map<Namespace, List<Namespace>> children)
        {
            this.children = ImmutableMap.copyOf(children);
        }

        @Override
        public List<Namespace> listNamespaces(Namespace namespace)
        {
            List<Namespace> namespaces = children.get(namespace);
            if (namespaces == null) {
                throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
            }
            return namespaces;
        }

        @Override
        public void createNamespace(Namespace namespace, Map<String, String> metadata)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> loadNamespaceMetadata(Namespace namespace)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean dropNamespace(Namespace namespace)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean setProperties(Namespace namespace, Map<String, String> properties)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeProperties(Namespace namespace, Set<String> properties)
        {
            throw new UnsupportedOperationException();
        }
    }
}
