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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.prestosql.PagesIndexPageSorter;
import io.prestosql.block.BlockEncodingManager;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.operator.PagesIndex;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.orc.DwrfPageSourceFactory;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.plugin.hive.parquet.ParquetPageSourceFactory;
import io.prestosql.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.prestosql.plugin.hive.s3.HiveS3Config;
import io.prestosql.plugin.hive.s3.PrestoS3ConfigurationUpdater;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.NamedTypeSignature;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.testing.TestingConnectorSession;
import io.prestosql.type.TypeRegistry;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import static io.prestosql.spi.type.Decimals.encodeScaledValue;
import static java.util.stream.Collectors.toList;

public final class HiveTestUtils
{
    private HiveTestUtils()
    {
    }

    public static final ConnectorSession SESSION = new TestingConnectorSession(
            new HiveSessionProperties(new HiveClientConfig(), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());

    public static final TypeRegistry TYPE_MANAGER = new TypeRegistry();

    static {
        // associate TYPE_MANAGER with a function registry
        new FunctionRegistry(TYPE_MANAGER, new BlockEncodingManager(TYPE_MANAGER), new FeaturesConfig());
    }

    public static final HdfsEnvironment HDFS_ENVIRONMENT = createTestHdfsEnvironment(new HiveClientConfig());

    public static final PageSorter PAGE_SORTER = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));

    public static Set<HivePageSourceFactory> getDefaultHiveDataStreamFactories(HiveClientConfig hiveClientConfig)
    {
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        HdfsEnvironment testHdfsEnvironment = createTestHdfsEnvironment(hiveClientConfig);
        return ImmutableSet.<HivePageSourceFactory>builder()
                .add(new RcFilePageSourceFactory(TYPE_MANAGER, testHdfsEnvironment, stats))
                .add(new OrcPageSourceFactory(TYPE_MANAGER, hiveClientConfig, testHdfsEnvironment, stats))
                .add(new DwrfPageSourceFactory(TYPE_MANAGER, testHdfsEnvironment, stats))
                .add(new ParquetPageSourceFactory(TYPE_MANAGER, testHdfsEnvironment, stats))
                .build();
    }

    public static Set<HiveRecordCursorProvider> getDefaultHiveRecordCursorProvider(HiveClientConfig hiveClientConfig)
    {
        HdfsEnvironment testHdfsEnvironment = createTestHdfsEnvironment(hiveClientConfig);
        return ImmutableSet.<HiveRecordCursorProvider>builder()
                .add(new GenericHiveRecordCursorProvider(testHdfsEnvironment))
                .build();
    }

    public static Set<HiveFileWriterFactory> getDefaultHiveFileWriterFactories(HiveClientConfig hiveClientConfig)
    {
        HdfsEnvironment testHdfsEnvironment = createTestHdfsEnvironment(hiveClientConfig);
        return ImmutableSet.<HiveFileWriterFactory>builder()
                .add(new RcFileFileWriterFactory(testHdfsEnvironment, TYPE_MANAGER, new NodeVersion("test_version"), hiveClientConfig, new FileFormatDataSourceStats()))
                .add(getDefaultOrcFileWriterFactory(hiveClientConfig))
                .build();
    }

    public static OrcFileWriterFactory getDefaultOrcFileWriterFactory(HiveClientConfig hiveClientConfig)
    {
        HdfsEnvironment testHdfsEnvironment = createTestHdfsEnvironment(hiveClientConfig);
        return new OrcFileWriterFactory(
                testHdfsEnvironment,
                TYPE_MANAGER,
                new NodeVersion("test_version"),
                hiveClientConfig,
                new FileFormatDataSourceStats(),
                new OrcFileWriterConfig());
    }

    public static List<Type> getTypes(List<? extends ColumnHandle> columnHandles)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ColumnHandle columnHandle : columnHandles) {
            types.add(TYPE_MANAGER.getType(((HiveColumnHandle) columnHandle).getTypeSignature()));
        }
        return types.build();
    }

    public static HdfsEnvironment createTestHdfsEnvironment(HiveClientConfig config)
    {
        HdfsConfiguration hdfsConfig = new HiveHdfsConfiguration(new HdfsConfigurationUpdater(config, new PrestoS3ConfigurationUpdater(new HiveS3Config())));
        return new HdfsEnvironment(hdfsConfig, config, new NoHdfsAuthentication());
    }

    public static MapType mapType(Type keyType, Type valueType)
    {
        return (MapType) TYPE_MANAGER.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));
    }

    public static ArrayType arrayType(Type elementType)
    {
        return (ArrayType) TYPE_MANAGER.getParameterizedType(
                StandardTypes.ARRAY,
                ImmutableList.of(TypeSignatureParameter.of(elementType.getTypeSignature())));
    }

    public static RowType rowType(List<NamedTypeSignature> elementTypeSignatures)
    {
        return (RowType) TYPE_MANAGER.getParameterizedType(
                StandardTypes.ROW,
                ImmutableList.copyOf(elementTypeSignatures.stream()
                        .map(TypeSignatureParameter::of)
                        .collect(toList())));
    }

    public static Long shortDecimal(String value)
    {
        return new BigDecimal(value).unscaledValue().longValueExact();
    }

    public static Slice longDecimal(String value)
    {
        return encodeScaledValue(new BigDecimal(value));
    }
}
