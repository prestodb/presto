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
package com.facebook.presto.hive;

import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.gcs.HiveGcsConfig;
import com.facebook.presto.hive.gcs.HiveGcsConfigurationInitializer;
import com.facebook.presto.hive.orc.DwrfBatchPageSourceFactory;
import com.facebook.presto.hive.orc.OrcBatchPageSourceFactory;
import com.facebook.presto.hive.parquet.ParquetPageSourceFactory;
import com.facebook.presto.hive.rcfile.RcFilePageSourceFactory;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.RowExpressionPredicateCompiler;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.spi.type.Decimals.encodeScaledValue;
import static java.util.stream.Collectors.toList;

public final class HiveTestUtils
{
    private HiveTestUtils()
    {
    }

    public static final ConnectorSession SESSION = new TestingConnectorSession(
            new HiveSessionProperties(new HiveClientConfig(), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());

    public static final TypeRegistry TYPE_MANAGER = new TypeRegistry();

    public static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();

    public static final StandardFunctionResolution FUNCTION_RESOLUTION = new FunctionResolution(METADATA.getFunctionManager());

    public static final RowExpressionService ROW_EXPRESSION_SERVICE = new RowExpressionService() {
        @Override
        public DomainTranslator getDomainTranslator()
        {
            return new RowExpressionDomainTranslator(METADATA);
        }

        @Override
        public ExpressionOptimizer getExpressionOptimizer()
        {
            return new RowExpressionOptimizer(METADATA);
        }

        @Override
        public PredicateCompiler getPredicateCompiler()
        {
            return new RowExpressionPredicateCompiler(METADATA);
        }

        @Override
        public DeterminismEvaluator getDeterminismEvaluator()
        {
            return new RowExpressionDeterminismEvaluator(METADATA);
        }
    };

    static {
        // associate TYPE_MANAGER with a function manager
        new FunctionManager(TYPE_MANAGER, new BlockEncodingManager(TYPE_MANAGER), new FeaturesConfig());
    }

    public static final HiveClientConfig HIVE_CLIENT_CONFIG = new HiveClientConfig();

    public static final HdfsEnvironment HDFS_ENVIRONMENT = createTestHdfsEnvironment(HIVE_CLIENT_CONFIG);

    public static final PageSorter PAGE_SORTER = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));

    public static Set<HiveBatchPageSourceFactory> getDefaultHiveDataStreamFactories(HiveClientConfig hiveClientConfig)
    {
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        HdfsEnvironment testHdfsEnvironment = createTestHdfsEnvironment(hiveClientConfig);
        return ImmutableSet.<HiveBatchPageSourceFactory>builder()
                .add(new RcFilePageSourceFactory(TYPE_MANAGER, testHdfsEnvironment, stats))
                .add(new OrcBatchPageSourceFactory(TYPE_MANAGER, hiveClientConfig, testHdfsEnvironment, stats))
                .add(new DwrfBatchPageSourceFactory(TYPE_MANAGER, hiveClientConfig, testHdfsEnvironment, stats))
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
        HdfsConfiguration hdfsConfig = new HiveHdfsConfiguration(
                new HdfsConfigurationInitializer(
                        config,
                        new PrestoS3ConfigurationUpdater(new HiveS3Config()),
                        new HiveGcsConfigurationInitializer(new HiveGcsConfig())),
                ImmutableSet.of());
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
