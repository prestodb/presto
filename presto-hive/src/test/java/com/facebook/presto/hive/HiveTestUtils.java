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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.smile.SmileCodec;
import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.cost.ConnectorFilterStatsCalculatorService;
import com.facebook.presto.cost.FilterStatsCalculator;
import com.facebook.presto.cost.ScalarStatsCalculator;
import com.facebook.presto.cost.StatsNormalizer;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.datasink.OutputStreamDataSinkFactory;
import com.facebook.presto.hive.gcs.HiveGcsConfig;
import com.facebook.presto.hive.gcs.HiveGcsConfigurationInitializer;
import com.facebook.presto.hive.orc.DwrfBatchPageSourceFactory;
import com.facebook.presto.hive.orc.DwrfSelectivePageSourceFactory;
import com.facebook.presto.hive.orc.OrcBatchPageSourceFactory;
import com.facebook.presto.hive.orc.OrcSelectivePageSourceFactory;
import com.facebook.presto.hive.orc.TupleDomainFilterCache;
import com.facebook.presto.hive.pagefile.PageFilePageSourceFactory;
import com.facebook.presto.hive.pagefile.PageFileWriterFactory;
import com.facebook.presto.hive.parquet.ParquetPageSourceFactory;
import com.facebook.presto.hive.rcfile.RcFilePageSourceFactory;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.orc.StorageStripeMetadataSource;
import com.facebook.presto.orc.StripeMetadataSourceFactory;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.sql.gen.RowExpressionPredicateCompiler;
import com.facebook.presto.sql.planner.planPrinter.RowExpressionFormatter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.json.smile.SmileCodec.smileCodec;
import static com.facebook.presto.common.type.Decimals.encodeScaledValue;
import static com.facebook.presto.hive.HiveDwrfEncryptionProvider.NO_ENCRYPTION;
import static java.util.stream.Collectors.toList;

public final class HiveTestUtils
{
    private HiveTestUtils()
    {
    }

    public static final JsonCodec<PartitionUpdate> PARTITION_UPDATE_CODEC = jsonCodec(PartitionUpdate.class);
    public static final SmileCodec<PartitionUpdate> PARTITION_UPDATE_SMILE_CODEC = smileCodec(PartitionUpdate.class);

    public static final ConnectorSession SESSION = new TestingConnectorSession(
            new HiveSessionProperties(new HiveClientConfig(), new OrcFileWriterConfig(), new ParquetFileWriterConfig(), new CacheConfig()).getSessionProperties());

    public static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();

    public static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = METADATA.getFunctionAndTypeManager();

    public static final StandardFunctionResolution FUNCTION_RESOLUTION = new FunctionResolution(METADATA.getFunctionAndTypeManager());

    public static final RowExpressionService ROW_EXPRESSION_SERVICE = new RowExpressionService()
    {
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

        @Override
        public String formatRowExpression(ConnectorSession session, RowExpression expression)
        {
            return new RowExpressionFormatter(METADATA.getFunctionAndTypeManager()).formatRowExpression(session, expression);
        }
    };

    public static final FilterStatsCalculatorService FILTER_STATS_CALCULATOR_SERVICE = new ConnectorFilterStatsCalculatorService(
            new FilterStatsCalculator(METADATA, new ScalarStatsCalculator(METADATA), new StatsNormalizer()));

    public static final HiveClientConfig HIVE_CLIENT_CONFIG = new HiveClientConfig();
    public static final MetastoreClientConfig METASTORE_CLIENT_CONFIG = new MetastoreClientConfig();

    public static final HdfsEnvironment HDFS_ENVIRONMENT = createTestHdfsEnvironment(HIVE_CLIENT_CONFIG, METASTORE_CLIENT_CONFIG);

    public static final PageSorter PAGE_SORTER = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));

    public static Set<HiveBatchPageSourceFactory> getDefaultHiveBatchPageSourceFactories(HiveClientConfig hiveClientConfig, MetastoreClientConfig metastoreClientConfig)
    {
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        HdfsEnvironment testHdfsEnvironment = createTestHdfsEnvironment(hiveClientConfig, metastoreClientConfig);
        return ImmutableSet.<HiveBatchPageSourceFactory>builder()
                .add(new RcFilePageSourceFactory(FUNCTION_AND_TYPE_MANAGER, testHdfsEnvironment, stats))
                .add(new OrcBatchPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, hiveClientConfig, testHdfsEnvironment, stats, new StorageOrcFileTailSource(), StripeMetadataSourceFactory.of(new StorageStripeMetadataSource())))
                .add(new DwrfBatchPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, hiveClientConfig, testHdfsEnvironment, stats, new StorageOrcFileTailSource(), StripeMetadataSourceFactory.of(new StorageStripeMetadataSource()), NO_ENCRYPTION))
                .add(new ParquetPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, testHdfsEnvironment, stats, new MetadataReader()))
                .add(new PageFilePageSourceFactory(testHdfsEnvironment, new BlockEncodingManager()))
                .build();
    }

    public static Set<HiveSelectivePageSourceFactory> getDefaultHiveSelectivePageSourceFactories(HiveClientConfig hiveClientConfig, MetastoreClientConfig metastoreClientConfig)
    {
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        HdfsEnvironment testHdfsEnvironment = createTestHdfsEnvironment(hiveClientConfig, metastoreClientConfig);
        return ImmutableSet.<HiveSelectivePageSourceFactory>builder()
                .add(new OrcSelectivePageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, ROW_EXPRESSION_SERVICE, hiveClientConfig, testHdfsEnvironment, stats, new StorageOrcFileTailSource(), StripeMetadataSourceFactory.of(new StorageStripeMetadataSource()), new TupleDomainFilterCache()))
                .add(new DwrfSelectivePageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, ROW_EXPRESSION_SERVICE, hiveClientConfig, testHdfsEnvironment, stats, new StorageOrcFileTailSource(), StripeMetadataSourceFactory.of(new StorageStripeMetadataSource()), new TupleDomainFilterCache(), NO_ENCRYPTION))
                .build();
    }

    public static Set<HiveRecordCursorProvider> getDefaultHiveRecordCursorProvider(HiveClientConfig hiveClientConfig, MetastoreClientConfig metastoreClientConfig)
    {
        HdfsEnvironment testHdfsEnvironment = createTestHdfsEnvironment(hiveClientConfig, metastoreClientConfig);
        return ImmutableSet.<HiveRecordCursorProvider>builder()
                .add(new GenericHiveRecordCursorProvider(testHdfsEnvironment))
                .build();
    }

    public static Set<HiveFileWriterFactory> getDefaultHiveFileWriterFactories(HiveClientConfig hiveClientConfig, MetastoreClientConfig metastoreClientConfig)
    {
        HdfsEnvironment testHdfsEnvironment = createTestHdfsEnvironment(hiveClientConfig, metastoreClientConfig);
        return ImmutableSet.<HiveFileWriterFactory>builder()
                .add(new RcFileFileWriterFactory(testHdfsEnvironment, FUNCTION_AND_TYPE_MANAGER, new NodeVersion("test_version"), hiveClientConfig, new FileFormatDataSourceStats()))
                .add(new PageFileWriterFactory(testHdfsEnvironment, new OutputStreamDataSinkFactory(), new BlockEncodingManager()))
                .add(getDefaultOrcFileWriterFactory(hiveClientConfig, metastoreClientConfig))
                .build();
    }

    public static OrcFileWriterFactory getDefaultOrcFileWriterFactory(HiveClientConfig hiveClientConfig, MetastoreClientConfig metastoreClientConfig)
    {
        HdfsEnvironment testHdfsEnvironment = createTestHdfsEnvironment(hiveClientConfig, metastoreClientConfig);
        return new OrcFileWriterFactory(
                testHdfsEnvironment,
                new OutputStreamDataSinkFactory(),
                FUNCTION_AND_TYPE_MANAGER,
                new NodeVersion("test_version"),
                hiveClientConfig,
                new FileFormatDataSourceStats(),
                new OrcFileWriterConfig(),
                NO_ENCRYPTION);
    }

    public static List<Type> getTypes(List<? extends ColumnHandle> columnHandles)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ColumnHandle columnHandle : columnHandles) {
            types.add(FUNCTION_AND_TYPE_MANAGER.getType(((HiveColumnHandle) columnHandle).getTypeSignature()));
        }
        return types.build();
    }

    public static HdfsEnvironment createTestHdfsEnvironment(HiveClientConfig config, MetastoreClientConfig metastoreClientConfig)
    {
        HdfsConfiguration hdfsConfig = new HiveHdfsConfiguration(
                new HdfsConfigurationInitializer(
                        config,
                        metastoreClientConfig,
                        new PrestoS3ConfigurationUpdater(new HiveS3Config()),
                        new HiveGcsConfigurationInitializer(new HiveGcsConfig())),
                ImmutableSet.of());
        return new HdfsEnvironment(hdfsConfig, metastoreClientConfig, new NoHdfsAuthentication());
    }

    public static MapType mapType(Type keyType, Type valueType)
    {
        return (MapType) FUNCTION_AND_TYPE_MANAGER.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));
    }

    public static ArrayType arrayType(Type elementType)
    {
        return (ArrayType) FUNCTION_AND_TYPE_MANAGER.getParameterizedType(
                StandardTypes.ARRAY,
                ImmutableList.of(TypeSignatureParameter.of(elementType.getTypeSignature())));
    }

    public static RowType rowType(List<NamedTypeSignature> elementTypeSignatures)
    {
        return (RowType) FUNCTION_AND_TYPE_MANAGER.getParameterizedType(
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
