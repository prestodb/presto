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
import com.facebook.airlift.log.Logger;
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
import com.facebook.presto.hive.azure.HiveAzureConfig;
import com.facebook.presto.hive.azure.HiveAzureConfigurationInitializer;
import com.facebook.presto.hive.datasink.OutputStreamDataSinkFactory;
import com.facebook.presto.hive.gcs.HiveGcsConfig;
import com.facebook.presto.hive.gcs.HiveGcsConfigurationInitializer;
import com.facebook.presto.hive.orc.DwrfAggregatedPageSourceFactory;
import com.facebook.presto.hive.orc.DwrfBatchPageSourceFactory;
import com.facebook.presto.hive.orc.DwrfSelectivePageSourceFactory;
import com.facebook.presto.hive.orc.OrcAggregatedPageSourceFactory;
import com.facebook.presto.hive.orc.OrcBatchPageSourceFactory;
import com.facebook.presto.hive.orc.OrcSelectivePageSourceFactory;
import com.facebook.presto.hive.orc.TupleDomainFilterCache;
import com.facebook.presto.hive.pagefile.PageFilePageSourceFactory;
import com.facebook.presto.hive.pagefile.PageFileWriterFactory;
import com.facebook.presto.hive.parquet.ParquetAggregatedPageSourceFactory;
import com.facebook.presto.hive.parquet.ParquetPageSourceFactory;
import com.facebook.presto.hive.rcfile.RcFilePageSourceFactory;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ClientFactory;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.hive.s3select.S3SelectRecordCursorProvider;
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
import com.facebook.presto.spi.session.PropertyMetadata;
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

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.json.smile.SmileCodec.smileCodec;
import static com.facebook.presto.common.type.Decimals.encodeScaledValue;
import static com.facebook.presto.hive.HiveDwrfEncryptionProvider.NO_ENCRYPTION;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public final class HiveTestUtils
{
    private static final Logger log = Logger.get(HiveTestUtils.class);

    private HiveTestUtils()
    {
    }

    public static final DirectoryLister DO_NOTHING_DIRECTORY_LISTER = (fileSystem, table, path, partition, namenodeStats, hiveDirectoryContext) -> null;

    public static final JsonCodec<PartitionUpdate> PARTITION_UPDATE_CODEC = jsonCodec(PartitionUpdate.class);
    public static final SmileCodec<PartitionUpdate> PARTITION_UPDATE_SMILE_CODEC = smileCodec(PartitionUpdate.class);

    public static final Set<String> TEST_CLIENT_TAGS = ImmutableSet.of("TAG1", "TAG2");

    public static final ConnectorSession SESSION = new TestingConnectorSession(getAllSessionProperties(new HiveClientConfig(), new HiveCommonClientConfig()), TEST_CLIENT_TAGS);
    public static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();

    public static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = METADATA.getFunctionAndTypeManager();

    public static final StandardFunctionResolution FUNCTION_RESOLUTION = new FunctionResolution(METADATA.getFunctionAndTypeManager().getFunctionAndTypeResolver());

    public static final RowExpressionService ROW_EXPRESSION_SERVICE = new RowExpressionService()
    {
        @Override
        public DomainTranslator getDomainTranslator()
        {
            return new RowExpressionDomainTranslator(METADATA);
        }

        @Override
        public ExpressionOptimizer getExpressionOptimizer(ConnectorSession session)
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
            new FilterStatsCalculator(METADATA, new ScalarStatsCalculator(METADATA, ROW_EXPRESSION_SERVICE), new StatsNormalizer()));

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

    public static Set<HiveAggregatedPageSourceFactory> getDefaultHiveAggregatedPageSourceFactories(HiveClientConfig hiveClientConfig, MetastoreClientConfig metastoreClientConfig)
    {
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        HdfsEnvironment testHdfsEnvironment = createTestHdfsEnvironment(hiveClientConfig, metastoreClientConfig);
        return ImmutableSet.<HiveAggregatedPageSourceFactory>builder()
                .add(new OrcAggregatedPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, hiveClientConfig, testHdfsEnvironment, stats, new StorageOrcFileTailSource(), StripeMetadataSourceFactory.of(new StorageStripeMetadataSource())))
                .add(new DwrfAggregatedPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, hiveClientConfig, testHdfsEnvironment, stats, new StorageOrcFileTailSource(), StripeMetadataSourceFactory.of(new StorageStripeMetadataSource())))
                .add(new ParquetAggregatedPageSourceFactory(FUNCTION_AND_TYPE_MANAGER, FUNCTION_RESOLUTION, testHdfsEnvironment, stats, new MetadataReader()))
                .build();
    }

    public static Set<HiveRecordCursorProvider> getDefaultHiveRecordCursorProvider(HiveClientConfig hiveClientConfig, MetastoreClientConfig metastoreClientConfig)
    {
        HdfsEnvironment testHdfsEnvironment = createTestHdfsEnvironment(hiveClientConfig, metastoreClientConfig);
        return ImmutableSet.<HiveRecordCursorProvider>builder()
                .add(new GenericHiveRecordCursorProvider(testHdfsEnvironment))
                .build();
    }

    public static Set<HiveRecordCursorProvider> getDefaultS3HiveRecordCursorProvider(HiveClientConfig hiveClientConfig, MetastoreClientConfig metastoreClientConfig)
    {
        HdfsEnvironment testHdfsEnvironment = createTestHdfsEnvironment(hiveClientConfig, metastoreClientConfig);
        // Without S3SelectRecordCursorProvider we are not pushing down to Select, but falling on GET path.
        // GenericHiveRecordCursorProvider is needed to handle Hive splits when the query does not filter data
        // as this is no longer pushed to Select.
        return ImmutableSet.<HiveRecordCursorProvider>builder()
                .add(new S3SelectRecordCursorProvider(testHdfsEnvironment, hiveClientConfig, new PrestoS3ClientFactory()))
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
                        new HiveGcsConfigurationInitializer(new HiveGcsConfig()),
                        new HiveAzureConfigurationInitializer(new HiveAzureConfig())),
                ImmutableSet.of(),
                config);
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

    public static Optional<String> getProperty(String name)
    {
        String systemPropertyValue = System.getProperty(name);
        String environmentVariableValue = System.getenv(name);
        if (systemPropertyValue == null) {
            if (environmentVariableValue == null) {
                return Optional.empty();
            }
            else {
                return Optional.of(environmentVariableValue);
            }
        }
        else {
            if (environmentVariableValue != null && !systemPropertyValue.equals(environmentVariableValue)) {
                throw new IllegalArgumentException(format("%s is set in both Java system property and environment variable, but their values are different. The Java system property value is %s, while the" +
                                " environment variable value is %s. Please use only one value.",
                        name,
                        systemPropertyValue,
                        environmentVariableValue));
            }
            return Optional.of(systemPropertyValue);
        }
    }

    public static Optional<Path> getDataDirectoryPath(Optional<String> suppliedDataDirectoryPath)
    {
        Optional<Path> dataDirectory = Optional.empty();
        if (!suppliedDataDirectoryPath.isPresent()) {
            //in case the path is not supplied as program argument, read it from env variable.
            suppliedDataDirectoryPath = getProperty("DATA_DIR");
        }
        if (suppliedDataDirectoryPath.isPresent()) {
            File dataDirectoryFile = new File(suppliedDataDirectoryPath.get());
            if (dataDirectoryFile.exists()) {
                if (!dataDirectoryFile.isDirectory()) {
                    log.error("Error: " + dataDirectoryFile.getAbsolutePath() + " is not a directory.");
                    System.exit(1);
                }
                else if (!dataDirectoryFile.canRead() || !dataDirectoryFile.canWrite()) {
                    log.error("Error: " + dataDirectoryFile.getAbsolutePath() + " is not readable/writable.");
                    System.exit(1);
                }
            }
            else {
                // For user supplied path like [path_exists_but_is_not_readable_or_writable]/[paths_do_not_exist], the hadoop file system won't
                // be able to create directory for it. e.g. "/aaa/bbb" is not creatable because path "/" is not writable.
                while (!dataDirectoryFile.exists()) {
                    dataDirectoryFile = dataDirectoryFile.getParentFile();
                }
                if (!dataDirectoryFile.canRead() || !dataDirectoryFile.canWrite()) {
                    log.error("Error: The ancestor directory " + dataDirectoryFile.getAbsolutePath() + " is not readable/writable.");
                    System.exit(1);
                }
            }

            dataDirectory = Optional.of(dataDirectoryFile.toPath());
        }
        return dataDirectory;
    }

    public static List<PropertyMetadata<?>> getAllSessionProperties(HiveClientConfig hiveClientConfig, HiveCommonClientConfig hiveCommonClientConfig)
    {
        return getAllSessionProperties(hiveClientConfig, new ParquetFileWriterConfig(), hiveCommonClientConfig);
    }

    public static List<PropertyMetadata<?>> getAllSessionProperties(HiveClientConfig hiveClientConfig, ParquetFileWriterConfig parquetFileWriterConfig, HiveCommonClientConfig hiveCommonClientConfig)
    {
        HiveSessionProperties hiveSessionProperties = new HiveSessionProperties(
                hiveClientConfig,
                new OrcFileWriterConfig(),
                parquetFileWriterConfig,
                new CacheConfig());

        List<PropertyMetadata<?>> allSessionProperties = new ArrayList<>(hiveSessionProperties.getSessionProperties());

        HiveCommonSessionProperties hiveCommonSessionProperties = new HiveCommonSessionProperties(
                hiveCommonClientConfig);

        allSessionProperties.addAll(hiveCommonSessionProperties.getSessionProperties());
        return allSessionProperties;
    }
}
