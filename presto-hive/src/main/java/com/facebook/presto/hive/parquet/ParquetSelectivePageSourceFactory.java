
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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.FilterFunction;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.TupleDomainFilter;
import com.facebook.presto.common.predicate.TupleDomainFilterUtils;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.hive.BucketAdaptation;
import com.facebook.presto.hive.EncryptionInformation;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveCoercer;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.HiveSelectivePageSourceFactory;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.orc.TupleDomainFilterCache;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.ParquetCorruptionException;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.cache.ParquetMetadataSource;
import com.facebook.presto.parquet.predicate.Predicate;
import com.facebook.presto.parquet.reader.ParquetSelectiveReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.binaryExpression;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.expressions.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.AGGREGATED;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveColumnHandle.isPushedDownSubfield;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.HiveSessionProperties.isFailOnCorruptedParquetStatistics;
import static com.facebook.presto.hive.HiveSessionProperties.isUseParquetColumnNames;
import static com.facebook.presto.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactory.getColumnType;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactory.getParquetTupleDomain;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactory.getParquetType;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumns;
import static com.facebook.presto.parquet.ParquetTypeUtils.getDescriptor;
import static com.facebook.presto.parquet.ParquetTypeUtils.getDescriptors;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByName;
import static com.facebook.presto.parquet.predicate.PredicateUtils.buildPredicate;
import static com.facebook.presto.parquet.predicate.PredicateUtils.predicateMatches;
import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.io.ColumnIOConverter.constructField;

public class ParquetSelectivePageSourceFactory
        implements HiveSelectivePageSourceFactory
{
    private static final Set<String> PARQUET_SERDE_CLASS_NAMES = ImmutableSet.<String>builder()
            .add("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
            .add("parquet.hive.serde.ParquetHiveSerDe")
            .build();

    private final TypeManager typeManager;
    private final RowExpressionService rowExpressionService;
    private final StandardFunctionResolution functionResolution;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final int domainCompactionThreshold;
    private final ParquetMetadataSource parquetMetadataSource;

    // TODO: Add TupleDomain cache

    @Inject
    public ParquetSelectivePageSourceFactory(
            TypeManager typeManager,
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution,
            HiveClientConfig config,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            ParquetMetadataSource parquetMetadataSource,
            TupleDomainFilterCache tupleDomainFilterCache)
    {
        this(
                typeManager,
                rowExpressionService, functionResolution,
                hdfsEnvironment,
                stats,
                config.getDomainCompactionThreshold(),
                parquetMetadataSource);
    }

    public ParquetSelectivePageSourceFactory(
            TypeManager typeManager,
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            int domainCompactionThreshold,
            ParquetMetadataSource parquetMetadataSource)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.domainCompactionThreshold = domainCompactionThreshold;
        this.parquetMetadataSource = requireNonNull(parquetMetadataSource, "ParquetMetadataSource is null");
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Storage storage,
            SchemaTableName tableName,
            List<HiveColumnHandle> columns,
            Map<Integer, String> prefilledValues,
            Map<Integer, HiveCoercer> coercers,
            Optional<BucketAdaptation> bucketAdaptation,
            List<Integer> outputColumns,
            Map<String, HiveColumnHandle> predicateColumns,
            TupleDomain<Subfield> domainPredicate,
            RowExpression remainingPredicate,
            DateTimeZone hiveStorageTimeZone,
            HiveFileContext hiveFileContext,
            Optional<EncryptionInformation> encryptionInformation)
    {
        if (!PARQUET_SERDE_CLASS_NAMES.contains(storage.getStorageFormat().getSerDe())) {
            return Optional.empty();
        }

        // per HIVE-13040 and ORC-162, empty files are allowed
        if (fileSize == 0) {
            return Optional.of(new FixedPageSource(ImmutableList.of()));
        }

        return Optional.of(createParquetPageSource(
                hdfsEnvironment,
                configuration,
                session,
                path,
                start,
                length,
                fileSize,
                tableName,
                columns,
                outputColumns,
                predicateColumns,
                prefilledValues,
                domainPredicate,
                remainingPredicate,
                typeManager,
                rowExpressionService,
                functionResolution,
                stats,
                hiveFileContext,
                parquetMetadataSource));
    }

    public static ConnectorPageSource createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            SchemaTableName tableName,
            List<HiveColumnHandle> columns,
            List<Integer> outputColumns,
            Map<String, HiveColumnHandle> predicateColumns,
            Map<Integer, String> prefilledValues,
            TupleDomain<Subfield> domainPredicate,
            RowExpression remainingPredicate,
            TypeManager typeManager,
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution,
            FileFormatDataSourceStats stats,
            HiveFileContext hiveFileContext,
            ParquetMetadataSource parquetMetadataSource)
    {
        ParquetDataSource dataSource = null;
        try {
            FSDataInputStream inputStream = hdfsEnvironment.getFileSystem(session.getUser(), path, configuration).openFile(path, hiveFileContext);
            dataSource = buildHdfsParquetDataSource(inputStream, path, stats);
            ParquetMetadata parquetMetadata = parquetMetadataSource.getParquetMetadata(dataSource, fileSize, hiveFileContext.isCacheable()).getParquetMetadata();

            if (!columns.isEmpty() && columns.stream().allMatch(hiveColumnHandle -> hiveColumnHandle.getColumnType() == AGGREGATED)) {
                return new AggregatedParquetPageSource(columns, parquetMetadata, typeManager, functionResolution);
            }

            boolean useParquetColumnNames = isUseParquetColumnNames(session);
            boolean failOnCorruptedParquetStatistics = isFailOnCorruptedParquetStatistics(session);

            // TODO: support reader verification when isParquetBatchReaderVerificationEnabled(session) is set

            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            Optional<MessageType> message = columns.stream()
                    .filter(column -> column.getColumnType() == REGULAR || isPushedDownSubfield(column))
                    .map(column -> getColumnType(typeManager.getType(column.getTypeSignature()), fileSchema, useParquetColumnNames, column, tableName, path))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(type -> new MessageType(fileSchema.getName(), type))
                    .reduce(MessageType::union);

            MessageType requestedSchema = message.orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));
            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<HiveColumnHandle> rewrittenDomainPredicate = domainPredicate
                    .transform(Subfield::getRootName)
                    .transform(predicateColumns::get);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, rewrittenDomainPredicate);
            Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath);

            ImmutableList.Builder<BlockMetaData> blockMetaDataBuilder = ImmutableList.builder();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();

                // predicatePushdownEnabled is default true for selective page source
                if (firstDataPage >= start && firstDataPage < start + length) {
                    // TODO: predicateMatches would read the rowgroup dictionary and convert it to SortedRangeSet in order to run the predicate. This is very expensive for
                    //  dictionary encoded columns like lineitem.suppkey. We need to 1) extract the dictionary read into SelectiveParquetReader so the dictionary won't be read
                    //  twice 2) optimize the predicate evaluation for dictionaries.
                    if (predicateMatches(parquetPredicate, block, dataSource, descriptorsByPath, parquetTupleDomain, failOnCorruptedParquetStatistics)) {
                        blockMetaDataBuilder.add(block);
                        hiveFileContext.incrementCounter("parquet.blocksRead", 1);
                        hiveFileContext.incrementCounter("parquet.rowsRead", block.getRowCount());
                        hiveFileContext.incrementCounter("parquet.totalBytesRead", block.getTotalByteSize());
                    }
                    else {
                        hiveFileContext.incrementCounter("parquet.blocksSkipped", 1);
                        hiveFileContext.incrementCounter("parquet.rowsSkipped", block.getRowCount());
                        hiveFileContext.incrementCounter("parquet.totalBytesSkipped", block.getTotalByteSize());
                    }
                }
            }

            List<String[]> paths = fileSchema.getPaths();
            List<PrimitiveColumnIO> primitiveColumnIOS = getColumns(fileSchema, requestedSchema);
            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            int channelCount = columns.size();

            Field[] fields = new Field[channelCount];
            ColumnDescriptor[] columnDescriptors = new ColumnDescriptor[channelCount];
            int[] hiveColumnIndexes = new int[channelCount];
            ImmutableBiMap.Builder<String, Integer> columnNameByChannelBuilder = ImmutableBiMap.builder();
            ImmutableMap.Builder<VariableReferenceExpression, InputReferenceExpression> inputByVariableReferenceBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<Integer, Integer> channelByHiveColumnIndexBuilder = ImmutableMap.builder();

            for (int i = 0; i < channelCount; i++) {
                HiveColumnHandle column = columns.get(i);
                // TODO: Support SYNTHESIZED and PARTITION_KEY types
                checkArgument(column.getColumnType() == REGULAR, "column type must be regular column");

                int hiveColumnIndex = column.getHiveColumnIndex();
                String columnName = column.getName();
                String[] columnPath = paths.get(hiveColumnIndex);
                Type type = typeManager.getType(column.getTypeSignature());

                hiveColumnIndexes[i] = hiveColumnIndex;
                columnDescriptors[i] = getDescriptor(primitiveColumnIOS, Arrays.asList(columnPath)).orElse(null);
                columnNameByChannelBuilder.put(columnName, i);
                inputByVariableReferenceBuilder.put(new VariableReferenceExpression(Optional.empty(), columnName, type), new InputReferenceExpression(Optional.empty(), i, type));
                channelByHiveColumnIndexBuilder.put(hiveColumnIndex, i);

                if (getParquetType(type, fileSchema, useParquetColumnNames, column, tableName, path).isPresent()) {
                    columnName = useParquetColumnNames ? columnName : fileSchema.getFields().get(hiveColumnIndex).getName();
                    fields[i] = constructField(type, lookupColumnByName(messageColumnIO, columnName)).orElse(null);
                }
            }

            ImmutableMap<Integer, Integer> channelByHiveColumnIndex = channelByHiveColumnIndexBuilder.build();
            List<Integer> outputChannels = outputColumns.stream().map(hiveColumnIndex -> channelByHiveColumnIndex.get(hiveColumnIndex)).collect(toImmutableList());
            Map<Integer, Map<Subfield, TupleDomainFilter>> tupleDomainFilters = toTupleDomainFilters(domainPredicate, columnNameByChannelBuilder.build());
            List<FilterFunction> filterFunctions = toFilterFunctions(
                    replaceExpression(remainingPredicate, inputByVariableReferenceBuilder.build()),
                    session,
                    rowExpressionService.getDeterminismEvaluator(),
                    rowExpressionService.getPredicateCompiler());

            AggregatedMemoryContext systemMemoryContext = newSimpleAggregatedMemoryContext();

            ParquetSelectiveReader parquetReader = new ParquetSelectiveReader(
                    dataSource,
                    blockMetaDataBuilder.build(),
                    fields,
                    columnDescriptors,
                    tupleDomainFilters,
                    filterFunctions,
                    outputChannels,
                    hiveColumnIndexes,
                    systemMemoryContext);

            return new ParquetSelectivePageSource(parquetReader, dataSource, systemMemoryContext, stats);
        }
        catch (Exception e) {
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            if (e instanceof ParquetCorruptionException) {
                throw new PrestoException(HIVE_BAD_DATA, e);
            }
            if (e instanceof AccessControlException) {
                throw new PrestoException(PERMISSION_DENIED, e.getMessage(), e);
            }
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            String message = format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static Map<Integer, Map<Subfield, TupleDomainFilter>> toTupleDomainFilters(TupleDomain<Subfield> domainPredicate, Map<String, Integer> columnNameByChannel)
    {
        // TODO Add support for filters on subfields
        checkArgument(domainPredicate.getDomains().get().keySet().stream()
                .allMatch(ParquetSelectivePageSourceFactory::isEntireColumn), "Filters on subfields are not supported yet");

        Map<Subfield, TupleDomainFilter> filtersBySubfield = Maps.transformValues(domainPredicate.getDomains().get(), TupleDomainFilterUtils::toFilter);

        Map<Integer, Map<Subfield, TupleDomainFilter>> filtersByColumn = new HashMap<>();
        for (Map.Entry<Subfield, TupleDomainFilter> entry : filtersBySubfield.entrySet()) {
            Subfield subfield = entry.getKey();
            int channel = columnNameByChannel.get(subfield.getRootName());
            TupleDomainFilter filter = entry.getValue();

            filtersByColumn.computeIfAbsent(channel, k -> new HashMap<>()).put(subfield, filter);
        }

        return ImmutableMap.copyOf(filtersByColumn);
    }

    /**
     * Split filter expression into groups of conjuncts that depend on the same set of inputs,
     * then compile each group into FilterFunction.
     */
    private static List<FilterFunction> toFilterFunctions(RowExpression filter, ConnectorSession session, DeterminismEvaluator determinismEvaluator, PredicateCompiler predicateCompiler)
    {
        if (TRUE_CONSTANT.equals(filter)) {
            return ImmutableList.of();
        }

        List<RowExpression> conjuncts = extractConjuncts(filter);
        if (conjuncts.size() == 1) {
            return ImmutableList.of(new FilterFunction(
                    session.getSqlFunctionProperties(),
                    determinismEvaluator.isDeterministic(filter),
                    predicateCompiler.compilePredicate(session.getSqlFunctionProperties(), session.getSessionFunctions(), filter).get()));
        }

        // Use LinkedHashMap to preserve user-specified order of conjuncts. This will be the initial order in which filters are applied.
        Map<Set<Integer>, List<RowExpression>> inputsToConjuncts = new LinkedHashMap<>();
        for (RowExpression conjunct : conjuncts) {
            inputsToConjuncts.computeIfAbsent(extractInputs(conjunct), k -> new ArrayList<>()).add(conjunct);
        }

        return inputsToConjuncts.values().stream()
                .map(expressions -> binaryExpression(AND, expressions))
                .map(predicate -> new FilterFunction(session.getSqlFunctionProperties(), determinismEvaluator.isDeterministic(predicate), predicateCompiler.compilePredicate(session.getSqlFunctionProperties(), session.getSessionFunctions(), predicate).get()))
                .collect(toImmutableList());
    }

    private static Set<Integer> extractInputs(RowExpression expression)
    {
        ImmutableSet.Builder<Integer> inputs = ImmutableSet.builder();
        expression.accept(new InputReferenceBuilderVisitor(), inputs);
        return inputs.build();
    }

    private static class InputReferenceBuilderVisitor
            extends DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<Integer>>
    {
        @Override
        public Void visitInputReference(InputReferenceExpression input, ImmutableSet.Builder<Integer> builder)
        {
            builder.add(input.getField());
            return null;
        }
    }

    private static boolean isEntireColumn(Subfield subfield)
    {
        return subfield.getPath().isEmpty();
    }

    private static String splitError(Throwable t, Path path, long start, long length)
    {
        return format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, t.getMessage());
    }
}
