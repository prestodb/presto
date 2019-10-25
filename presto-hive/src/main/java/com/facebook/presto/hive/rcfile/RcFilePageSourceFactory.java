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
package com.facebook.presto.hive.rcfile;

import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.FileOpener;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveBatchPageSourceFactory;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.orc.OrcSelectivePageSourceFactory;
import com.facebook.presto.orc.FilterFunction;
import com.facebook.presto.rcfile.AircompressorCodecFactory;
import com.facebook.presto.rcfile.HadoopCodecFactory;
import com.facebook.presto.rcfile.RcFileCorruptionException;
import com.facebook.presto.rcfile.RcFileEncoding;
import com.facebook.presto.rcfile.RcFileReader;
import com.facebook.presto.rcfile.binary.BinaryRcFileEncoding;
import com.facebook.presto.rcfile.text.TextRcFileEncoding;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.binaryExpression;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.expressions.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveSchema;
import static com.facebook.presto.rcfile.text.TextRcFileEncoding.DEFAULT_NULL_SEQUENCE;
import static com.facebook.presto.rcfile.text.TextRcFileEncoding.DEFAULT_SEPARATORS;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.serde.serdeConstants.COLLECTION_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.ESCAPE_CHAR;
import static org.apache.hadoop.hive.serde.serdeConstants.FIELD_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.MAPKEY_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_FORMAT;
import static org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters.SERIALIZATION_EXTEND_NESTING_LEVELS;
import static org.apache.hadoop.hive.serde2.lazy.LazyUtils.getByte;

public class RcFilePageSourceFactory
        implements HiveBatchPageSourceFactory
{
    private static final int TEXT_LEGACY_NESTING_LEVELS = 8;
    private static final int TEXT_EXTENDED_NESTING_LEVELS = 29;

    private final TypeManager typeManager;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final FileOpener fileOpener;
    private final RowExpressionService rowExpressionService;

    @Inject
    public RcFilePageSourceFactory(TypeManager typeManager, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats, FileOpener fileOpener, RowExpressionService service)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.fileOpener = requireNonNull(fileOpener, "fileOpener is null");
        this.rowExpressionService = service;
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
            Map<String, String> tableParameters,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            RowExpression remainingPredicate,
            DateTimeZone hiveStorageTimeZone,
            Optional<byte[]> extraFileInfo)
    {
        RcFileEncoding rcFileEncoding;
        if (LazyBinaryColumnarSerDe.class.getName().equals(storage.getStorageFormat().getSerDe())) {
            rcFileEncoding = new BinaryRcFileEncoding();
        }
        else if (ColumnarSerDe.class.getName().equals(storage.getStorageFormat().getSerDe())) {
            rcFileEncoding = createTextVectorEncoding(getHiveSchema(storage.getSerdeParameters(), tableParameters), hiveStorageTimeZone);
        }
        else {
            return Optional.empty();
        }

        if (fileSize == 0) {
            throw new PrestoException(HIVE_BAD_DATA, "RCFile is empty: " + path);
        }

        FSDataInputStream inputStream;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), path, configuration);
            inputStream = fileOpener.open(fileSystem, path, extraFileInfo);
        }
        catch (Exception e) {
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }

        try {
            ImmutableMap.Builder<Integer, Type> readColumns = ImmutableMap.builder();
            for (HiveColumnHandle column : columns) {
                readColumns.put(column.getHiveColumnIndex(), column.getHiveType().getType(typeManager));
            }

            RcFileReader rcFileReader = new RcFileReader(
                    new HdfsRcFileDataSource(path.toString(), inputStream, fileSize, stats),
                    rcFileEncoding,
                    readColumns.build(),
                    new AircompressorCodecFactory(new HadoopCodecFactory(configuration.getClassLoader())),
                    start,
                    length,
                    new DataSize(8, Unit.MEGABYTE));

            if (HiveSessionProperties.isPushdownFilterEnabled(session)) {
                Map<VariableReferenceExpression, InputReferenceExpression> variableToInput = columns.stream()
                        .collect(toImmutableMap(
                                hiveColumnIndex -> new VariableReferenceExpression(hiveColumnIndex.getName(), hiveColumnIndex.getHiveType().getType(typeManager)),
                                hiveColumnIndex -> new InputReferenceExpression(hiveColumnIndex.getHiveColumnIndex(), hiveColumnIndex.getHiveType().getType(typeManager))));

                List<FilterFunction> filterFunctions = toFilterFunctions(replaceExpression(remainingPredicate, variableToInput), session, rowExpressionService.getDeterminismEvaluator(), rowExpressionService.getPredicateCompiler());
                return Optional.of(new RcFileSelectivePageSource(
                        rcFileReader,
                        columns,
                        hiveStorageTimeZone,
                        typeManager,
                        session,
                        effectivePredicate,
                        filterFunctions));
            }

            return Optional.of(new RcFilePageSource(
                    rcFileReader,
                    columns,
                    hiveStorageTimeZone,
                    typeManager,
                    session));
        }
        catch (Throwable e) {
            try {
                inputStream.close();
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = splitError(e, path, start, length);
            if (e instanceof RcFileCorruptionException) {
                throw new PrestoException(HIVE_BAD_DATA, message, e);
            }
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static String splitError(Throwable t, Path path, long start, long length)
    {
        return format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, t.getMessage());
    }

    public static TextRcFileEncoding createTextVectorEncoding(Properties schema, DateTimeZone hiveStorageTimeZone)
    {
        // separators
        int nestingLevels;
        if (!"true".equalsIgnoreCase(schema.getProperty(SERIALIZATION_EXTEND_NESTING_LEVELS))) {
            nestingLevels = TEXT_LEGACY_NESTING_LEVELS;
        }
        else {
            nestingLevels = TEXT_EXTENDED_NESTING_LEVELS;
        }
        byte[] separators = Arrays.copyOf(DEFAULT_SEPARATORS, nestingLevels);

        // the first three separators are set by old-old properties
        separators[0] = getByte(schema.getProperty(FIELD_DELIM, schema.getProperty(SERIALIZATION_FORMAT)), DEFAULT_SEPARATORS[0]);
        separators[1] = getByte(schema.getProperty(COLLECTION_DELIM), DEFAULT_SEPARATORS[1]);
        separators[2] = getByte(schema.getProperty(MAPKEY_DELIM), DEFAULT_SEPARATORS[2]);

        // null sequence
        Slice nullSequence;
        String nullSequenceString = schema.getProperty(SERIALIZATION_NULL_FORMAT);
        if (nullSequenceString == null) {
            nullSequence = DEFAULT_NULL_SEQUENCE;
        }
        else {
            nullSequence = Slices.utf8Slice(nullSequenceString);
        }

        // last column takes rest
        String lastColumnTakesRestString = schema.getProperty(SERIALIZATION_LAST_COLUMN_TAKES_REST);
        boolean lastColumnTakesRest = "true".equalsIgnoreCase(lastColumnTakesRestString);

        // escaped
        String escapeProperty = schema.getProperty(ESCAPE_CHAR);
        Byte escapeByte = null;
        if (escapeProperty != null) {
            escapeByte = getByte(escapeProperty, (byte) '\\');
        }

        return new TextRcFileEncoding(
                hiveStorageTimeZone,
                nullSequence,
                separators,
                escapeByte,
                lastColumnTakesRest);
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
            return ImmutableList.of(new FilterFunction(session, determinismEvaluator.isDeterministic(filter), predicateCompiler.compilePredicate(filter).get()));
        }

        // Use LinkedHashMap to preserve user-specified order of conjuncts. This will be the initial order in which filters are applied.
        Map<Set<Integer>, List<RowExpression>> inputsToConjuncts = new LinkedHashMap<>();
        for (RowExpression conjunct : conjuncts) {
            inputsToConjuncts.computeIfAbsent(extractInputs(conjunct), k -> new ArrayList<>()).add(conjunct);
        }

        return inputsToConjuncts.values().stream()
                .map(expressions -> binaryExpression(AND, expressions))
                .map(predicate -> new FilterFunction(session, determinismEvaluator.isDeterministic(predicate), predicateCompiler.compilePredicate(predicate).get()))
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
}
