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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.expressions.DynamicFilters;
import com.facebook.presto.orc.FilterFunction;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.Chars.isCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.expressions.DynamicFilters.extractDynamicFilters;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NOT_NULL;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NULL;
import static com.facebook.presto.orc.TupleDomainFilterUtils.toFilter;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class FilteringPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final TupleDomainFilter[] domainFilters;
    private final Type[] columnTypes;
    private final Map<Integer, Integer> functionInputs;     // key: hiveColumnIndex
    private final FilterFunction filterFunction;
    private final int outputBlockCount;

    public FilteringPageSource(
            List<HivePageSourceProvider.ColumnMapping> columnMappings,
            TupleDomain<HiveColumnHandle> domainPredicate,
            RowExpression remainingPredicate,
            TypeManager typeManager,
            RowExpressionService rowExpressionService,
            ConnectorSession session,
            Set<Integer> originalIndices,
            ConnectorPageSource delegate)
    {
        requireNonNull(rowExpressionService, "rowExpressionService is null");
        requireNonNull(remainingPredicate, "remainingPredicate is null");
        requireNonNull(typeManager, "typeManager is null");

        this.delegate = requireNonNull(delegate, "delegate is null");

        domainFilters = new TupleDomainFilter[columnMappings.size()];
        columnTypes = new Type[columnMappings.size()];
        if (!domainPredicate.isAll()) {
            Map<Integer, Domain> domains = domainPredicate.transform(HiveColumnHandle::getHiveColumnIndex).getDomains().get();
            for (int i = 0; i < columnMappings.size(); i++) {
                HiveColumnHandle columnHandle = columnMappings.get(i).getHiveColumnHandle();
                int hiveColumnIndex = columnHandle.getHiveColumnIndex();
                if (domains.containsKey(hiveColumnIndex)) {
                    domainFilters[i] = toFilter(domains.get(hiveColumnIndex));
                    columnTypes[i] = columnHandle.getHiveType().getType(typeManager);
                }
            }
        }

        this.functionInputs = IntStream.range(0, columnMappings.size())
                .boxed()
                .collect(toImmutableMap(i -> columnMappings.get(i).getHiveColumnHandle().getHiveColumnIndex(), Function.identity()));

        Map<VariableReferenceExpression, InputReferenceExpression> variableToInput = columnMappings.stream()
                .map(HivePageSourceProvider.ColumnMapping::getHiveColumnHandle)
                .collect(toImmutableMap(
                        columnHandle -> new VariableReferenceExpression(columnHandle.getName(), columnHandle.getHiveType().getType(typeManager)),
                        columnHandle -> new InputReferenceExpression(columnHandle.getHiveColumnIndex(), columnHandle.getHiveType().getType(typeManager))));

        RowExpression optimizedRemainingPredicate = rowExpressionService.getExpressionOptimizer().optimize(remainingPredicate, OPTIMIZED, session);
        if (TRUE_CONSTANT.equals(optimizedRemainingPredicate)) {
            this.filterFunction = null;
        }
        else {
            RowExpression expression = replaceExpression(optimizedRemainingPredicate, variableToInput);

            DynamicFilters.DynamicFilterExtractResult extractDynamicFilterResult = extractDynamicFilters(expression);

            // dynamic filter will be added through subfield pushdown
            expression = and(extractDynamicFilterResult.getStaticConjuncts());

            this.filterFunction = new FilterFunction(
                    session.getSqlFunctionProperties(),
                    rowExpressionService.getDeterminismEvaluator().isDeterministic(expression),
                    rowExpressionService.getPredicateCompiler().compilePredicate(session.getSqlFunctionProperties(), session.getSessionFunctions(), expression).get());
        }

        this.outputBlockCount = requireNonNull(originalIndices, "originalIndices is null").size();
    }

    @Override
    public Page getNextPage()
    {
        Page page = delegate.getNextPage();
        if (page == null || page.getPositionCount() == 0) {
            return page;
        }

        int positionCount = page.getPositionCount();
        int[] positions = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            positions[i] = i;
        }

        for (int i = 0; i < page.getChannelCount(); i++) {
            TupleDomainFilter domainFilter = domainFilters[i];
            if (domainFilter != null) {
                positionCount = filterBlock(page.getBlock(i), columnTypes[i], domainFilter, positions, positionCount);
                if (positionCount == 0) {
                    return new Page(0);
                }
            }
        }

        if (filterFunction != null) {
            RuntimeException[] errors = new RuntimeException[positionCount];

            int[] inputChannels = filterFunction.getInputChannels();
            Block[] inputBlocks = new Block[inputChannels.length];

            for (int i = 0; i < inputChannels.length; i++) {
                inputBlocks[i] = page.getBlock(this.functionInputs.get(inputChannels[i]));
            }

            Page inputPage = new Page(page.getPositionCount(), inputBlocks);
            positionCount = filterFunction.filter(inputPage, positions, positionCount, errors);
            for (int i = 0; i < positionCount; i++) {
                if (errors[i] != null) {
                    throw errors[i];
                }
            }
            if (positionCount == 0) {
                return new Page(0);
            }
        }

        if (outputBlockCount == page.getChannelCount()) {
            return page.getPositions(positions, 0, positionCount);
        }

        Block[] blocks = new Block[outputBlockCount];
        for (int i = 0; i < outputBlockCount; i++) {
            blocks[i] = page.getBlock(i);
        }

        return new Page(page.getPositionCount(), blocks).getPositions(positions, 0, positionCount);
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
    }

    @Override
    public RuntimeStats getRuntimeStats()
    {
        return delegate.getRuntimeStats();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    private static int filterBlock(Block block, Type type, TupleDomainFilter filter, int[] positions, int positionCount)
    {
        int outputPositionsCount = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (block.isNull(position)) {
                if (filter.testNull()) {
                    positions[outputPositionsCount] = position;
                    outputPositionsCount++;
                }
            }
            else if (testNonNullPosition(block, position, type, filter)) {
                positions[outputPositionsCount] = position;
                outputPositionsCount++;
            }
        }

        return outputPositionsCount;
    }

    private static boolean testNonNullPosition(Block block, int position, Type type, TupleDomainFilter filter)
    {
        if (type == BIGINT || type == INTEGER || type == SMALLINT || type == TINYINT || type == TIMESTAMP || type == DATE) {
            return filter.testLong(type.getLong(block, position));
        }

        if (type == BOOLEAN) {
            return filter.testBoolean(type.getBoolean(block, position));
        }

        if (type == DOUBLE) {
            return filter.testDouble(longBitsToDouble(block.getLong(position)));
        }

        if (type == REAL) {
            return filter.testFloat(intBitsToFloat(block.getInt(position)));
        }

        if (type instanceof DecimalType) {
            if (((DecimalType) type).isShort()) {
                return filter.testLong(block.getLong(position));
            }
            else {
                return filter.testDecimal(block.getLong(position, 0), block.getLong(position, Long.BYTES));
            }
        }

        if (isVarcharType(type) || isCharType(type)) {
            Slice slice = block.getSlice(position, 0, block.getSliceLength(position));
            return filter.testBytes((byte[]) slice.getBase(), (int) slice.getAddress() - ARRAY_BYTE_BASE_OFFSET, slice.length());
        }

        if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            if (IS_NULL == filter) {
                return block.isNull(position);
            }
            if (IS_NOT_NULL == filter) {
                return !block.isNull(position);
            }
        }

        throw new UnsupportedOperationException("Unexpected column type " + type);
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public long getCompletedPositions()
    {
        return delegate.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }
}
