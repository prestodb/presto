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
package com.facebook.presto.operator.index;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.operator.project.PageProjectionWithOutputs;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.sql.relational.Expressions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.facebook.presto.operator.FilterAndProjectOperator.FilterAndProjectOperatorFactory;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;

public class DynamicTupleFilterFactory
{
    private final int filterOperatorId;
    private final PlanNodeId planNodeId;

    private final int[] tupleFilterChannels;
    private final List<Integer> outputFilterChannels;
    private final List<Type> filterTypes;

    private final List<Type> outputTypes;
    private final List<Supplier<PageProjectionWithOutputs>> outputProjections;

    public DynamicTupleFilterFactory(
            int filterOperatorId,
            PlanNodeId planNodeId,
            int[] tupleFilterChannels,
            int[] outputFilterChannels,
            List<Type> outputTypes,
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            PageFunctionCompiler pageFunctionCompiler)
    {
        requireNonNull(planNodeId, "planNodeId is null");
        requireNonNull(tupleFilterChannels, "tupleFilterChannels is null");
        checkArgument(tupleFilterChannels.length > 0, "Must have at least one tupleFilterChannel");
        requireNonNull(outputFilterChannels, "outputFilterChannels is null");
        checkArgument(outputFilterChannels.length == tupleFilterChannels.length, "outputFilterChannels must have same length as tupleFilterChannels");
        requireNonNull(outputTypes, "outputTypes is null");
        checkArgument(outputTypes.size() >= outputFilterChannels.length, "Must have at least as many output channels as those used for filtering");
        requireNonNull(pageFunctionCompiler, "pageFunctionCompiler is null");

        this.filterOperatorId = filterOperatorId;
        this.planNodeId = planNodeId;

        this.tupleFilterChannels = tupleFilterChannels.clone();
        this.outputFilterChannels = ImmutableList.copyOf(Ints.asList(outputFilterChannels));
        this.filterTypes = IntStream.of(outputFilterChannels)
                .mapToObj(outputTypes::get)
                .collect(toImmutableList());

        this.outputTypes = ImmutableList.copyOf(outputTypes);
        this.outputProjections = pageFunctionCompiler.compileProjections(
                sqlFunctionProperties,
                sessionFunctions,
                IntStream.range(0, outputTypes.size())
                        .mapToObj(field -> Expressions.field(field, outputTypes.get(field)))
                        .collect(toImmutableList()),
                false,
                Optional.empty());
    }

    public OperatorFactory filterWithTuple(Page tuplePage)
    {
        Supplier<PageProcessor> processor = createPageProcessor(tuplePage.extractChannels(tupleFilterChannels), OptionalInt.empty());
        return new FilterAndProjectOperatorFactory(filterOperatorId, planNodeId, processor, outputTypes, new DataSize(0, BYTE), 0);
    }

    @VisibleForTesting
    public Supplier<PageProcessor> createPageProcessor(Page filterTuple, OptionalInt initialBatchSize)
    {
        TuplePageFilter filter = new TuplePageFilter(filterTuple, filterTypes, outputFilterChannels);
        return () -> new PageProcessor(
                Optional.of(filter),
                outputProjections.stream()
                        .map(Supplier::get)
                        .collect(toImmutableList()),
                initialBatchSize);
    }
}
