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
package com.facebook.presto.benchmark;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static io.airlift.units.DataSize.Unit.BYTE;

public class PredicateFilterBenchmark
        extends AbstractSimpleOperatorBenchmark
{
    public PredicateFilterBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "predicate_filter", 5, 50);
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        Metadata metadata = localQueryRunner.getMetadata();
        OperatorFactory tableScanOperator = createTableScanOperator(0, new PlanNodeId("test"), "orders", "totalprice");
        RowExpression filter = call(
                GREATER_THAN_OR_EQUAL.name(),
                metadata.getFunctionAndTypeManager().resolveOperator(GREATER_THAN_OR_EQUAL, fromTypes(DOUBLE, DOUBLE)),
                BOOLEAN,
                field(0, DOUBLE),
                constant(50000.0, DOUBLE));
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
        Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(localQueryRunner.getDefaultSession().getSqlFunctionProperties(), Optional.of(filter), ImmutableList.of(field(0, DOUBLE)));

        FilterAndProjectOperator.FilterAndProjectOperatorFactory filterAndProjectOperator = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                1,
                new PlanNodeId("test"),
                pageProcessor,
                ImmutableList.of(DOUBLE),
                new DataSize(0, BYTE),
                0);

        return ImmutableList.of(tableScanOperator, filterAndProjectOperator);
    }

    public static void main(String[] args)
    {
        new PredicateFilterBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
