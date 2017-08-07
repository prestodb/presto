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

import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
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
        OperatorFactory tableScanOperator = createTableScanOperator(0, new PlanNodeId("test"), "orders", "totalprice");
        RowExpression filter = call(
                internalOperator(GREATER_THAN_OR_EQUAL, BOOLEAN.getTypeSignature(), ImmutableList.of(DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature())),
                BOOLEAN,
                field(0, DOUBLE),
                constant(50000.0, DOUBLE));
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(localQueryRunner.getMetadata(), new PageFunctionCompiler(localQueryRunner.getMetadata(), 0));
        Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(Optional.of(filter), ImmutableList.of(field(0, DOUBLE)));

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
