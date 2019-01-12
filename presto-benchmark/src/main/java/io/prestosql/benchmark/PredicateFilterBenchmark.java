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
package io.prestosql.benchmark;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.operator.FilterAndProjectOperator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.testing.LocalQueryRunner;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static io.prestosql.metadata.Signature.internalOperator;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.Expressions.field;

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
