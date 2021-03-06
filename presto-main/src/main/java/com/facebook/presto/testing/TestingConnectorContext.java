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
package com.facebook.presto.testing;

import com.facebook.presto.GroupByHashPageIndexerFactory;
import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.connector.ConnectorAwareNodeManager;
import com.facebook.presto.cost.ConnectorFilterStatsCalculatorService;
import com.facebook.presto.cost.FilterStatsCalculator;
import com.facebook.presto.cost.ScalarStatsCalculator;
import com.facebook.presto.cost.StatsNormalizer;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.RowExpressionPredicateCompiler;
import com.facebook.presto.sql.planner.planPrinter.RowExpressionFormatter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;

import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;

public class TestingConnectorContext
        implements ConnectorContext
{
    private final NodeManager nodeManager = new ConnectorAwareNodeManager(new InMemoryNodeManager(), "testenv", new ConnectorId("test"));
    private final FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
    private final StandardFunctionResolution functionResolution = new FunctionResolution(functionAndTypeManager);
    private final PageSorter pageSorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));
    private final PageIndexerFactory pageIndexerFactory = new GroupByHashPageIndexerFactory(new JoinCompiler(MetadataManager.createTestMetadataManager(), new FeaturesConfig()));
    private final Metadata metadata = MetadataManager.createTestMetadataManager();
    private final DomainTranslator domainTranslator = new RowExpressionDomainTranslator(metadata);
    private final PredicateCompiler predicateCompiler = new RowExpressionPredicateCompiler(metadata);
    private final DeterminismEvaluator determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
    private final FilterStatsCalculatorService filterStatsCalculatorService = new ConnectorFilterStatsCalculatorService(new FilterStatsCalculator(metadata, new ScalarStatsCalculator(metadata), new StatsNormalizer()));
    private final BlockEncodingSerde blockEncodingSerde = new BlockEncodingManager();

    @Override
    public NodeManager getNodeManager()
    {
        return nodeManager;
    }

    @Override
    public TypeManager getTypeManager()
    {
        return functionAndTypeManager;
    }

    @Override
    public FunctionMetadataManager getFunctionMetadataManager()
    {
        return functionAndTypeManager;
    }

    @Override
    public StandardFunctionResolution getStandardFunctionResolution()
    {
        return functionResolution;
    }

    @Override
    public PageSorter getPageSorter()
    {
        return pageSorter;
    }

    @Override
    public PageIndexerFactory getPageIndexerFactory()
    {
        return pageIndexerFactory;
    }

    @Override
    public RowExpressionService getRowExpressionService()
    {
        return new RowExpressionService()
        {
            @Override
            public DomainTranslator getDomainTranslator()
            {
                return domainTranslator;
            }

            @Override
            public ExpressionOptimizer getExpressionOptimizer()
            {
                return new RowExpressionOptimizer(metadata);
            }

            @Override
            public PredicateCompiler getPredicateCompiler()
            {
                return predicateCompiler;
            }

            @Override
            public DeterminismEvaluator getDeterminismEvaluator()
            {
                return determinismEvaluator;
            }

            @Override
            public String formatRowExpression(ConnectorSession session, RowExpression expression)
            {
                return new RowExpressionFormatter(functionAndTypeManager).formatRowExpression(session, expression);
            }
        };
    }

    @Override
    public FilterStatsCalculatorService getFilterStatsCalculatorService()
    {
        return filterStatsCalculatorService;
    }

    @Override
    public BlockEncodingSerde getBlockEncodingSerde()
    {
        return blockEncodingSerde;
    }
}
