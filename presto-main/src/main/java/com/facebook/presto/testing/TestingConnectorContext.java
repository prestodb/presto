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
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.connector.ConnectorAwareNodeManager;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.RowExpressionPredicateCompiler;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.type.TypeRegistry;

public class TestingConnectorContext
        implements ConnectorContext
{
    private final NodeManager nodeManager = new ConnectorAwareNodeManager(new InMemoryNodeManager(), "testenv", new ConnectorId("test"));
    private final TypeManager typeManager = new TypeRegistry();
    private final FunctionManager functionManager = new FunctionManager(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
    private final StandardFunctionResolution functionResolution = new FunctionResolution(functionManager);
    private final PageSorter pageSorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));
    private final PageIndexerFactory pageIndexerFactory = new GroupByHashPageIndexerFactory(new JoinCompiler(MetadataManager.createTestMetadataManager(), new FeaturesConfig()));
    private final Metadata metadata = MetadataManager.createTestMetadataManager();
    private final DomainTranslator domainTranslator = new RowExpressionDomainTranslator(metadata);
    private final PredicateCompiler predicateCompiler = new RowExpressionPredicateCompiler(metadata);
    private final DeterminismEvaluator determinismEvaluator = new RowExpressionDeterminismEvaluator(functionManager);

    @Override
    public NodeManager getNodeManager()
    {
        return nodeManager;
    }

    @Override
    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    @Override
    public FunctionMetadataManager getFunctionMetadataManager()
    {
        return functionManager;
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
        };
    }
}
