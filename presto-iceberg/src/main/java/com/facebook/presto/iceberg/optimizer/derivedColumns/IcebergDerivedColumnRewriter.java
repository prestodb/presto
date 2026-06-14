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
package com.facebook.presto.iceberg.optimizer.derivedColumns;

import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.iceberg.transaction.IcebergTransactionManager;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.parser.SqlParser;

import static com.facebook.presto.iceberg.IcebergSessionProperties.isDerivedColumnsEnabled;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;

public class IcebergDerivedColumnRewriter
        implements ConnectorPlanOptimizer
{
    private final IcebergTransactionManager transactionManager;
    private final SqlParser sqlParser;
    private final StandardFunctionResolution functionResolution;
    private final TypeManager typeManager;

    public IcebergDerivedColumnRewriter(
            IcebergTransactionManager transactionManager,
            StandardFunctionResolution functionResolution,
            TypeManager typeManager,
            SqlParser sqlParser)
    {
        this.transactionManager = transactionManager;
        this.functionResolution = functionResolution;
        this.typeManager = typeManager;
        this.sqlParser = sqlParser;
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        if (!isDerivedColumnsEnabled(session) || session.getQueryType().isEmpty() ||
                !((session.getQueryType().get()).equals(QueryType.SELECT) || session.getQueryType().get().equals(QueryType.EXPLAIN))) {
            return maxSubplan;
        }
        PlanNode rewritten = rewriteWith(new SimplifySubExpressionsRewriter(
                functionResolution,
                typeManager,
                transactionManager,
                idAllocator,
                session,
                sqlParser,
                variableAllocator), maxSubplan);
        return rewritten;
    }
}
