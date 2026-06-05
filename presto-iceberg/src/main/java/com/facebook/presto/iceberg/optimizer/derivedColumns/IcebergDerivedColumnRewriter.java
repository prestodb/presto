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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.iceberg.IcebergTableProperties;
import com.facebook.presto.iceberg.transaction.IcebergTransactionManager;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.parser.SqlParser;

import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;

public class IcebergDerivedColumnRewriter
        implements ConnectorPlanOptimizer
{
    private final IcebergTableProperties tableProperties;
    private final IcebergTransactionManager transactionManager;
    private final SqlParser sqlParser;
    private final StandardFunctionResolution functionResolution;
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;

    public IcebergDerivedColumnRewriter(
            IcebergTableProperties tableProperties,
            IcebergTransactionManager transactionManager,
            StandardFunctionResolution functionResolution,
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            SqlParser sqlParser)
    {
        this.tableProperties = tableProperties;
        this.transactionManager = transactionManager;
        this.functionResolution = functionResolution;
        this.typeManager = typeManager;
        this.functionMetadataManager = functionMetadataManager;
        this.sqlParser = sqlParser;
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new FilterPredicateCSERewriter(
                tableProperties,
                functionResolution,
                typeManager,
                functionMetadataManager,
                transactionManager,
                idAllocator,
                session,
                sqlParser,
                variableAllocator), maxSubplan);
    }
}
