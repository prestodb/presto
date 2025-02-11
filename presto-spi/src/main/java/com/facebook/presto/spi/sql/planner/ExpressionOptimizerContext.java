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
package com.facebook.presto.spi.sql.planner;

import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.RowExpressionSerde;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;

import static java.util.Objects.requireNonNull;

public class ExpressionOptimizerContext
{
    private final NodeManager nodeManager;
    private final RowExpressionSerde rowExpressionSerde;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution functionResolution;

    public ExpressionOptimizerContext(NodeManager nodeManager, RowExpressionSerde rowExpressionSerde, FunctionMetadataManager functionMetadataManager, StandardFunctionResolution functionResolution)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.rowExpressionSerde = requireNonNull(rowExpressionSerde, "rowExpressionSerde is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
    }

    public NodeManager getNodeManager()
    {
        return nodeManager;
    }

    public RowExpressionSerde getRowExpressionSerde()
    {
        return rowExpressionSerde;
    }

    public FunctionMetadataManager getFunctionMetadataManager()
    {
        return functionMetadataManager;
    }

    public StandardFunctionResolution getFunctionResolution()
    {
        return functionResolution;
    }
}
