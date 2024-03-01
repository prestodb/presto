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
package com.facebook.presto.spi.connector;

import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.RowExpressionService;

public interface ConnectorContext
{
    default NodeManager getNodeManager()
    {
        throw new UnsupportedOperationException();
    }

    default TypeManager getTypeManager()
    {
        throw new UnsupportedOperationException();
    }

    default FunctionMetadataManager getFunctionMetadataManager()
    {
        throw new UnsupportedOperationException();
    }

    default StandardFunctionResolution getStandardFunctionResolution()
    {
        throw new UnsupportedOperationException();
    }

    default PageSorter getPageSorter()
    {
        throw new UnsupportedOperationException();
    }

    default PageIndexerFactory getPageIndexerFactory()
    {
        throw new UnsupportedOperationException();
    }

    default RowExpressionService getRowExpressionService()
    {
        throw new UnsupportedOperationException();
    }

    default FilterStatsCalculatorService getFilterStatsCalculatorService()
    {
        throw new UnsupportedOperationException();
    }

    default BlockEncodingSerde getBlockEncodingSerde()
    {
        throw new UnsupportedOperationException();
    }
}
