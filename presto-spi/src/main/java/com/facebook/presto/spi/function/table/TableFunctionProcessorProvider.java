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
package com.facebook.presto.spi.function.table;

public interface TableFunctionProcessorProvider
{
    /**
     * This method returns a {@code TableFunctionDataProcessor}. All the necessary information collected during analysis is available
     * in the form of {@link ConnectorTableFunctionHandle}. It is called once per each partition processed by the table function.
     */
    default TableFunctionDataProcessor getDataProcessor(ConnectorTableFunctionHandle handle)
    {
        throw new UnsupportedOperationException("this table function does not process input data");
    }

    /**
     * This method returns a {@code TableFunctionSplitProcessor}. All the necessary information collected during analysis is available
     * in the form of {@link ConnectorTableFunctionHandle}. It is called once per each split processed by the table function.
     */
    default TableFunctionSplitProcessor getSplitProcessor(ConnectorTableFunctionHandle handle)
    {
        throw new UnsupportedOperationException("this table function does not process splits");
    }
}
