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

import com.facebook.presto.spi.ConnectorSplit;

public interface TableFunctionSplitProcessor
{
    /**
     * This method processes a split. It is called multiple times until the whole output for the split is produced.
     *
     * @param split a {@link ConnectorSplit} representing a subtask.
     * @return {@link TableFunctionProcessorState} including the processor's state and optionally a portion of result.
     * After the returned state is {@code FINISHED}, the method will not be called again.
     */
    TableFunctionProcessorState process(ConnectorSplit split);
}
