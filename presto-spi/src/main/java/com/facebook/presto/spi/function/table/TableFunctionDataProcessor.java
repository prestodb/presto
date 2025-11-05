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

import com.facebook.presto.common.Page;

import java.util.List;
import java.util.Optional;

public interface TableFunctionDataProcessor
{
    /**
     * This method processes a portion of data. It is called multiple times until the partition is fully processed.
     *
     * @param input a tuple of {@link Page} including one page for each table function's input table.
     * Pages list is ordered according to the corresponding argument specifications in {@link ConnectorTableFunction}.
     * A page for an argument consists of columns requested during analysis (see {@link TableFunctionAnalysis#getRequiredColumns()}}.
     * If any of the sources is fully processed, {@code Optional.empty)()} is returned for that source.
     * If all sources are fully processed, the argument is {@code null}.
     * @return {@link TableFunctionProcessorState} including the processor's state and optionally a portion of result.
     * After the returned state is {@code FINISHED}, the method will not be called again.
     */
    TableFunctionProcessorState process(List<Optional<Page>> input);
}
