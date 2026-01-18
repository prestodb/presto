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
package com.facebook.presto.operator;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.table.TableFunctionDataProcessor;
import com.facebook.presto.spi.function.table.TableFunctionProcessorState;

import java.util.List;

import static com.facebook.airlift.concurrent.MoreFutures.toListenableFuture;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This is a class representing empty input to a table function. An EmptyTableFunctionPartition is created
 * when the table function has KEEP WHEN EMPTY property, which means that the function should be executed
 * even if the input is empty, and all the table arguments are empty relations.
 * <p>
 * An EmptyTableFunctionPartition is created and processed once per node. To avoid duplicated execution,
 * a table function having KEEP WHEN EMPTY property must have single distribution.
 */
public class EmptyTableFunctionPartition
        implements TableFunctionPartition
{
    private final TableFunctionDataProcessor tableFunction;
    private final int properChannelsCount;
    private final int passThroughSourcesCount;
    private final Type[] passThroughTypes;

    public EmptyTableFunctionPartition(TableFunctionDataProcessor tableFunction, int properChannelsCount, int passThroughSourcesCount, List<Type> passThroughTypes)
    {
        this.tableFunction = requireNonNull(tableFunction, "tableFunction is null");
        this.properChannelsCount = properChannelsCount;
        this.passThroughSourcesCount = passThroughSourcesCount;
        this.passThroughTypes = passThroughTypes.toArray(new Type[] {});
    }

    @Override
    public WorkProcessor<Page> toOutputPages()
    {
        return WorkProcessor.create(() -> {
            TableFunctionProcessorState state = tableFunction.process(null);
            if (state == FINISHED) {
                return WorkProcessor.ProcessState.finished();
            }
            if (state instanceof TableFunctionProcessorState.Blocked) {
                return WorkProcessor.ProcessState.blocked(toListenableFuture(((TableFunctionProcessorState.Blocked) state).getFuture()));
            }
            TableFunctionProcessorState.Processed processed = (TableFunctionProcessorState.Processed) state;
            if (processed.getResult() != null) {
                return WorkProcessor.ProcessState.ofResult(appendNullsForPassThroughColumns(processed.getResult()));
            }
            throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "When function got no input, it should either produce output or return Blocked state");
        });
    }

    private Page appendNullsForPassThroughColumns(Page page)
    {
        if (page.getChannelCount() != properChannelsCount + passThroughSourcesCount) {
            throw new PrestoException(
                    FUNCTION_IMPLEMENTATION_ERROR,
                    format(
                            "Table function returned a page containing %s channels. Expected channel number: %s (%s proper columns, %s pass-through index columns)",
                            page.getChannelCount(),
                            properChannelsCount + passThroughSourcesCount,
                            properChannelsCount,
                            passThroughSourcesCount));
        }

        Block[] resultBlocks = new Block[properChannelsCount + passThroughTypes.length];

        // proper outputs first
        for (int channel = 0; channel < properChannelsCount; channel++) {
            resultBlocks[channel] = page.getBlock(channel);
        }

        // pass-through columns next
        // because no input was processed, all pass-through indexes in the result page must be null (there are no input rows they could refer to).
        // for performance reasons this is not checked. All pass-through columns are filled with nulls.
        int channel = properChannelsCount;
        for (Type type : passThroughTypes) {
            resultBlocks[channel] = RunLengthEncodedBlock.create(type, null, page.getPositionCount());
            channel++;
        }

        // pass the position count so that the Page can be successfully created in the case when there are no output channels (resultBlocks is empty)
        return new Page(page.getPositionCount(), resultBlocks);
    }
}
