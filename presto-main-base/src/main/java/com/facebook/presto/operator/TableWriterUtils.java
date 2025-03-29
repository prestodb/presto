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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class TableWriterUtils
{
    public static final int ROW_COUNT_CHANNEL = 0;
    public static final int FRAGMENT_CHANNEL = 1;
    public static final int CONTEXT_CHANNEL = 2;
    public static final int STATS_START_CHANNEL = 3;

    private TableWriterUtils() {}

    public static Optional<Page> extractStatisticsRows(Page page)
    {
        int statisticsPositionCount = 0;
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (isStatisticsPosition(page, position)) {
                statisticsPositionCount++;
            }
        }

        if (statisticsPositionCount == 0) {
            return Optional.empty();
        }

        if (statisticsPositionCount == page.getPositionCount()) {
            return Optional.of(page);
        }

        int selectedPositionsIndex = 0;
        int[] selectedPositions = new int[statisticsPositionCount];
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (isStatisticsPosition(page, position)) {
                selectedPositions[selectedPositionsIndex] = position;
                selectedPositionsIndex++;
            }
        }

        Block[] blocks = new Block[page.getChannelCount()];
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            blocks[channel] = page.getBlock(channel).getPositions(selectedPositions, 0, statisticsPositionCount);
        }
        return Optional.of(new Page(statisticsPositionCount, blocks));
    }

    /**
     * Both the statistics and the row_count + fragments are transferred over the same communication
     * link between the TableWriterOperator and the TableFinishOperator. Thus the multiplexing is needed.
     * <p>
     * The transferred page layout looks like:
     * <p>
     * [[row_count_channel], [fragment_channel], [statistic_channel_1] ... [statistic_channel_N]]
     * <p>
     * [row_count_channel] - contains number of rows processed by a TableWriterOperator instance
     * [fragment_channel] - contains arbitrary binary data provided by the ConnectorPageSink#finish for
     * the further post processing on the coordinator
     * <p>
     * [statistic_channel_1] ... [statistic_channel_N] - contain pre-aggregated statistics computed by the
     * statistics aggregation operator within the
     * TableWriterOperator
     * <p>
     * Since the final aggregation operator in the TableFinishOperator doesn't know what to do with the
     * first two channels, those must be pruned. For the convenience we never set both, the
     * [row_count_channel] + [fragment_channel] and the [statistic_channel_1] ... [statistic_channel_N].
     * <p>
     * If this is a row that holds statistics - the [row_count_channel] + [fragment_channel] will be NULL.
     * <p>
     * It this is a row that holds the row count or the fragment - all the statistics channels will be set
     * to NULL.
     * <p>
     * Since neither [row_count_channel] or [fragment_channel] cannot hold the NULL value naturally, by
     * checking isNull on these two channels we can determine if this is a row that contains statistics.
     */
    private static boolean isStatisticsPosition(Page page, int position)
    {
        return page.getBlock(ROW_COUNT_CHANNEL).isNull(position) && page.getBlock(FRAGMENT_CHANNEL).isNull(position);
    }

    // Statistics page layout:
    //
    // row     fragments     context     stats1     stats2 ...
    // null       null          X          X          X
    // null       null          X          X          X
    // null       null          X          X          X
    // null       null          X          X          X
    // ...
    public static Page createStatisticsPage(List<Type> types, Page aggregationOutput, Slice tableCommitContext)
    {
        int positionCount = aggregationOutput.getPositionCount();
        Block[] outputBlocks = new Block[types.size()];
        for (int channel = 0; channel < types.size(); channel++) {
            if (channel < STATS_START_CHANNEL) {
                // Include table commit context into statistics page to allow TableFinishOperator publish correct statistics for recoverable grouped execution.
                if (channel == CONTEXT_CHANNEL) {
                    outputBlocks[channel] = RunLengthEncodedBlock.create(types.get(channel), tableCommitContext, positionCount);
                }
                else {
                    outputBlocks[channel] = RunLengthEncodedBlock.create(types.get(channel), null, positionCount);
                }
            }
            else {
                outputBlocks[channel] = aggregationOutput.getBlock(channel - STATS_START_CHANNEL);
            }
        }
        return new Page(positionCount, outputBlocks);
    }

    public static TableCommitContext getTableCommitContext(Page page, JsonCodec<TableCommitContext> tableCommitContextCodec)
    {
        checkState(page.getPositionCount() > 0, "page is empty");
        Block operatorExecutionContextBlock = page.getBlock(CONTEXT_CHANNEL);
        TableCommitContext context = tableCommitContextCodec.fromJson(operatorExecutionContextBlock.getSlice(0, 0, operatorExecutionContextBlock.getSliceLength(0)).getBytes());
        return requireNonNull(context, "context is null");
    }
}
