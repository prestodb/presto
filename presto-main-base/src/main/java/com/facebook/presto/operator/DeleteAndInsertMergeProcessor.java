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
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.common.block.ColumnarRow.toColumnarRow;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static com.facebook.presto.spi.ConnectorMergeSink.INSERT_OPERATION_NUMBER;
import static com.facebook.presto.spi.ConnectorMergeSink.UPDATE_OPERATION_NUMBER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class DeleteAndInsertMergeProcessor
        implements MergeRowChangeProcessor
{
    private final List<Type> targetColumnTypes;
    private final Type rowIdType;
    private final int rowIdChannel;
    private final int mergeRowChannel;
    private final List<Integer> targetColumnChannels;
    private final int redistributionColumnCount;
    private final List<Integer> redistributionChannelNumbers;

    public DeleteAndInsertMergeProcessor(
            List<Type> targetColumnTypes,
            Type rowIdType,
            int rowIdChannel,
            int mergeRowChannel,
            List<Integer> redistributionChannelNumbers,
            List<Integer> targetColumnChannels)
    {
        this.targetColumnTypes = requireNonNull(targetColumnTypes, "targetColumnTypes is null");
        this.rowIdType = requireNonNull(rowIdType, "rowIdType is null");
        this.rowIdChannel = rowIdChannel;
        this.mergeRowChannel = mergeRowChannel;
        this.redistributionColumnCount = redistributionChannelNumbers.size();
        int redistributionSourceIndex = 0;
        this.targetColumnChannels = requireNonNull(targetColumnChannels, "targetColumnChannels is null");
        ImmutableList.Builder<Integer> redistributionChannelNumbersBuilder = ImmutableList.builder();
        for (int dataColumnChannel : targetColumnChannels) {
            if (redistributionChannelNumbers.contains(dataColumnChannel)) {
                redistributionChannelNumbersBuilder.add(redistributionSourceIndex);
                redistributionSourceIndex++;
            }
            else {
                redistributionChannelNumbersBuilder.add(-1);
            }
        }
        this.redistributionChannelNumbers = redistributionChannelNumbersBuilder.build();
    }

    @JsonProperty
    public Type getRowIdType()
    {
        return rowIdType;
    }

    /**
     * Transform UPDATE operations into an INSERT and DELETE operation.
     * See {@link MergeRowChangeProcessor#transformPage} for details.
     * @param inputPage It has 5 channels/blocks:<br>
     *                  1. Unique ID<br>
     *                  2. Target Table Row ID (_file:varchar, _pos:bigint, file_record_count:bigint, partition_spec_id:integer, partition_data:varchar)<br>
     *                  3. Merge Row (source table columns, operation, case number)<br>
     *                  4. Merge case number<br>
     *                  5. Is distinct row: it is 1 if no other row has the same unique id and WHEN clause number, 0 otherwise.<br>
     */
    @Override
    public Page transformPage(Page inputPage)
    {
        requireNonNull(inputPage, "inputPage is null");
        int inputChannelCount = inputPage.getChannelCount();
        checkArgument(inputChannelCount >= 2 + redistributionColumnCount, "inputPage channelCount (%s) should be >= 2 + partition columns size (%s)", inputChannelCount, redistributionColumnCount);

        int originalPositionCount = inputPage.getPositionCount();
        checkArgument(originalPositionCount > 0, "originalPositionCount should be > 0, but is %s", originalPositionCount);

        ColumnarRow mergeRow = toColumnarRow(inputPage.getBlock(mergeRowChannel));
        Block operationChannelBlock = mergeRow.getField(mergeRow.getFieldCount() - 2);

        int updatePositions = 0;
        int insertPositions = 0;
        int deletePositions = 0;
        for (int position = 0; position < originalPositionCount; position++) {
            byte operation = TINYINT.getByte(operationChannelBlock, position);
            switch (operation) {
                case DEFAULT_CASE_OPERATION_NUMBER:/* ignored */
                    break;
                case INSERT_OPERATION_NUMBER:
                    insertPositions++;
                    break;
                case DELETE_OPERATION_NUMBER:
                    deletePositions++;
                    break;
                case UPDATE_OPERATION_NUMBER:
                    updatePositions++;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operator number: " + operation);
            }
        }

        int totalPositions = insertPositions + deletePositions + (2 * updatePositions);
        List<Type> pageTypes = ImmutableList.<Type>builder()
                .addAll(targetColumnTypes)
                .add(TINYINT) // Operation: INSERT(1), DELETE(2), UPDATE(3). More info: ConnectorMergeSink
                .add(rowIdType)
                .add(TINYINT) // Insert from update: it is 1 if the cause of the insert is an UPDATE, 0 otherwise.
                .build();

        PageBuilder pageBuilder = new PageBuilder(totalPositions, pageTypes);
        for (int position = 0; position < originalPositionCount; position++) {
            byte operation = TINYINT.getByte(operationChannelBlock, position);
            if (operation != DEFAULT_CASE_OPERATION_NUMBER) {
                // Delete and Update because both create a delete row
                if (operation == DELETE_OPERATION_NUMBER || operation == UPDATE_OPERATION_NUMBER) {
                    addDeleteRow(pageBuilder, inputPage, position);
                }
                // Insert and update because both create an insert row
                if (operation == INSERT_OPERATION_NUMBER || operation == UPDATE_OPERATION_NUMBER) {
                    addInsertRow(pageBuilder, mergeRow, position, operation == UPDATE_OPERATION_NUMBER);
                }
            }
        }

        Page page = pageBuilder.build();
        verify(page.getPositionCount() == totalPositions, "page positions (%s) is not equal to (%s)", page.getPositionCount(), totalPositions);
        return page;
    }

    private void addDeleteRow(PageBuilder pageBuilder, Page originalPage, int position)
    {
        // Copy the write redistribution columns
        for (int targetChannel : targetColumnChannels) {
            Type columnType = targetColumnTypes.get(targetChannel);
            BlockBuilder targetBlock = pageBuilder.getBlockBuilder(targetChannel);

            int redistributionChannelNumber = redistributionChannelNumbers.get(targetChannel);
            if (redistributionChannelNumbers.get(targetChannel) >= 0) {
                // The value comes from that column of the page
                columnType.appendTo(originalPage.getBlock(redistributionChannelNumber), position, targetBlock);
            }
            else {
                // We don't care about the other data columns
                targetBlock.appendNull();
            }
        }

        // Add the operation column == deleted
        TINYINT.writeLong(pageBuilder.getBlockBuilder(targetColumnChannels.size()), DELETE_OPERATION_NUMBER);

        // Copy row ID column
        rowIdType.appendTo(originalPage.getBlock(rowIdChannel), position, pageBuilder.getBlockBuilder(targetColumnChannels.size() + 1));

        // Write 0, meaning this row is not an insert derived from an update
        TINYINT.writeLong(pageBuilder.getBlockBuilder(targetColumnChannels.size() + 2), 0);

        pageBuilder.declarePosition();
    }

    private void addInsertRow(PageBuilder pageBuilder, ColumnarRow mergeCaseBlock, int position, boolean causedByUpdate)
    {
        // Copy the values from the merge block
        for (int targetChannel : targetColumnChannels) {
            Type columnType = targetColumnTypes.get(targetChannel);
            BlockBuilder targetBlock = pageBuilder.getBlockBuilder(targetChannel);
            // The value comes from that column of the page
            columnType.appendTo(mergeCaseBlock.getField(targetChannel), position, targetBlock);
        }

        // Add the operation column == insert
        TINYINT.writeLong(pageBuilder.getBlockBuilder(targetColumnChannels.size()), INSERT_OPERATION_NUMBER);

        // Add null row ID column
        pageBuilder.getBlockBuilder(targetColumnChannels.size() + 1).appendNull();

        // Write 1 if this row is an insert derived from an update, 0 otherwise
        TINYINT.writeLong(pageBuilder.getBlockBuilder(targetColumnChannels.size() + 2), causedByUpdate ? 1 : 0);

        pageBuilder.declarePosition();
    }
}
