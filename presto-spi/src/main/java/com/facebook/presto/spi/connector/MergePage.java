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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;

import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static com.facebook.presto.spi.ConnectorMergeSink.INSERT_OPERATION_NUMBER;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Separate deletions and insertions pages from a merge using
 * {@link RowChangeParadigm#DELETE_ROW_AND_INSERT_ROW}.
 */
public final class MergePage
{
    private final Optional<Page> deletionsPage;
    private final Optional<Page> insertionsPage;

    private MergePage(Optional<Page> deletionsPage, Optional<Page> insertionsPage)
    {
        this.deletionsPage = requireNonNull(deletionsPage);
        this.insertionsPage = requireNonNull(insertionsPage);
    }

    /**
     * @return delete page with data columns followed by row ID column
     */
    public Optional<Page> getDeletionsPage()
    {
        return deletionsPage;
    }

    /**
     * @return insert page with data columns
     */
    public Optional<Page> getInsertionsPage()
    {
        return insertionsPage;
    }

    public static MergePage createDeleteAndInsertPages(Page inputPage, int dataColumnCount)
    {
        // see page description in ConnectorMergeSink
        int inputChannelCount = inputPage.getChannelCount();
        if (inputChannelCount != dataColumnCount + 2) {
            throw new IllegalArgumentException(format("inputPage channelCount (%s) == dataColumns size (%s) + 2", inputChannelCount, dataColumnCount));
        }

        int positionCount = inputPage.getPositionCount();
        if (positionCount <= 0) {
            throw new IllegalArgumentException("positionCount should be > 0, but is " + positionCount);
        }

        Block operationBlock = inputPage.getBlock(inputChannelCount - 2);

        int[] deletePositions = new int[positionCount];
        int[] insertPositions = new int[positionCount];
        int deletePositionCount = 0;
        int insertPositionCount = 0;

        for (int position = 0; position < positionCount; position++) {
            int operation = toIntExact(TINYINT.getLong(operationBlock, position));
            switch (operation) {
                case DELETE_OPERATION_NUMBER:
                    deletePositions[deletePositionCount] = position;
                    deletePositionCount++;
                    break;
                case INSERT_OPERATION_NUMBER:
                    insertPositions[insertPositionCount] = position;
                    insertPositionCount++;
                    break;
                default:
                    throw new IllegalArgumentException("Invalid merge operation: " + operation);
            }
        }

        Optional<Page> deletePage = Optional.empty();
        if (deletePositionCount > 0) {
            int[] columns = new int[dataColumnCount + 1];
            for (int i = 0; i < dataColumnCount; i++) {
                columns[i] = i;
            }
            columns[dataColumnCount] = dataColumnCount + 1; // row ID channel
            deletePage = Optional.of(inputPage
                    .getColumns(columns)
                    .getPositions(deletePositions, 0, deletePositionCount));
        }

        Optional<Page> insertPage = Optional.empty();
        if (insertPositionCount > 0) {
            insertPage = Optional.of(inputPage
                    .getColumns(IntStream.range(0, dataColumnCount).toArray())
                    .getPositions(insertPositions, 0, insertPositionCount));
        }

        return new MergePage(deletePage, insertPage);
    }
}
