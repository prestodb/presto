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
package io.prestosql.operator;

import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;

public interface PagesHashStrategy
{
    /**
     * Gets the number of columns appended by this PagesHashStrategy.
     */
    int getChannelCount();

    /**
     * Get the total of allocated size
     */
    long getSizeInBytes();

    /**
     * Appends all values at the specified position to the page builder starting at {@code outputChannelOffset}.
     */
    void appendTo(int blockIndex, int position, PageBuilder pageBuilder, int outputChannelOffset);

    /**
     * Calculates the hash code the hashed columns in this PagesHashStrategy at the specified position.
     */
    long hashPosition(int blockIndex, int position);

    /**
     * Calculates the hash code at {@code position} in {@code page}. Page must have the same number of
     * Blocks as the hashed columns and each entry is expected to be the same type.
     */
    long hashRow(int position, Page page);

    /**
     * Compares the values in the specified pages. The values are compared positionally, so {@code leftPage}
     * and {@code rightPage} must have the same number of entries as the hashed columns and each entry
     * is expected to be the same type.
     */
    boolean rowEqualsRow(int leftPosition, Page leftPage, int rightPosition, Page rightPage);

    /**
     * Compares the hashed columns in this PagesHashStrategy to the values in the specified page. The
     * values are compared positionally, so {@code rightPage} must have the same number of entries as
     * the hashed columns and each entry is expected to be the same type.
     * {@code rightPage} is used if join uses filter function and must contain all columns from probe side of join.
     */
    boolean positionEqualsRow(int leftBlockIndex, int leftPosition, int rightPosition, Page rightPage);

    /**
     * Compares the hashed columns in this PagesHashStrategy to the values in the specified page. The
     * values are compared positionally, so {@code rightPage} must have the same number of entries as
     * the hashed columns and each entry is expected to be the same type.
     * {@code rightPage} is used if join uses filter function and must contain all columns from probe side of join.
     * <p>
     * This method does not perform any null checks.
     */
    boolean positionEqualsRowIgnoreNulls(int leftBlockIndex, int leftPosition, int rightPosition, Page rightPage);

    /**
     * Compares the hashed columns in this PagesHashStrategy to the hashed columns in the Page. The
     * values are compared positionally, so {@code rightChannels} must have the same number of entries as
     * the hashed columns and each entry is expected to be the same type.
     */
    boolean positionEqualsRow(int leftBlockIndex, int leftPosition, int rightPosition, Page page, int[] rightChannels);

    /**
     * Compares the hashed columns in this PagesHashStrategy to the hashed columns in the Page.
     * The values are compared positionally under "not distinct from" semantics.
     * {@code rightChannels} must have the same number of entries as the hashed columns
     * and each entry is expected to be the same type.
     */
    boolean positionNotDistinctFromRow(int leftBlockIndex, int leftPosition, int rightPosition, Page page, int[] rightChannels);

    /**
     * Compares the hashed columns in this PagesHashStrategy at the specified positions.
     */
    boolean positionEqualsPosition(int leftBlockIndex, int leftPosition, int rightBlockIndex, int rightPosition);

    /**
     * Compares the hashed columns in this PagesHashStrategy at the specified positions.
     * <p>
     * This method does not perform any null checks.
     */
    boolean positionEqualsPositionIgnoreNulls(int leftBlockIndex, int leftPosition, int rightBlockIndex, int rightPosition);

    /**
     * Checks if any of the hashed columns is null
     */
    boolean isPositionNull(int blockIndex, int blockPosition);

    /**
     * Compares sort channel (if applicable) values at the specified positions.
     */
    int compareSortChannelPositions(int leftBlockIndex, int leftBlockPosition, int rightBlockIndex, int rightBlockPosition);

    /**
     * Checks if sort channel is null at the specified position
     */
    boolean isSortChannelPositionNull(int blockIndex, int blockPosition);
}
