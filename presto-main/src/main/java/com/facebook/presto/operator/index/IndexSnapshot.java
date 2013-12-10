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
package com.facebook.presto.operator.index;

import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.spi.block.BlockCursor;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class IndexSnapshot
{
    public static final long UNLOADED_INDEX_KEY = -2;
    public static final long NO_MORE_POSITIONS = -1;

    private final LookupSource values;
    private final LookupSource missingKeys;

    public IndexSnapshot(LookupSource values, LookupSource missingKeys)
    {
        this.values = checkNotNull(values, "values is null");
        this.missingKeys = checkNotNull(missingKeys, "missingKeys is null");
    }

    /**
     * Returns UNLOADED_INDEX_KEY if the key has not been loaded.
     * Returns NO_MORE_POSITIONS if the key has been loaded, but has no values.
     * Returns a valid address if the key has been loaded and has values.
     */
    public long getJoinPosition(BlockCursor... cursors)
    {
        long joinPosition = values.getJoinPosition(cursors);
        if (joinPosition < 0) {
            if (missingKeys.getJoinPosition(cursors) < 0) {
                return UNLOADED_INDEX_KEY;
            }
            else {
                return NO_MORE_POSITIONS;
            }
        }
        return joinPosition;
    }

    /**
     * Returns the next address to join.
     * Returns NO_MORE_POSITIONS if there are no more values to join.
     */
    public long getNextJoinPosition(long currentPosition)
    {
        return values.getNextJoinPosition(currentPosition);
    }

    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        values.appendTo(position, pageBuilder, outputChannelOffset);
    }
}
