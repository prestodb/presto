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
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;

import javax.annotation.concurrent.Immutable;

import static java.util.Objects.requireNonNull;

@Immutable
public class IndexSnapshot
        implements IndexedData
{
    private final LookupSource values;
    private final LookupSource missingKeys;

    public IndexSnapshot(LookupSource values, LookupSource missingKeys)
    {
        this.values = requireNonNull(values, "values is null");
        this.missingKeys = requireNonNull(missingKeys, "missingKeys is null");
    }

    @Override
    public long getJoinPosition(int position, Page page)
    {
        long joinPosition = values.getJoinPosition(position, page);
        if (joinPosition < 0) {
            if (missingKeys.getJoinPosition(position, page) < 0) {
                return UNLOADED_INDEX_KEY;
            }
            else {
                return NO_MORE_POSITIONS;
            }
        }
        return joinPosition;
    }

    @Override
    public long getNextJoinPosition(long currentPosition)
    {
        return values.getNextJoinPosition(currentPosition);
    }

    @Override
    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        values.appendTo(position, pageBuilder, outputChannelOffset);
    }

    @Override
    public void close()
    {
    }
}
