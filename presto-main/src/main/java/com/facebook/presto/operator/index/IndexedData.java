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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;

import java.io.Closeable;

public interface IndexedData
        extends Closeable
{
    long UNLOADED_INDEX_KEY = -2;
    long NO_MORE_POSITIONS = -1;

    /**
     * Returns UNLOADED_INDEX_KEY if the key has not been loaded.
     * Returns NO_MORE_POSITIONS if the key has been loaded, but has no values.
     * Returns a valid address if the key has been loaded and has values.
     */
    long getJoinPosition(int position, Page page);

    /**
     * Returns the next address to join.
     * Returns NO_MORE_POSITIONS if there are no more values to join.
     */
    long getNextJoinPosition(long currentPosition);

    void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset);

    @Override
    void close();
}
