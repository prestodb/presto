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
package com.facebook.presto.metadata;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.spi.ColumnHandle;

import java.io.IOException;
import java.util.List;

public interface LocalStorageManager
{
    BlockIterable getBlocks(long shardId, ColumnHandle columnHandle);

    boolean shardExists(long shardId);

    void dropShard(long shardId);

    boolean isShardActive(long shardId);

    ColumnFileHandle createStagingFileHandles(long shardId, List<? extends ColumnHandle> columnHandles)
            throws IOException;

    void commit(ColumnFileHandle columnFileHandle)
            throws IOException;
}
