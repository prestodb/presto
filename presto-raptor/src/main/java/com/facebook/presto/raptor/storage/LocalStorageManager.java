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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.block.Block;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public interface LocalStorageManager
{
    Iterable<Block> getBlocks(UUID shardUuid, ConnectorColumnHandle columnHandle);

    boolean shardExists(UUID shardUuid);

    ColumnFileHandle createStagingFileHandles(UUID shardUuid, List<RaptorColumnHandle> columnHandles)
            throws IOException;

    void commit(ColumnFileHandle columnFileHandle)
            throws IOException;
}
