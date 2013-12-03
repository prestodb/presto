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
import com.facebook.presto.metadata.ColumnFileHandle.Builder;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.base.Throwables;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class MockLocalStorageManager
        implements LocalStorageManager
{
    public static MockLocalStorageManager createMockLocalStorageManager()
    {
        try {
            return new MockLocalStorageManager();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private final File storageFolder;

    private MockLocalStorageManager()
            throws IOException
    {
        this(Files.createTempDir());
    }

    public MockLocalStorageManager(File storageFolder)
            throws IOException
    {
        this.storageFolder = storageFolder;
        Files.createParentDirs(this.storageFolder);
        this.storageFolder.deleteOnExit();
    }

    @Override
    public BlockIterable getBlocks(UUID shardUuid, ColumnHandle columnHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean shardExists(UUID shardUuid)
    {
        return false;
    }

    @Override
    public void dropShard(UUID shardUuid)
    {
    }

    @Override
    public boolean isShardActive(UUID shardUuid)
    {
        return false;
    }

    @Override
    public ColumnFileHandle createStagingFileHandles(UUID shardUuid, List<? extends ColumnHandle> columnHandles)
            throws IOException
    {
        Builder builder = ColumnFileHandle.builder(shardUuid);
        for (ColumnHandle handle : columnHandles) {
            File tmpfile = File.createTempFile("mock-storage", "mock", storageFolder);
            tmpfile.deleteOnExit();
            builder.addColumn(handle, tmpfile);
        }
        return builder.build();
    }

    @Override
    public void commit(ColumnFileHandle columnFileHandle)
            throws IOException
    {
        columnFileHandle.commit();
    }
}
