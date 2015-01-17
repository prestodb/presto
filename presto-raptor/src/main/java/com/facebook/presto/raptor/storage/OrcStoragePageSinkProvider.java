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

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public class OrcStoragePageSinkProvider
        implements StoragePageSinkProvider
{
    private final List<Long> columnIds;
    private final List<Type> columnTypes;
    private final StorageService storageService;
    private final List<UUID> shardUuids = new ArrayList<>();

    public OrcStoragePageSinkProvider(List<Long> columnIds, List<Type> columnTypes, StorageService storageService)
    {
        this.columnIds = checkNotNull(columnIds, "columnIds is null");
        this.columnTypes = checkNotNull(columnTypes, "columnTypes is null");
        this.storageService = checkNotNull(storageService, "storageService is null");
    }

    @Override
    public StoragePageSink createPageSink()
    {
        UUID shardUuid = UUID.randomUUID();
        File stagingFile = storageService.getStagingFile(shardUuid);
        storageService.createParents(stagingFile);
        shardUuids.add(shardUuid);
        return new OrcStoragePageSink(columnIds, columnTypes, stagingFile);
    }

    @Override
    public List<UUID> getShardUuids()
    {
        return ImmutableList.copyOf(shardUuids);
    }
}
