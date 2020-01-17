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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.hadoop.fs.FileSystem;

import java.util.BitSet;
import java.util.Optional;
import java.util.UUID;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DeltaShardLoader
{
    private static final RowsToKeepResult KEEP_ALL = new RowsToKeepResult(true, new IntArrayList(0));

    private final Optional<UUID> deltaShardUuid;
    private final boolean tableSupportsDeltaDelete;
    private final OrcStorageManager orcStorageManager;
    private final FileSystem fileSystem;

    private boolean loaded;
    private Optional<BitSet> rowsDeleted = Optional.empty();

    public DeltaShardLoader(
            Optional<UUID> deltaShardUuid,
            boolean tableSupportsDeltaDelete,
            OrcStorageManager orcStorageManager,
            FileSystem fileSystem)
    {
        this.deltaShardUuid = requireNonNull(deltaShardUuid, "deltaShardUuid is null");
        this.tableSupportsDeltaDelete = tableSupportsDeltaDelete;
        this.orcStorageManager = requireNonNull(orcStorageManager, "storageManager is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
    }

    public RowsToKeepResult getRowsToKeep(int batchSize, long filePosition)
    {
        // lazy load rowsDeleted until getNextPage call
        // rowsDeleted will only be loaded once
        Optional<BitSet> rowsDeleted = getRowsDeleted();
        if (rowsDeleted.isPresent() && rowsDeleted.get().cardinality() > 0) {
            IntArrayList rowsToKeep = new IntArrayList(batchSize);
            for (int position = 0; position < batchSize; position++) {
                if (!rowsDeleted.get().get(toIntExact(filePosition) + position)) {
                    rowsToKeep.add(position);
                }
            }
            if (rowsToKeep.size() == batchSize) {
                return KEEP_ALL;
            }
            return new RowsToKeepResult(false, rowsToKeep);
        }
        return KEEP_ALL;
    }

    private Optional<BitSet> getRowsDeleted()
    {
        // Just load once
        if (!loaded && tableSupportsDeltaDelete && deltaShardUuid.isPresent()) {
            rowsDeleted = orcStorageManager.getRowsFromUuid(fileSystem, deltaShardUuid);
            loaded = true;
        }
        return rowsDeleted;
    }

    static class RowsToKeepResult
    {
        private final boolean keepAll;
        private final IntArrayList rowsToKeep;

        private RowsToKeepResult(boolean keepAll, IntArrayList rowsToKeep)
        {
            this.keepAll = keepAll;
            this.rowsToKeep = requireNonNull(rowsToKeep, "rowsToKeep is null");
        }

        public boolean keepAll()
        {
            return keepAll;
        }

        public int size()
        {
            return rowsToKeep.size();
        }

        public IntArrayList getRowsToKeep()
        {
            return rowsToKeep;
        }

        public int[] elements()
        {
            return rowsToKeep.elements();
        }
    }
}
