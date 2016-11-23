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
package com.facebook.presto.hive.util;

import com.facebook.presto.hive.DirectoryLister;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.NamenodeStats;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.AbstractIterator;
import io.airlift.stats.TimeStat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.InputFormat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILE_NOT_FOUND;
import static java.util.Objects.requireNonNull;

public class HiveFileIterator
        extends AbstractIterator<LocatedFileStatus>
{
    private final FileSystem fileSystem;
    private final DirectoryLister directoryLister;
    private final NamenodeStats namenodeStats;
    private final Path path;
    private final String partitionName;
    private final InputFormat<?, ?> inputFormat;
    private final Properties schema;
    private final List<HivePartitionKey> partitionKeys;
    private final TupleDomain<HiveColumnHandle> effectivePredicate;
    private final Map<Integer, HiveType> columnCoercions;

    private RemoteIterator<LocatedFileStatus> remoteIterator;

    public HiveFileIterator(
            Path path,
            FileSystem fileSystem,
            DirectoryLister directoryLister,
            NamenodeStats namenodeStats,
            String partitionName,
            InputFormat<?, ?> inputFormat,
            Properties schema,
            List<HivePartitionKey> partitionKeys,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Map<Integer, HiveType> columnCoercions)
    {
        this.partitionName = requireNonNull(partitionName, "partitionName is null");
        this.inputFormat = requireNonNull(inputFormat, "inputFormat is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.partitionKeys = requireNonNull(partitionKeys, "partitionKeys is null");
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.path = requireNonNull(path, "path is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.directoryLister = requireNonNull(directoryLister, "directoryLister is null");
        this.namenodeStats = requireNonNull(namenodeStats, "namenodeStats is null");
        this.columnCoercions = requireNonNull(columnCoercions, "columnCoercions is null");
    }

    @Override
    protected LocatedFileStatus computeNext()
    {
        try {
            if (remoteIterator == null) {
                remoteIterator = getLocatedFileStatusRemoteIterator(path);
            }

            while (remoteIterator.hasNext()) {
                LocatedFileStatus status = getLocatedFileStatus(remoteIterator);

                // ignore hidden files. Hive ignores files starting with _ and . as well.
                String fileName = status.getPath().getName();
                if (fileName.startsWith("_") || fileName.startsWith(".")) {
                    continue;
                }
                return status;
            }
            return endOfData();
        }
        catch (FileNotFoundException e) {
            throw new PrestoException(HIVE_FILE_NOT_FOUND, "Partition location does not exist: " + path);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed to list directory: " + path, e);
        }
    }

    private RemoteIterator<LocatedFileStatus> getLocatedFileStatusRemoteIterator(Path path)
            throws IOException
    {
        try (TimeStat.BlockTimer ignored = namenodeStats.getListLocatedStatus().time()) {
            return directoryLister.list(fileSystem, path);
        }
        catch (IOException | RuntimeException e) {
            namenodeStats.getListLocatedStatus().recordException(e);
            throw e;
        }
    }

    private LocatedFileStatus getLocatedFileStatus(RemoteIterator<LocatedFileStatus> iterator)
            throws IOException
    {
        try (TimeStat.BlockTimer ignored = namenodeStats.getRemoteIteratorNext().time()) {
            return iterator.next();
        }
        catch (IOException | RuntimeException e) {
            namenodeStats.getRemoteIteratorNext().recordException(e);
            throw e;
        }
    }

    public FileSystem getFileSystem()
    {
        return fileSystem;
    }

    public DirectoryLister getDirectoryLister()
    {
        return directoryLister;
    }

    public NamenodeStats getNamenodeStats()
    {
        return namenodeStats;
    }

    public String getPartitionName()
    {
        return partitionName;
    }

    public InputFormat<?, ?> getInputFormat()
    {
        return inputFormat;
    }

    public Properties getSchema()
    {
        return schema;
    }

    public List<HivePartitionKey> getPartitionKeys()
    {
        return partitionKeys;
    }

    public TupleDomain<HiveColumnHandle> getEffectivePredicate()
    {
        return effectivePredicate;
    }

    public Map<Integer, HiveType> getColumnCoercions()
    {
        return columnCoercions;
    }
}
