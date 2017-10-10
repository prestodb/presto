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
package com.facebook.presto.hive;

import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Properties;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class InternalHiveSplit
{
    // Overhead of ImmutableList and ImmutableMap is not accounted because of its complexity.
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(InternalHiveSplit.class).instanceSize() +
            ClassLayout.parseClass(String.class).instanceSize() +
            ClassLayout.parseClass(Properties.class).instanceSize() +
            ClassLayout.parseClass(String.class).instanceSize() +
            ClassLayout.parseClass(OptionalInt.class).instanceSize();
    private static final int HOST_ADDRESS_INSTANCE_SIZE = ClassLayout.parseClass(HostAddress.class).instanceSize() +
            ClassLayout.parseClass(String.class).instanceSize();
    private static final int INTEGER_INSTANCE_SIZE = ClassLayout.parseClass(Integer.class).instanceSize();

    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final Properties schema;
    private final List<HivePartitionKey> partitionKeys;
    private final List<HostAddress> addresses;
    private final String partitionName;
    private final OptionalInt bucketNumber;
    private final boolean forceLocalScheduling;
    private final Map<Integer, HiveTypeName> columnCoercions;

    public InternalHiveSplit(
            String partitionName,
            String path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HivePartitionKey> partitionKeys,
            List<HostAddress> addresses,
            OptionalInt bucketNumber,
            boolean forceLocalScheduling,
            Map<Integer, HiveTypeName> columnCoercions)
    {
        checkArgument(start >= 0, "start must be positive");
        checkArgument(length >= 0, "length must be positive");
        checkArgument(fileSize >= 0, "fileSize must be positive");
        requireNonNull(partitionName, "partitionName is null");
        requireNonNull(path, "path is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(partitionKeys, "partitionKeys is null");
        requireNonNull(addresses, "addresses is null");
        requireNonNull(bucketNumber, "bucketNumber is null");
        requireNonNull(columnCoercions, "columnCoercions is null");

        this.partitionName = partitionName;
        this.path = path;
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.schema = schema;
        this.partitionKeys = ImmutableList.copyOf(partitionKeys);
        this.addresses = ImmutableList.copyOf(addresses);
        this.bucketNumber = bucketNumber;
        this.forceLocalScheduling = forceLocalScheduling;
        this.columnCoercions = ImmutableMap.copyOf(columnCoercions);
    }

    public String getPath()
    {
        return path;
    }

    public long getStart()
    {
        return start;
    }

    public long getLength()
    {
        return length;
    }

    public long getFileSize()
    {
        return fileSize;
    }

    public Properties getSchema()
    {
        return schema;
    }

    public List<HivePartitionKey> getPartitionKeys()
    {
        return partitionKeys;
    }

    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    public String getPartitionName()
    {
        return partitionName;
    }

    public OptionalInt getBucketNumber()
    {
        return bucketNumber;
    }

    public boolean isForceLocalScheduling()
    {
        return forceLocalScheduling;
    }

    public Map<Integer, HiveTypeName> getColumnCoercions()
    {
        return columnCoercions;
    }

    public int getEstimatedSizeInBytes()
    {
        int result = INSTANCE_SIZE;
        result += path.length() * Character.BYTES;
        result += SizeOf.sizeOfObjectArray(partitionKeys.size());
        for (HivePartitionKey partitionKey : partitionKeys) {
            result += partitionKey.getEstimatedSizeInBytes();
        }
        result += SizeOf.sizeOfObjectArray(addresses.size());
        for (HostAddress address : addresses) {
            result += HOST_ADDRESS_INSTANCE_SIZE + address.getHostText().length() * Character.BYTES;
        }
        result += partitionName.length() * Character.BYTES;
        result += SizeOf.sizeOfObjectArray(columnCoercions.size());
        for (HiveTypeName hiveTypeName : columnCoercions.values()) {
            result += INTEGER_INSTANCE_SIZE + hiveTypeName.getEstimatedSizeInBytes();
        }
        return result;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(path)
                .addValue(start)
                .addValue(length)
                .addValue(fileSize)
                .toString();
    }
}
