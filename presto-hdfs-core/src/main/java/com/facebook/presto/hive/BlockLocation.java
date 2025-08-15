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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.google.common.collect.ImmutableList;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class BlockLocation
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(BlockLocation.class).instanceSize();

    private final List<String> hosts;
    private final long offset;
    private final long length;

    @ThriftConstructor
    public BlockLocation(List<String> hosts, long offset, long length)
    {
        this.hosts = requireNonNull(hosts, "hosts is null");
        this.offset = offset;
        this.length = length;
    }

    public BlockLocation(org.apache.hadoop.fs.BlockLocation blockLocation)
            throws IOException
    {
        this.hosts = ImmutableList.copyOf(requireNonNull(blockLocation, "blockLocation is null").getHosts());
        this.offset = blockLocation.getOffset();
        this.length = blockLocation.getLength();
    }

    public static List<BlockLocation> fromHiveBlockLocations(@Nullable org.apache.hadoop.fs.BlockLocation[] blockLocations)
            throws IOException
    {
        if (blockLocations == null) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<BlockLocation> result = ImmutableList.builder();
        for (org.apache.hadoop.fs.BlockLocation blockLocation : blockLocations) {
            result.add(new BlockLocation(blockLocation));
        }

        return result.build();
    }

    @ThriftField(1)
    public List<String> getHosts()
    {
        return hosts;
    }

    @ThriftField(2)
    public long getOffset()
    {
        return offset;
    }

    @ThriftField(3)
    public long getLength()
    {
        return length;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + hosts.stream().mapToLong(String::length).reduce(0, Long::sum);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BlockLocation that = (BlockLocation) o;
        return offset == that.offset
                && length == that.length
                && hosts.equals(that.hosts);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(hosts, offset, length);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hosts", hosts)
                .add("offset", offset)
                .add("length", length)
                .toString();
    }
}
