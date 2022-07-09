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
package com.facebook.presto.orc.metadata.statistics;

import com.facebook.presto.orc.metadata.statistics.StatisticsHasher.Hashable;
import com.facebook.presto.orc.proto.DwrfProto;
import org.openjdk.jol.info.ClassLayout;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class MapStatisticsEntry
        implements Hashable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapStatisticsEntry.class).instanceSize();
    private static final int KEY_INSTANCE_SIZE = ClassLayout.parseClass(DwrfProto.KeyInfo.class).instanceSize();
    private final DwrfProto.KeyInfo key;
    private final ColumnStatistics columnStatistics;

    public MapStatisticsEntry(DwrfProto.KeyInfo key, ColumnStatistics columnStatistics)
    {
        this.key = requireNonNull(key, "key is null");
        this.columnStatistics = requireNonNull(columnStatistics, "columnStatistics is null");
    }

    public DwrfProto.KeyInfo getKey()
    {
        return key;
    }

    public ColumnStatistics getColumnStatistics()
    {
        return columnStatistics;
    }

    public long getRetainedSizeInBytes()
    {
        long keySize = KEY_INSTANCE_SIZE;
        if (key.hasBytesKey()) {
            keySize += Byte.BYTES * key.getBytesKey().size();
        }
        return INSTANCE_SIZE + keySize + columnStatistics.getRetainedSizeInBytes();
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
        MapStatisticsEntry that = (MapStatisticsEntry) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(columnStatistics, that.columnStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(key, columnStatistics);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("key", key.hasBytesKey() ? key.getBytesKey() : key.getIntKey())
                .add("columnStatistics", columnStatistics)
                .toString();
    }

    @Override
    public void addHash(StatisticsHasher hasher)
    {
        if (key.hasBytesKey()) {
            hasher.putBytes(key.getBytesKey().asReadOnlyByteBuffer());
        }
        else {
            hasher.putLong(key.getIntKey());
        }
        hasher.putOptionalHashable(columnStatistics);
    }
}
