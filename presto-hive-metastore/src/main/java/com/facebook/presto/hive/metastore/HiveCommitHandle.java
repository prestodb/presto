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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorCommitHandle;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class HiveCommitHandle
        implements ConnectorCommitHandle
{
    public static final HiveCommitHandle EMPTY_HIVE_COMMIT_HANDLE = new HiveCommitHandle(ImmutableMap.of(), ImmutableMap.of());

    // This field records the last data commit times for partitions as data input.
    private final Map<SchemaTableName, List<Long>> lastDataCommitTimesForRead;
    // This field records the last data commit times for created or altered partitions/table.
    private final Map<SchemaTableName, List<Long>> lastDataCommitTimesForWrite;

    public HiveCommitHandle(
            Map<SchemaTableName, List<Long>> lastDataCommitTimesForRead,
            Map<SchemaTableName, List<Long>> lastDataCommitTimesForWrite)
    {
        this.lastDataCommitTimesForRead = requireNonNull(lastDataCommitTimesForRead, "lastDataCommitTimesForRead is null");
        this.lastDataCommitTimesForWrite = requireNonNull(lastDataCommitTimesForWrite, "lastDataCommitTimesForWrite is null");
    }

    @Override
    public String getSerializedCommitOutputForRead(SchemaTableName table)
    {
        return serializeCommitOutput(lastDataCommitTimesForRead, table);
    }

    @Override
    public String getSerializedCommitOutputForWrite(SchemaTableName table)
    {
        return serializeCommitOutput(lastDataCommitTimesForWrite, table);
    }

    static String serializeCommitOutput(Map<SchemaTableName, List<Long>> lastDataCommitTimes, SchemaTableName table)
    {
        List<Long> commitTimes = lastDataCommitTimes.getOrDefault(table, ImmutableList.of());
        return Joiner.on(",").join(commitTimes);
    }

    @Override
    public boolean hasCommitOutput(SchemaTableName table)
    {
        return lastDataCommitTimesForRead.containsKey(table) || lastDataCommitTimesForWrite.containsKey(table);
    }
}
