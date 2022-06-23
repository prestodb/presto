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
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class HiveCommitHandle
        implements ConnectorCommitHandle
{
    public static final HiveCommitHandle EMPTY_HIVE_COMMIT_HANDLE = new HiveCommitHandle(ImmutableMap.of());

    private final Map<SchemaTableName, List<DateTime>> lastDataCommitTimes;

    public HiveCommitHandle(Map<SchemaTableName, List<DateTime>> lastDataCommitTimes)
    {
        this.lastDataCommitTimes = requireNonNull(lastDataCommitTimes, "lastDataCommitTimes is null");
    }

    @Override
    public String getSerializedCommitOutput(SchemaTableName table)
    {
        List<Long> commitTimes = lastDataCommitTimes.getOrDefault(table, ImmutableList.of()).stream()
                .map(commitTime -> TimeUnit.MILLISECONDS.toSeconds(commitTime.getMillis()))
                .collect(toImmutableList());
        return Joiner.on(",").join(commitTimes);
    }
}
