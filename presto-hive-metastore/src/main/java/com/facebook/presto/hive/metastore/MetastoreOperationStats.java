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

import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class MetastoreOperationStats
{
    private final MetastoreOperationType operationType;
    private final List<DateTime> lastDataCommitTimes;
    private static final int DUMMY_MILLISECONDS = -1000;

    public MetastoreOperationStats()
    {
        operationType = MetastoreOperationType.OTHERS;
        lastDataCommitTimes = Arrays.asList(new DateTime(DUMMY_MILLISECONDS));
    }

    public MetastoreOperationStats(MetastoreOperationType operationType, List<DateTime> lastDataCommitTimes)
    {
        this.operationType = requireNonNull(operationType, "operationType is null");
        this.lastDataCommitTimes = requireNonNull(lastDataCommitTimes, "lastDataCommitTimes is null");
    }

    public List<DateTime> getLastDataCommitTimes()
    {
        return lastDataCommitTimes;
    }

    public MetastoreOperationType getOperationType()
    {
        return operationType;
    }
}
