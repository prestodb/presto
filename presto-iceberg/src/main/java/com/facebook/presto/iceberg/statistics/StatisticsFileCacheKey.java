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
package com.facebook.presto.iceberg.statistics;

import org.apache.iceberg.StatisticsFile;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class StatisticsFileCacheKey
{
    private final StatisticsFile file;
    private final int columnId;

    public StatisticsFileCacheKey(StatisticsFile file, int columnId)
    {
        this.file = requireNonNull(file, "file is null");
        this.columnId = columnId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StatisticsFileCacheKey)) {
            return false;
        }
        StatisticsFileCacheKey that = (StatisticsFileCacheKey) o;
        return columnId == that.columnId && Objects.equals(file, that.file);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(file, columnId);
    }
}
