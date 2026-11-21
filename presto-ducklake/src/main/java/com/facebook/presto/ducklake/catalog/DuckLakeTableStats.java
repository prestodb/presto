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
package com.facebook.presto.ducklake.catalog;

/**
 * A row of {@code ducklake_table_stats}. {@code next_row_id} is not modeled; nothing in this
 * connector uses it yet.
 */
public class DuckLakeTableStats
{
    private final long recordCount;
    private final long fileSizeBytes;

    public DuckLakeTableStats(long recordCount, long fileSizeBytes)
    {
        this.recordCount = recordCount;
        this.fileSizeBytes = fileSizeBytes;
    }

    public long getRecordCount()
    {
        return recordCount;
    }

    public long getFileSizeBytes()
    {
        return fileSizeBytes;
    }
}
