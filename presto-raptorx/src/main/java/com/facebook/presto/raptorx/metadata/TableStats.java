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
package com.facebook.presto.raptorx.metadata;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TableStats
{
    private final String tableSchema;
    private final String tableName;
    private final long createTime;
    private final long updateTime;
    private final long tableVersion;
    private final long rowCount;
    private final long chunkCount;
    private final long compressedSize;
    private final long uncompressedSize;

    public TableStats(
            String tableSchema,
            String tableName,
            long createTime,
            long updateTime,
            long tableVersion,
            long rowCount,
            long chunkCount,
            long compressedSize,
            long uncompressedSize)
    {
        this.tableSchema = requireNonNull(tableSchema, "tableSchema is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.tableVersion = tableVersion;
        this.rowCount = rowCount;
        this.chunkCount = chunkCount;
        this.compressedSize = compressedSize;
        this.uncompressedSize = uncompressedSize;
    }

    public String getTableSchema()
    {
        return tableSchema;
    }

    public String getTableName()
    {
        return tableName;
    }

    public long getCreateTime()
    {
        return createTime;
    }

    public long getUpdateTime()
    {
        return updateTime;
    }

    public long getTableVersion()
    {
        return tableVersion;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public long getChunkCount()
    {
        return chunkCount;
    }

    public long getCompressedSize()
    {
        return compressedSize;
    }

    public long getUncompressedSize()
    {
        return uncompressedSize;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableSchema", tableSchema)
                .add("tableName", tableName)
                .add("createTime", createTime)
                .add("updateTime", updateTime)
                .add("tableVersion", tableVersion)
                .add("rowCount", rowCount)
                .add("chunkCount", chunkCount)
                .add("compressedSize", compressedSize)
                .add("uncompressedSize", uncompressedSize)
                .toString();
    }
}
