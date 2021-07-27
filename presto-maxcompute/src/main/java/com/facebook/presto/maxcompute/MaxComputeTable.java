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
package com.facebook.presto.maxcompute;

import com.aliyun.odps.Table;
import com.facebook.presto.spi.SchemaTableName;

import java.util.Date;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class MaxComputeTable
{
    private final SchemaTableName schemaTableName;
    private final Date lastDataModifiedTime;

    public MaxComputeTable(Table table)
    {
        requireNonNull(table.getProject(), "table.getProject() is null");
        requireNonNull(table.getName(), "table.getName() is null");
        schemaTableName = new SchemaTableName(table.getProject(), table.getName());
        this.lastDataModifiedTime = requireNonNull(table.getLastDataModifiedTime(), "table.getLastDataModifiedTime() is null");
    }

    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName.getSchemaName(), schemaTableName.getTableName(), lastDataModifiedTime);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MaxComputeTable other = (MaxComputeTable) obj;
        return Objects.equals(this.schemaTableName, other.schemaTableName) &&
                Objects.equals(this.lastDataModifiedTime, other.lastDataModifiedTime);
    }

    @Override
    public String toString()
    {
        return schemaTableName.toString();
    }
}
