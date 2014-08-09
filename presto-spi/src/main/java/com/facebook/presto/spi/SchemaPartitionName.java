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
package com.facebook.presto.spi;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static com.facebook.presto.spi.SchemaUtil.checkNotEmpty;

public class SchemaPartitionName
{
    private final String schemaName;
    private final String tableName;
    private final List<String> spec;

    public SchemaPartitionName(String schemaName, String tableName, List<String> spec)
    {
        this.schemaName = checkNotEmpty(schemaName, "schemaName");
        this.tableName = checkNotEmpty(tableName, "tableName");
        this.spec = spec;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public List<String> getPartitionSpec()
    {
        return spec;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, spec);
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
        final SchemaPartitionName other = (SchemaPartitionName) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.spec, other.spec);
    }

    @JsonValue
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(schemaName + '.' + tableName);
        Iterator<String> itr = spec.iterator();
        while (itr.hasNext()) {
            sb.append(" " + itr.next());
        }
        return sb.toString();
    }
}
