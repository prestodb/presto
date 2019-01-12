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
package io.prestosql.plugin.hive;

import io.prestosql.spi.connector.NotFoundException;
import io.prestosql.spi.connector.SchemaTableName;

import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PartitionNotFoundException
        extends NotFoundException
{
    private final SchemaTableName tableName;
    private final List<String> partitionValues;

    public PartitionNotFoundException(SchemaTableName tableName, List<String> partitionValue)
    {
        this(tableName, partitionValue, format("Partition '%s' not found", tableName), null);
    }

    public PartitionNotFoundException(SchemaTableName tableName, List<String> partitionValues, String message)
    {
        this(tableName, partitionValues, message, null);
    }

    public PartitionNotFoundException(SchemaTableName tableName, List<String> partitionValue, Throwable cause)
    {
        this(tableName, partitionValue, format("Partition '%s' not found", tableName), cause);
    }

    public PartitionNotFoundException(SchemaTableName tableName, List<String> partitionValues, String message, Throwable cause)
    {
        super(message, cause);
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.partitionValues = requireNonNull(partitionValues, "partitionValue is null");
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }

    public List<String> getPartitionValues()
    {
        return partitionValues;
    }
}
