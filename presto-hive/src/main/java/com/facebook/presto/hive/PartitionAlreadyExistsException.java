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
package com.facebook.presto.hive;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PartitionAlreadyExistsException
        extends PrestoException
{
    private final SchemaTableName tableName;
    private final Optional<List<String>> partitionValues;

    public PartitionAlreadyExistsException(SchemaTableName tableName, Optional<List<String>> partitionValues)
    {
        this(tableName, partitionValues, format("Partition already exists: '%s' %s", tableName, partitionValues.map(Object::toString).orElse("")));
    }

    public PartitionAlreadyExistsException(SchemaTableName tableName, Optional<List<String>> partitionValues, String message)
    {
        this(tableName, partitionValues, message, null);
    }

    public PartitionAlreadyExistsException(SchemaTableName tableName, Optional<List<String>> partitionValues, String message, Throwable cause)
    {
        super(ALREADY_EXISTS, message, cause);
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.partitionValues = requireNonNull(partitionValues, "partitionValues is null");
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }

    public Optional<List<String>> getPartitionValues()
    {
        return partitionValues;
    }
}
