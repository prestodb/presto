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

import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.SchemaPartitionName;

import static java.lang.String.format;

public class PartitionAlreadyExistsException
        extends NotFoundException
{
    private final SchemaPartitionName partitionName;

    public PartitionAlreadyExistsException(SchemaPartitionName partitionName)
    {
        this(partitionName, format("Partition already exists: '%s'", partitionName.toString()));
    }

    public PartitionAlreadyExistsException(SchemaPartitionName partitionName, String message)
    {
        super(message);
        this.partitionName = partitionName;
    }

    public SchemaPartitionName getPartitionName()
    {
        return partitionName;
    }
}
