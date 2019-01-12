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

import static java.lang.String.format;

public class HiveViewNotSupportedException
        extends NotFoundException
{
    private final SchemaTableName tableName;

    public HiveViewNotSupportedException(SchemaTableName tableName)
    {
        this(tableName, format("Hive views are not supported: '%s'", tableName));
    }

    public HiveViewNotSupportedException(SchemaTableName tableName, String message)
    {
        super(message);
        this.tableName = tableName;
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }
}
