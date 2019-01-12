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
package io.prestosql.spi.connector;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TableNotFoundException
        extends NotFoundException
{
    private final SchemaTableName tableName;

    public TableNotFoundException(SchemaTableName tableName)
    {
        this(tableName, format("Table '%s' not found", tableName));
    }

    public TableNotFoundException(SchemaTableName tableName, String message)
    {
        super(message);
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    public TableNotFoundException(SchemaTableName tableName, Throwable cause)
    {
        this(tableName, format("Table '%s' not found", tableName), cause);
    }

    public TableNotFoundException(SchemaTableName tableName, String message, Throwable cause)
    {
        super(message, cause);
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }
}
