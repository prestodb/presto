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

import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static java.lang.String.format;

public class MaterializedViewAlreadyExistsException
        extends PrestoException
{
    private final SchemaTableName viewName;

    public MaterializedViewAlreadyExistsException(SchemaTableName viewName)
    {
        this(viewName, format("Materialized view already exists: '%s'", viewName));
    }

    public MaterializedViewAlreadyExistsException(SchemaTableName viewName, String message)
    {
        this(viewName, message, null);
    }

    public MaterializedViewAlreadyExistsException(SchemaTableName viewName, String message, Throwable cause)
    {
        super(ALREADY_EXISTS, message, cause);
        this.viewName = viewName;
    }

    public SchemaTableName getViewName()
    {
        return viewName;
    }
}
