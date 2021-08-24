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

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ViewNotFoundException
        extends NotFoundException
{
    private final SchemaTableName viewName;

    public ViewNotFoundException(SchemaTableName viewName)
    {
        this(viewName, format("View '%s' not found", viewName));
    }

    public ViewNotFoundException(SchemaTableName viewName, String message)
    {
        super(message);
        this.viewName = requireNonNull(viewName, "viewName is null");
    }

    public ViewNotFoundException(SchemaTableName viewName, Throwable cause)
    {
        this(viewName, format("View '%s' not found", viewName), cause);
    }

    public ViewNotFoundException(SchemaTableName viewName, String message, Throwable cause)
    {
        super(message, cause);
        this.viewName = requireNonNull(viewName, "viewName is null");
    }

    public SchemaTableName getViewName()
    {
        return viewName;
    }
}
