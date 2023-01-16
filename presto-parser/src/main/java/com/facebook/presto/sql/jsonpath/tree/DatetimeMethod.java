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
package com.facebook.presto.sql.jsonpath.tree;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DatetimeMethod
        extends Method
{
    private final Optional<String> format;

    public DatetimeMethod(PathNode base, Optional<String> format)
    {
        super(base);
        this.format = requireNonNull(format, "format is null"); // TODO in IR, translate to input for java.time and create a formatter.
    }

    @Override
    public <R, C> R accept(JsonPathTreeVisitor<R, C> visitor, C context)
    {
        return visitor.visitDatetimeMethod(this, context);
    }

    public Optional<String> getFormat()
    {
        return format;
    }
}
