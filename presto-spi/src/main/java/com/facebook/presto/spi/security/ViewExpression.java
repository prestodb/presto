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
package com.facebook.presto.spi.security;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ViewExpression
{
    private final String identity;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final String expression;

    public ViewExpression(String identity, Optional<String> catalog, Optional<String> schema, String expression)
    {
        this.identity = requireNonNull(identity, "identity is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.expression = requireNonNull(expression, "expression is null");

        if (!catalog.isPresent() && schema.isPresent()) {
            throw new IllegalArgumentException("catalog must be present if schema is present");
        }
    }

    public String getIdentity()
    {
        return identity;
    }

    public Optional<String> getCatalog()
    {
        return catalog;
    }

    public Optional<String> getSchema()
    {
        return schema;
    }

    public String getExpression()
    {
        return expression;
    }
}
