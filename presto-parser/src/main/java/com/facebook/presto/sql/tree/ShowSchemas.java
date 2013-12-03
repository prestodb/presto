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
package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class ShowSchemas
        extends Statement
{
    private final Optional<String> catalog;

    public ShowSchemas(Optional<String> catalog)
    {
        this.catalog = checkNotNull(catalog, "catalog is null");
    }

    public Optional<String> getCatalog()
    {
        return catalog;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowSchemas(this, context);
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        return (obj != null) && (getClass() == obj.getClass());
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this).toString();
    }
}
