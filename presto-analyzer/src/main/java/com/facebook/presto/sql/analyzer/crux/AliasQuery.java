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
package com.facebook.presto.sql.analyzer.crux;

import static java.util.Objects.requireNonNull;

public class AliasQuery
        extends Query
{
    private final Query parent;
    private final Qualifier name;
    private final boolean isImplicit;

    public AliasQuery(CodeLocation location, Query parent, Qualifier name, boolean isImplicit)
    {
        super(QueryKind.ALIAS, location, parent.getSchema());
        this.parent = requireNonNull(parent, "parent is null");
        this.name = requireNonNull(name, "name is null");
        this.isImplicit = isImplicit;
    }

    public Query getParent()
    {
        return parent;
    }

    public Qualifier getName()
    {
        return name;
    }

    public boolean getIsImplicit()
    {
        return isImplicit;
    }
}
