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

import static com.google.common.base.Preconditions.checkNotNull;

public class CreateAlias
        extends Statement
{
    private final QualifiedName alias;
    private final QualifiedName remote;

    public CreateAlias(QualifiedName alias, QualifiedName remote)
    {
        this.alias = checkNotNull(alias, "alias is null");
        this.remote = checkNotNull(remote, "remote is null");
    }

    public QualifiedName getAlias()
    {
        return alias;
    }

    public QualifiedName getRemote()
    {
        return remote;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateAlias(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(alias, remote);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        else if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        CreateAlias o = (CreateAlias) obj;
        return Objects.equal(alias, o.alias)
                && Objects.equal(remote, o.remote);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("alias", alias)
                .add("remote", remote)
                .toString();
    }
}
