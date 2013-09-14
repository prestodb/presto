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

public class DropAlias
        extends Statement
{
    private final QualifiedName remote;

    public DropAlias(QualifiedName remote)
    {
        this.remote = checkNotNull(remote, "remote is null");
    }

    public QualifiedName getRemote()
    {
        return remote;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropAlias(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(remote);
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
        DropAlias o = (DropAlias) obj;
        return Objects.equal(remote, o.remote);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("remote", remote)
                .toString();
    }
}
