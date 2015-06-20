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

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShowCreateView
        extends Statement
{
    private final QualifiedName view;

    public ShowCreateView(QualifiedName view)
    {
        this(Optional.empty(), view);
    }

    public ShowCreateView(NodeLocation location, QualifiedName view)
    {
        this(Optional.of(location), view);
    }

    private ShowCreateView(Optional<NodeLocation> location, QualifiedName view)
    {
        super(location);
        this.view = requireNonNull(view, "view is null");
    }

    public QualifiedName getView()
    {
        return view;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowCreateView(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(view);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ShowCreateView o = (ShowCreateView) obj;
        return Objects.equals(view, o.view);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("view", view)
                .toString();
    }
}
