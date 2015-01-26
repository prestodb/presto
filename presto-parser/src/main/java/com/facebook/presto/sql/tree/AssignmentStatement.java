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

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class AssignmentStatement
        extends ControlStatement
{
    private final List<QualifiedName> targets;
    private final Expression value;

    public AssignmentStatement(List<QualifiedName> targets, Expression value)
    {
        this.targets = checkNotNull(targets, "targets is null");
        this.value = checkNotNull(value, "value is null");
    }

    public List<QualifiedName> getTargets()
    {
        return targets;
    }

    public Expression getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAssignmentStatement(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(targets, value);
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
        AssignmentStatement o = (AssignmentStatement) obj;
        return Objects.equals(targets, o.targets) &&
                Objects.equals(value, o.value);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("targets", targets)
                .add("value", value)
                .toString();
    }
}
