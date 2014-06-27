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

public class SetVariable
        extends Statement
{
    private final QualifiedName var;
    private final Expression val;
    private final SetCommandType type;

    public enum SetCommandType
    {
        SET_VARIABLE,
        UNSET_VARIABLE,
        SHOW_VARIABLE,
        SHOW_ALL_VARIABLES
    }

    public SetVariable(QualifiedName var, Expression val, SetCommandType type)
    {
        this.var = checkNotNull(var, "name is null");
        this.val = val;
        this.type = type;
    }

    public SetVariable(QualifiedName var, SetCommandType type)
    {
        this.var = checkNotNull(var, "name is null");
        this.val = null;
        this.type = type;
    }

    public SetVariable()
    {
        this.var = null;
        this.val = null;
        this.type = SetCommandType.SHOW_ALL_VARIABLES;
    }

    public QualifiedName getVar()
    {
        return var;
    }

    public Expression getVal()
    {
        return val;
    }

    public SetCommandType getType()
    {
        return type;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSetVariable(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(var, val);
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
        SetVariable o = (SetVariable) obj;
        return Objects.equal(var, o.var) &&
                Objects.equal(val, o.val) &&
                Objects.equal(type, o.type);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("var", var)
                .add("val", val)
                .add("type", type)
                .toString();
    }
}
