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

public class AlterTable
        extends Statement
{
    public enum FunctionType
    {
        RENAME
    }

    protected final FunctionType functionType;

    private final QualifiedName source;
    private final QualifiedName target;

    public AlterTable(FunctionType functionType,
                        QualifiedName source,
                        QualifiedName target)
    {
        this.functionType = checkNotNull(functionType, "Alter Table Function is null");
        this.source = checkNotNull(source, "source name is null");
        this.target = checkNotNull(target, "target name is null");
    }

    public FunctionType getType()
    {
        return functionType;
    }

    public QualifiedName getSource()
    {
        return source;
    }

    public QualifiedName getTarget()
    {
        return target;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitStatement(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(functionType, source, target);
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
        AlterTable o = (AlterTable) obj;
        return Objects.equal(functionType, o.functionType)
                && Objects.equal(source, o.source)
                && Objects.equal(target, o.target);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("functionType", functionType)
                .add("source", source)
                .add("target", target)
                .toString();
    }
}
