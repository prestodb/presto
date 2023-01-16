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
package com.facebook.presto.json.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class IrStartsWithPredicate
        extends IrPredicate
{
    private final IrPathNode value;
    private final IrPathNode prefix;

    @JsonCreator
    public IrStartsWithPredicate(@JsonProperty("value") IrPathNode value, @JsonProperty("prefix") IrPathNode prefix)
    {
        super();
        this.value = requireNonNull(value, "value is null");
        this.prefix = requireNonNull(prefix, "prefix is null");
    }

    @Override
    protected <R, C> R accept(IrJsonPathVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrStartsWithPredicate(this, context);
    }

    @JsonProperty
    public IrPathNode getValue()
    {
        return value;
    }

    @JsonProperty
    public IrPathNode getPrefix()
    {
        return prefix;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        IrStartsWithPredicate other = (IrStartsWithPredicate) obj;
        return Objects.equals(this.value, other.value) &&
                Objects.equals(this.prefix, other.prefix);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, prefix);
    }
}
