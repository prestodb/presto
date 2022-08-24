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

import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IrDatetimeMethod
        extends IrMethod
{
    private final Optional<String> format; // this is a string literal

    @JsonCreator
    public IrDatetimeMethod(@JsonProperty("base") IrPathNode base, @JsonProperty("format") Optional<String> format, @JsonProperty("type") Optional<Type> type)
    {
        super(base, type);
        this.format = requireNonNull(format, "format is null");
    }

    @Override
    protected <R, C> R accept(IrJsonPathVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrDatetimeMethod(this, context);
    }

    @JsonProperty
    public Optional<String> getFormat()
    {
        return format;
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
        IrDatetimeMethod other = (IrDatetimeMethod) obj;
        return Objects.equals(this.base, other.base) && Objects.equals(this.format, other.format);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(base, format);
    }
}
