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
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IrConstantJsonSequence
        extends IrPathNode
{
    public static final IrConstantJsonSequence EMPTY_SEQUENCE = new IrConstantJsonSequence(ImmutableList.of(), Optional.empty());

    private final List<JsonNode> sequence;

    public static IrConstantJsonSequence singletonSequence(JsonNode jsonNode, Optional<Type> type)
    {
        return new IrConstantJsonSequence(ImmutableList.of(jsonNode), type);
    }

    @JsonCreator
    public IrConstantJsonSequence(@JsonProperty("sequence") List<JsonNode> sequence, @JsonProperty("type") Optional<Type> type)
    {
        super(type);
        this.sequence = ImmutableList.copyOf(requireNonNull(sequence, "sequence is null"));
    }

    @Override
    protected <R, C> R accept(IrJsonPathVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrConstantJsonSequence(this, context);
    }

    @JsonProperty
    public List<JsonNode> getSequence()
    {
        return sequence;
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
        IrConstantJsonSequence other = (IrConstantJsonSequence) obj;
        return Objects.equals(this.sequence, other.sequence);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sequence);
    }
}
