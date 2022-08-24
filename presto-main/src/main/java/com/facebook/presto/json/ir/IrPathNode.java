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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = IrAbsMethod.class, name = "abs"),
        @JsonSubTypes.Type(value = IrArithmeticBinary.class, name = "binary"),
        @JsonSubTypes.Type(value = IrArithmeticUnary.class, name = "unary"),
        @JsonSubTypes.Type(value = IrArrayAccessor.class, name = "arrayaccessor"),
        @JsonSubTypes.Type(value = IrCeilingMethod.class, name = "ceiling"),
        @JsonSubTypes.Type(value = IrComparisonPredicate.class, name = "comparison"),
        @JsonSubTypes.Type(value = IrConjunctionPredicate.class, name = "conjunction"),
        @JsonSubTypes.Type(value = IrConstantJsonSequence.class, name = "jsonsequence"),
        @JsonSubTypes.Type(value = IrContextVariable.class, name = "contextvariable"),
        @JsonSubTypes.Type(value = IrDatetimeMethod.class, name = "datetime"),
        @JsonSubTypes.Type(value = IrDisjunctionPredicate.class, name = "disjunction"),
        @JsonSubTypes.Type(value = IrDoubleMethod.class, name = "double"),
        @JsonSubTypes.Type(value = IrExistsPredicate.class, name = "exists"),
        @JsonSubTypes.Type(value = IrFilter.class, name = "filter"),
        @JsonSubTypes.Type(value = IrFloorMethod.class, name = "floor"),
        @JsonSubTypes.Type(value = IrIsUnknownPredicate.class, name = "isunknown"),
        @JsonSubTypes.Type(value = IrJsonNull.class, name = "jsonnull"),
        @JsonSubTypes.Type(value = IrKeyValueMethod.class, name = "keyvalue"),
        @JsonSubTypes.Type(value = IrLastIndexVariable.class, name = "last"),
        @JsonSubTypes.Type(value = IrLiteral.class, name = "literal"),
        @JsonSubTypes.Type(value = IrMemberAccessor.class, name = "memberaccessor"),
        @JsonSubTypes.Type(value = IrNamedJsonVariable.class, name = "namedjsonvariable"),
        @JsonSubTypes.Type(value = IrNamedValueVariable.class, name = "namedvaluevariable"),
        @JsonSubTypes.Type(value = IrNegationPredicate.class, name = "negation"),
        @JsonSubTypes.Type(value = IrPredicateCurrentItemVariable.class, name = "currentitem"),
        @JsonSubTypes.Type(value = IrSizeMethod.class, name = "size"),
        @JsonSubTypes.Type(value = IrStartsWithPredicate.class, name = "startswith"),
        @JsonSubTypes.Type(value = IrTypeMethod.class, name = "type"),
})
public abstract class IrPathNode
{
    // `type` is intentionally skipped in equals() and hashCode() methods of all IrPathNodes, so that
    // those methods consider te node's structure only. `type` is a function of the other properties,
    // and it might be optionally set or not, depending on when and how the node is created - e.g. either
    // initially or by some optimization that will be added in the future (like constant folding, tree flattening).
    private final Optional<Type> type;

    protected IrPathNode(Optional<Type> type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    protected <R, C> R accept(IrJsonPathVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrPathNode(this, context);
    }

    /**
     * Get the result type, whenever known.
     * Type might be known for IrPathNodes returning a singleton sequence (e.g. IrArithmeticBinary),
     * as well as for IrPathNodes returning a sequence of arbitrary length (e.g. IrSizeMethod).
     * If the node potentially returns a non-singleton sequence, this method shall return Type
     * only if the type is the same for all elements of the sequence.
     * NOTE: Type is not applicable to every IrPathNode. If the IrPathNode produces an empty sequence,
     * a JSON null, or a sequence containing non-literal JSON items, Type cannot be determined.
     */
    @JsonProperty
    public final Optional<Type> getType()
    {
        return type;
    }

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract int hashCode();
}
