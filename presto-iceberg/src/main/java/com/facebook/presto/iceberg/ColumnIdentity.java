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
package com.facebook.presto.iceberg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.ARRAY;
import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.MAP;
import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.STRUCT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ColumnIdentity
{
    private final int id;
    private final String name;
    private final TypeCategory typeCategory;
    private final List<ColumnIdentity> children;
    private final Optional<IcebergTypeAttributes> typeAttributes;

    public ColumnIdentity(
            int id,
            String name,
            TypeCategory typeCategory,
            List<ColumnIdentity> children)
    {
        this(id, name, typeCategory, children, Optional.empty());
    }

    @JsonCreator
    public ColumnIdentity(
            @JsonProperty("id") int id,
            @JsonProperty("name") String name,
            @JsonProperty("typeCategory") TypeCategory typeCategory,
            @JsonProperty("children") List<ColumnIdentity> children,
            @JsonProperty("typeAttributes") Optional<IcebergTypeAttributes> typeAttributes)
    {
        this.id = id;
        this.name = requireNonNull(name, "name is null");
        this.typeCategory = requireNonNull(typeCategory, "typeCategory is null");
        this.children = requireNonNull(children, "children is null");
        this.typeAttributes = requireNonNull(typeAttributes, "typeAttributes is null");
        checkArgument(
                children.isEmpty() == (typeCategory == PRIMITIVE),
                "Children should be empty if and only if column type is primitive");
    }

    @JsonProperty
    public int getId()
    {
        return id;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public TypeCategory getTypeCategory()
    {
        return typeCategory;
    }

    @JsonProperty
    public List<ColumnIdentity> getChildren()
    {
        return children;
    }

    /**
     * Iceberg V3 type-disambiguation attributes for this node, empty when none
     * were derived. Modeled as Optional so the C++ protocol codegen emits a
     * nullable std::shared_ptr field and serialized only when present, so older
     * coordinators/workers tolerate the JSON shape without upgrade-ordering
     * constraints.
     */
    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    public Optional<IcebergTypeAttributes> getTypeAttributes()
    {
        return typeAttributes;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnIdentity that = (ColumnIdentity) o;
        return id == that.id &&
                name.equals(that.name) &&
                typeCategory == that.typeCategory &&
                children.equals(that.children) &&
                Objects.equals(typeAttributes, that.typeAttributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, name, typeCategory, children, typeAttributes);
    }

    @Override
    public String toString()
    {
        return id + ":" + name + ":" + typeCategory + ":" + children +
                (typeAttributes.isPresent() ? ":" + typeAttributes.get() : "");
    }

    public enum TypeCategory
    {
        PRIMITIVE,
        STRUCT,
        ARRAY,
        MAP
    }

    public static ColumnIdentity primitiveColumnIdentity(int id, String name)
    {
        return new ColumnIdentity(id, name, PRIMITIVE, ImmutableList.of());
    }

    public static ColumnIdentity createColumnIdentity(Types.NestedField column)
    {
        return createColumnIdentity(column.name(), column.fieldId(), column.type());
    }

    public static ColumnIdentity createColumnIdentity(String name, int id, org.apache.iceberg.types.Type fieldType)
    {
        Optional<IcebergTypeAttributes> typeAttributes = deriveTypeAttributes(fieldType);

        if (!fieldType.isNestedType()) {
            return new ColumnIdentity(id, name, PRIMITIVE, ImmutableList.of(), typeAttributes);
        }

        if (fieldType.isListType()) {
            ColumnIdentity elementColumn = createColumnIdentity(getOnlyElement(fieldType.asListType().fields()));
            return new ColumnIdentity(id, name, ARRAY, ImmutableList.of(elementColumn), typeAttributes);
        }

        if (fieldType.isStructType()) {
            List<ColumnIdentity> fieldColumns = fieldType.asStructType().fields().stream()
                    .map(ColumnIdentity::createColumnIdentity)
                    .collect(toImmutableList());
            return new ColumnIdentity(id, name, STRUCT, fieldColumns, typeAttributes);
        }

        if (fieldType.isMapType()) {
            List<ColumnIdentity> keyValueColumns = fieldType.asMapType().fields().stream()
                    .map(ColumnIdentity::createColumnIdentity)
                    .collect(toImmutableList());
            checkArgument(keyValueColumns.size() == 2, "Expected map type to have two fields");
            return new ColumnIdentity(id, name, MAP, keyValueColumns, typeAttributes);
        }

        throw new UnsupportedOperationException(format("Iceberg column type %s is not supported", fieldType.typeId()));
    }

    // Derives the Iceberg V3 type-disambiguation attributes (ORC Appendix A
    // semantics) that the format-erased Presto type cannot represent. Returns
    // Optional.empty() when nothing is set, so the attribute is omitted from the
    // serialized handle and column identities are unchanged for the common
    // case.
    //
    // Only UUID and FIXED are derived today: both Iceberg types map to Presto
    // VARBINARY, so the binary variant and (for FIXED) the length are
    // genuinely lost without these attributes. Iceberg LONG already maps 1:1
    // to Presto BIGINT (and INT to INTEGER), so a `long-type` attribute would
    // be redundant; timestamp unit is always microseconds in the current
    // Iceberg spec; and `iceberg.required` (nullability) is carried by the ORC
    // writer path. Those fields remain on IcebergTypeAttributes so the wire
    // format and the native worker are ready for them, but deriving them here
    // is deferred to avoid rewriting every column identity.
    private static Optional<IcebergTypeAttributes> deriveTypeAttributes(org.apache.iceberg.types.Type fieldType)
    {
        switch (fieldType.typeId()) {
            case UUID:
                return Optional.of(new IcebergTypeAttributes(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of("UUID"),
                        Optional.empty(),
                        Optional.of(16)));
            case FIXED:
                return Optional.of(new IcebergTypeAttributes(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of("FIXED"),
                        Optional.empty(),
                        Optional.of(((Types.FixedType) fieldType).length())));
            default:
                return Optional.empty();
        }
    }

    /**
     * Iceberg V3 type-disambiguation attributes that the format-erased Presto
     * type cannot represent (e.g. LONG vs INT widening, FIXED/UUID binary
     * variants, timestamp unit). Derived from the Iceberg schema during
     * planning and forwarded to the native worker so NIMBLE/ORC files can be
     * stamped with the spec-compliant ORC Appendix A attribute keys. Every
     * field is Optional so the C++ protocol codegen emits nullable
     * std::shared_ptr fields and each attribute is serialized only when present.
     */
    public static class IcebergTypeAttributes
    {
        private final Optional<Boolean> required;
        private final Optional<String> longType;
        private final Optional<String> timestampUnit;
        private final Optional<String> binaryType;
        private final Optional<String> structType;
        private final Optional<Integer> length;

        @JsonCreator
        public IcebergTypeAttributes(
                @JsonProperty("required") Optional<Boolean> required,
                @JsonProperty("longType") Optional<String> longType,
                @JsonProperty("timestampUnit") Optional<String> timestampUnit,
                @JsonProperty("binaryType") Optional<String> binaryType,
                @JsonProperty("structType") Optional<String> structType,
                @JsonProperty("length") Optional<Integer> length)
        {
            this.required = requireNonNull(required, "required is null");
            this.longType = requireNonNull(longType, "longType is null");
            this.timestampUnit = requireNonNull(timestampUnit, "timestampUnit is null");
            this.binaryType = requireNonNull(binaryType, "binaryType is null");
            this.structType = requireNonNull(structType, "structType is null");
            this.length = requireNonNull(length, "length is null");
        }

        @JsonProperty
        @JsonInclude(JsonInclude.Include.NON_ABSENT)
        public Optional<Boolean> getRequired()
        {
            return required;
        }

        @JsonProperty
        @JsonInclude(JsonInclude.Include.NON_ABSENT)
        public Optional<String> getLongType()
        {
            return longType;
        }

        @JsonProperty
        @JsonInclude(JsonInclude.Include.NON_ABSENT)
        public Optional<String> getTimestampUnit()
        {
            return timestampUnit;
        }

        @JsonProperty
        @JsonInclude(JsonInclude.Include.NON_ABSENT)
        public Optional<String> getBinaryType()
        {
            return binaryType;
        }

        @JsonProperty
        @JsonInclude(JsonInclude.Include.NON_ABSENT)
        public Optional<String> getStructType()
        {
            return structType;
        }

        @JsonProperty
        @JsonInclude(JsonInclude.Include.NON_ABSENT)
        public Optional<Integer> getLength()
        {
            return length;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IcebergTypeAttributes that = (IcebergTypeAttributes) o;
            return Objects.equals(required, that.required) &&
                    Objects.equals(longType, that.longType) &&
                    Objects.equals(timestampUnit, that.timestampUnit) &&
                    Objects.equals(binaryType, that.binaryType) &&
                    Objects.equals(structType, that.structType) &&
                    Objects.equals(length, that.length);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(required, longType, timestampUnit, binaryType, structType, length);
        }

        @Override
        public String toString()
        {
            return "IcebergTypeAttributes{" +
                    "required=" + required +
                    ", longType=" + longType +
                    ", timestampUnit=" + timestampUnit +
                    ", binaryType=" + binaryType +
                    ", structType=" + structType +
                    ", length=" + length +
                    '}';
        }
    }
}
