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
package com.facebook.presto.common.type;

import com.facebook.presto.common.type.encoding.Base32;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.TypeUtils.normalizeEnumMap;
import static com.facebook.presto.common.type.TypeUtils.validateEnumMap;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class VarcharEnumType
        extends AbstractVarcharType
        implements EnumType<String>
{
    private final VarcharEnumMap enumMap;

    public VarcharEnumType(VarcharEnumMap enumMap)
    {
        super(VarcharType.UNBOUNDED_LENGTH, new TypeSignature(StandardTypes.VARCHAR_ENUM, TypeSignatureParameter.of(enumMap)));
        this.enumMap = enumMap;
    }

    @Override
    public Map<String, String> getEnumMap()
    {
        return enumMap.getEnumMap();
    }

    @Override
    public Optional<String> getEnumKeyForValue(String value)
    {
        return enumMap.getKeyForValue(value);
    }

    @Override
    public Type getValueType()
    {
        return VARCHAR;
    }

    @Override
    public String getDisplayName()
    {
        return enumMap.getTypeName();
    }

    public static class VarcharEnumMap
    {
        private final String typeName;
        private final Map<String, String> enumMap;
        private final Map<String, String> flippedEnumMap;

        @JsonCreator
        public VarcharEnumMap(@JsonProperty("typeName") String typeName, @JsonProperty("enumMap") Map<String, String> enumMap)
        {
            validateEnumMap(requireNonNull(enumMap, "enumMap is null"));
            this.typeName = requireNonNull(typeName.toLowerCase(ENGLISH), "typeName is null");
            this.enumMap = normalizeEnumMap(enumMap);
            this.flippedEnumMap = this.enumMap.entrySet().stream()
                    .collect(toMap(Map.Entry::getValue, Map.Entry::getKey));
        }

        @JsonProperty
        public String getTypeName()
        {
            return typeName;
        }

        @JsonProperty
        public Map<String, String> getEnumMap()
        {
            return enumMap;
        }

        public Optional<String> getKeyForValue(String value)
        {
            return Optional.ofNullable(flippedEnumMap.get(value));
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

            VarcharEnumMap other = (VarcharEnumMap) o;

            return Objects.equals(typeName, other.typeName) && Objects.equals(enumMap, other.enumMap);
        }

        @Override
        public String toString()
        {
            // Varchar enum values are base32-encoded so that they are case-insensitive, which is expected of TypeSigntures
            Base32 base32 = new Base32();
            return format("%s{%s}", typeName, enumMap.entrySet().stream()
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    .map(e -> format("\"%s\": \"%s\"", e.getKey().replaceAll("\"", "\"\""), base32.encodeAsString(e.getValue().getBytes())))
                    .collect(Collectors.joining(", ")));
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(typeName, enumMap);
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getTypeSignature().getBase(), enumMap);
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

        VarcharEnumType other = (VarcharEnumType) o;

        return Objects.equals(getTypeSignature().getBase(), other.getTypeSignature().getBase())
                && Objects.equals(getEnumMap(), other.getEnumMap());
    }
}
