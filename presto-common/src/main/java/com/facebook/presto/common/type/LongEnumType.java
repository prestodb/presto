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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TypeUtils.normalizeEnumMap;
import static com.facebook.presto.common.type.TypeUtils.validateEnumMap;
import static java.lang.String.format;
import static java.util.stream.Collectors.toMap;

public class LongEnumType
        extends AbstractLongType
        implements EnumType<Long>
{
    private final LongEnumMap enumMap;

    public LongEnumType(QualifiedObjectName name, LongEnumMap enumMap)
    {
        super(new TypeSignature(name, TypeSignatureParameter.of(enumMap)));
        this.enumMap = enumMap;
    }

    @Override
    public Map<String, Long> getEnumMap()
    {
        return enumMap.getEnumMap();
    }

    @Override
    public Optional<String> getEnumKeyForValue(Long value)
    {
        return enumMap.getKeyForValue(value);
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return block.getLong(position);
    }

    @Override
    public Type getValueType()
    {
        return BIGINT;
    }

    @Override
    public String getDisplayName()
    {
        return getTypeSignature().getBase();
    }

    public static class LongEnumMap
    {
        private final Map<String, Long> enumMap;
        private final Map<Long, String> flippedEnumMap;

        @JsonCreator
        public LongEnumMap(@JsonProperty("enumMap") Map<String, Long> enumMap)
        {
            validateEnumMap(enumMap);
            this.enumMap = normalizeEnumMap(enumMap);
            this.flippedEnumMap = this.enumMap.entrySet().stream()
                    .collect(toMap(Map.Entry::getValue, Map.Entry::getKey));
        }

        @JsonProperty
        public Map<String, Long> getEnumMap()
        {
            return enumMap;
        }

        public Optional<String> getKeyForValue(Long value)
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

            LongEnumMap other = (LongEnumMap) o;

            return Objects.equals(this.enumMap, other.enumMap);
        }

        @Override
        public String toString()
        {
            return "enum:bigint{"
                    + enumMap.entrySet()
                    .stream()
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    .map(e -> format("\"%s\": %d", e.getKey().replaceAll("\"", "\"\""), e.getValue()))
                    .collect(Collectors.joining(", "))
                    + "}";
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(enumMap);
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

        LongEnumType other = (LongEnumType) o;

        return Objects.equals(getTypeSignature().getBase(), other.getTypeSignature().getBase())
                && Objects.equals(getEnumMap(), other.getEnumMap());
    }
}
