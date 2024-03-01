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

import com.facebook.drift.annotations.ThriftEnum;
import com.facebook.drift.annotations.ThriftEnumValue;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Optional;

@ThriftEnum
public enum ParameterKind
{
    TYPE(Optional.of("TYPE_SIGNATURE"), 1),
    NAMED_TYPE(Optional.of("NAMED_TYPE_SIGNATURE"), 2),
    LONG(Optional.of("LONG_LITERAL"), 3),
    VARIABLE(Optional.empty(), 4),
    LONG_ENUM(Optional.of("LONG_ENUM"), 5),
    VARCHAR_ENUM(Optional.of("VARCHAR_ENUM"), 6),
    DISTINCT_TYPE(Optional.of("DISTINCT_TYPE"), 7);

    // TODO: drop special serialization code as soon as all clients
    //       migrate to version which can deserialize new format.

    private final Optional<String> oldName;
    private final int value;

    ParameterKind(Optional<String> oldName, int value)
    {
        this.oldName = oldName;
        this.value = value;
    }

    @JsonValue
    public String jsonName()
    {
        return oldName.orElse(name());
    }

    @JsonCreator
    public static ParameterKind fromJsonValue(String value)
    {
        for (ParameterKind kind : values()) {
            if (kind.oldName.isPresent() && kind.oldName.get().equals(value)) {
                return kind;
            }
            if (kind.name().equals(value)) {
                return kind;
            }
        }
        throw new IllegalArgumentException("Invalid serialized ParameterKind value: " + value);
    }

    @ThriftEnumValue
    public int getValue()
    {
        return value;
    }
}
