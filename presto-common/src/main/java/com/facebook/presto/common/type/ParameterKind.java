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

/**
 * The {@code ParameterKind} enum represents various kinds of parameters used in Presto's type system.
 * The available parameter kinds are:
 *
 * <ul>
 * <li><b>TYPE</b>:
 * Used when the parameter itself is of type {@code TYPE_SIGNATURE}, representing a type definition.</li>
 *
 * <li><b>NAMED_TYPE</b>:
 * Represents parameters that are explicitly named and can be referenced using their name.
 *  This is primarily used when the base type is a row type.</li>
 *
 * <li><b>LONG</b>:
 *  Used for types that take a long literal as a parameter. Examples include
 *  types like {@code decimal} and {@code varchar}.</li>
 *
 * <li><b>VARIABLE</b>:
 * Used when variables are passed as parameters. This allows dynamic and flexible parameter handling.</li>
 *
 * <li><b>LONG_ENUM</b>:
 * Represents a mapping of string values to long values. It is efficient for cases where
 * symbolic names correspond to numeric values.</li>
 *
 * <li><b>VARCHAR_ENUM</b>:
 * Represents a mapping of string values to string values. This is useful for symbolic names
 * that do not require numeric representation.</li>
 *
 * <li><b>DISTINCT_TYPE</b>:
 * Represents distinct user-defined types, enabling the creation of custom types in Presto's type system.</li>
 * </ul>
 */

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
