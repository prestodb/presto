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
package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import static java.util.Objects.requireNonNull;

public enum ExecutionType
{
    JAVA("java"),
    NATIVE("native"),
    NATIVE_GPU("native-gpu");

    private final String value;

    ExecutionType(String value)
    {
        this.value = requireNonNull(value, "value is null");
    }

    @JsonValue
    public String getValue()
    {
        return value;
    }

    @JsonCreator
    public static ExecutionType fromValue(String value)
    {
        requireNonNull(value, "value is null");
        for (ExecutionType executionType : ExecutionType.values()) {
            if (executionType.getValue().equals(value)) {
                return executionType;
            }
        }
        throw new IllegalArgumentException("Unknown execution type: " + value);
    }

    @Override
    public String toString()
    {
        return value;
    }
}
