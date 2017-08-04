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
package com.facebook.presto.execution;

import com.facebook.presto.spi.WarningCode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class PrestoWarning
{
    private final WarningCode code;
    private final String message;

    @JsonCreator
    public PrestoWarning(@JsonProperty("warning") WarningCode code, @JsonProperty("message") String message)
    {
        this.code = requireNonNull(code, "code is null");
        this.message = requireNonNull(message, "message is null");
    }

    @JsonProperty
    public WarningCode getCode()
    {
        return code;
    }

    @JsonProperty
    public String getMessage()
    {
        return message;
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
        PrestoWarning that = (PrestoWarning) o;
        return code == that.code &&
                Objects.equals(message, that.message);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(code, message);
    }

    @Override
    public String toString()
    {
        return code.name() + ": " + message;
    }
}
