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
package io.prestosql.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class PrestoWarning
{
    private final WarningCode warningCode;
    private final String message;

    @JsonCreator
    public PrestoWarning(
            @JsonProperty("warningCode") WarningCode warningCode,
            @JsonProperty("message") String message)
    {
        this.warningCode = requireNonNull(warningCode, "warningCode is null");
        this.message = requireNonNull(message, "message is null");
    }

    public PrestoWarning(WarningCodeSupplier warningCodeSupplier, String message)
    {
        this(requireNonNull(warningCodeSupplier, "warningCodeSupplier is null").toWarningCode(), message);
    }

    @JsonProperty
    public WarningCode getWarningCode()
    {
        return warningCode;
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
        return Objects.equals(warningCode, that.warningCode) && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(warningCode, message);
    }

    @Override
    public String toString()
    {
        return format("%s, %s", warningCode, message);
    }
}
