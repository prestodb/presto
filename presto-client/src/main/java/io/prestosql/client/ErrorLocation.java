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
package io.prestosql.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

@Immutable
public class ErrorLocation
{
    private final int lineNumber;
    private final int columnNumber;

    @JsonCreator
    public ErrorLocation(
            @JsonProperty("lineNumber") int lineNumber,
            @JsonProperty("columnNumber") int columnNumber)
    {
        checkArgument(lineNumber >= 1, "lineNumber must be at least one");
        checkArgument(columnNumber >= 1, "columnNumber must be at least one");

        this.lineNumber = lineNumber;
        this.columnNumber = columnNumber;
    }

    @JsonProperty
    public int getLineNumber()
    {
        return lineNumber;
    }

    @JsonProperty
    public int getColumnNumber()
    {
        return columnNumber;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("lineNumber", lineNumber)
                .add("columnNumber", columnNumber)
                .toString();
    }
}
