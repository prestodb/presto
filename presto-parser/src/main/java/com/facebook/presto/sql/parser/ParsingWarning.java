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
package com.facebook.presto.sql.parser;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ParsingWarning
{
    // Avoid extending ParsingException due to cost of filling in a stacktrace on creation
    private final int lineNumber;
    private final int columnNumber;
    private final String warning;

    public ParsingWarning(String warning, int line, int columnNumber)
    {
        this.warning = requireNonNull(warning, "warning is null");
        this.lineNumber = line;
        this.columnNumber = columnNumber;
    }

    public ParsingWarning(String message)
    {
        this(message, 1, 1);
    }

    public int getLineNumber()
    {
        return lineNumber;
    }

    public int getColumnNumber()
    {
        return columnNumber;
    }

    public String getWarning()
    {
        return warning;
    }

    public String getMessage()
    {
        return format("lineNumber %s:%s: %s", lineNumber, columnNumber, getWarning());
    }
}
