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
package com.facebook.presto.spi.eventlistener;

import java.util.Objects;

public class QueryErrorLocation
{
    private final int lineNumber;
    private final int columnNumber;

    public QueryErrorLocation(int lineNumber, int columnNumber)
    {
        this.lineNumber = lineNumber;
        this.columnNumber = columnNumber;
    }

    public int getLineNumber()
    {
        return lineNumber;
    }

    public int getColumnNumber()
    {
        return columnNumber;
    }

    @Override
    public String toString()
    {
        return "line " + lineNumber + " column " + columnNumber;
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
        QueryErrorLocation that = (QueryErrorLocation) o;
        return lineNumber == that.lineNumber &&
                columnNumber == that.columnNumber;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lineNumber, columnNumber);
    }
}
