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
package com.facebook.presto.lark.sheets;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class LarkSheetsTableHandle
        implements ConnectorTableHandle
{
    private final String spreadsheetToken;
    private final String sheetId;
    private final String sheetTitle;
    private final int sheetIndex;
    private final int columnCount;
    private final int rowCount;

    @JsonCreator
    public LarkSheetsTableHandle(
            @JsonProperty("spreadsheetToken") String spreadsheetToken,
            @JsonProperty("sheetId") String sheetId,
            @JsonProperty("sheetTitle") String sheetTitle,
            @JsonProperty("sheetIndex") int sheetIndex,
            @JsonProperty("columnCount") int columnCount,
            @JsonProperty("rowCount") int rowCount)
    {
        this.spreadsheetToken = requireNonNull(spreadsheetToken, "spreadsheetToken is null");
        this.sheetId = requireNonNull(sheetId, "sheetId is null");
        this.sheetTitle = requireNonNull(sheetTitle, "sheetTitle is null");
        this.sheetIndex = sheetIndex;
        this.columnCount = columnCount;
        this.rowCount = rowCount;
    }

    @JsonProperty
    public String getSpreadsheetToken()
    {
        return spreadsheetToken;
    }

    @JsonProperty
    public String getSheetId()
    {
        return sheetId;
    }

    @JsonProperty
    public String getSheetTitle()
    {
        return sheetTitle;
    }

    @JsonProperty
    public int getSheetIndex()
    {
        return sheetIndex;
    }

    @JsonProperty
    public int getColumnCount()
    {
        return columnCount;
    }

    @JsonProperty
    public int getRowCount()
    {
        return rowCount;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(spreadsheetToken, sheetId);
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
        LarkSheetsTableHandle that = (LarkSheetsTableHandle) o;
        return sheetIndex == that.sheetIndex && spreadsheetToken.equals(that.spreadsheetToken) && sheetId.equals(that.sheetId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(spreadsheetToken, sheetId, sheetIndex);
    }

    @Override
    public String toString()
    {
        return spreadsheetToken + "." + sheetId + "#" + sheetIndex;
    }
}
