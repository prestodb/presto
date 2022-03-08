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
package com.facebook.presto.lark.sheets.api;

import java.util.Comparator;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class SheetInfo
{
    private static final Comparator<SheetInfo> INDEX_COMPARATOR = Comparator.comparingInt(SheetInfo::getIndex);

    private final String token;
    private final String sheetId;
    private final String title;
    private final int index;
    private final int columnCount;
    private final int rowCount;

    public static Comparator<SheetInfo> indexComparator()
    {
        return INDEX_COMPARATOR;
    }

    public SheetInfo(
            String token,
            String sheetId,
            String title,
            int index,
            int columnCount,
            int rowCount)
    {
        this.token = requireNonNull(token, "spreadsheetToken is null");
        this.sheetId = requireNonNull(sheetId, "sheetId is null");
        this.title = requireNonNull(title, "title is null");
        this.index = index;
        this.columnCount = columnCount;
        this.rowCount = rowCount;
    }

    public String getToken()
    {
        return token;
    }

    public String getSheetId()
    {
        return sheetId;
    }

    public String getTitle()
    {
        return title;
    }

    public int getIndex()
    {
        return index;
    }

    public int getColumnCount()
    {
        return columnCount;
    }

    public int getRowCount()
    {
        return rowCount;
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
        SheetInfo sheetInfo = (SheetInfo) o;
        return index == sheetInfo.index && token.equals(sheetInfo.token) && sheetId.equals(sheetInfo.sheetId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(token, sheetId, index);
    }

    @Override
    public String toString()
    {
        return token + "." + sheetId + "#" + index;
    }
}
