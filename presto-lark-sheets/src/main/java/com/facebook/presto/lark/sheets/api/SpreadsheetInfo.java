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

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public class SpreadsheetInfo
{
    private final String token;
    private final String title;
    private final long revision;
    private final List<SheetInfo> sheets;

    public SpreadsheetInfo(String token, String title, long revision, List<SheetInfo> sheets)
    {
        this.title = title;
        this.revision = revision;
        this.sheets = sheets;
        this.token = token;
    }

    public String getToken()
    {
        return token;
    }

    public String getTitle()
    {
        return title;
    }

    public long getRevision()
    {
        return revision;
    }

    public List<SheetInfo> getSheets()
    {
        return sheets;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("spreadsheetToken", token)
                .add("title", title)
                .add("revision", revision)
                .add("sheets", sheets)
                .toString();
    }
}
