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
package com.facebook.presto.google.sheets;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SheetsTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final SheetsTableHandle table;

    @JsonCreator
    public SheetsTableLayoutHandle(@JsonProperty("table") SheetsTableHandle table)
    {
        this.table = table;
    }

    @JsonProperty
    public SheetsTableHandle getTable()
    {
        return table;
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
