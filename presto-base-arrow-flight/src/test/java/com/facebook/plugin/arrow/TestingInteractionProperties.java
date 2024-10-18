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
package com.facebook.plugin.arrow;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TestingInteractionProperties
{
    @JsonProperty("select_statement")
    private String selectStatement;

    @JsonProperty("schema_name")
    private String schema;
    @JsonProperty("table_name")
    private String table;
    public String getSelectStatement()
    {
        return selectStatement;
    }

    public String getSchema()
    {
        return schema;
    }

    public String getTable()
    {
        return table;
    }

    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    public void setSelectStatement(String selectStatement)
    {
        this.selectStatement = selectStatement;
    }

    public void setTable(String table)
    {
        this.table = table;
    }
}
