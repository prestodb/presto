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
package com.facebook.presto.sql.analyzer.crux;

public class CodeLocation
{
    private final String filename;
    private final int line;
    private final String function;
    private final QueryLocation queryLocation;

    public CodeLocation(String filename, int line, String function, QueryLocation queryLocation)
    {
        this.filename = filename;
        this.line = line;
        this.function = function;
        this.queryLocation = queryLocation;
    }

    public String getFilename()
    {
        return this.filename;
    }

    public int getLine()
    {
        return this.line;
    }

    public String getFunction()
    {
        return this.function;
    }

    public QueryLocation getQueryLocation()
    {
        return this.queryLocation;
    }
}
