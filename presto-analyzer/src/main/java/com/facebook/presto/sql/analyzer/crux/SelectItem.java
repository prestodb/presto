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

import java.util.List;

import static java.util.Objects.requireNonNull;

public class SelectItem
        extends SemanticTree
{
    private final List<String> names;
    private final Expression value;
    private final String regexp;

    public SelectItem(CodeLocation location, List<String> names, Expression value, String regexp)
    {
        super(SemanticTreeKind.SELECT_ITEM, location);
        this.names = requireNonNull(names, "names is null");
        this.value = requireNonNull(value, "value is null");
        this.regexp = regexp;
    }

    public List<String> getNames()
    {
        return names;
    }

    public Expression getValue()
    {
        return value;
    }

    public String getRegexp()
    {
        return regexp;
    }
}
