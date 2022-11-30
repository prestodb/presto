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
package com.facebook.presto.analyzer.crux.tree;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class SelectItem
        extends SemanticTree
{
    private final Type type;
    private final ExpressionCardinality kind;
    private final List<String> names;
    private final Expression value;
    private final String regexp;

    public SelectItem(CodeLocation location, Type type, ExpressionCardinality kind, List<String> names, Expression value, String regexp)
    {
        super(Kind.SelectItem, location);
        this.type = requireNonNull(type, "type is null");
        this.kind = requireNonNull(kind, "kind is null");
        this.names = requireNonNull(names, "names is null");
        this.value = requireNonNull(value, "value is null");
        this.regexp = regexp;
    }

    public Type getType()
    {
        return type;
    }

    public ExpressionCardinality getKind()
    {
        return kind;
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
