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
package com.facebook.presto.sql.jsonpath.tree;

import static java.util.Objects.requireNonNull;

public class JsonPath
        extends PathNode
{
    private final boolean lax;
    private final PathNode root;

    public JsonPath(boolean lax, PathNode root)
    {
        this.lax = lax;
        this.root = requireNonNull(root, "root is null");
    }

    @Override
    public <R, C> R accept(JsonPathTreeVisitor<R, C> visitor, C context)
    {
        return visitor.visitJsonPath(this, context);
    }

    public boolean isLax()
    {
        return lax;
    }

    public PathNode getRoot()
    {
        return root;
    }
}
