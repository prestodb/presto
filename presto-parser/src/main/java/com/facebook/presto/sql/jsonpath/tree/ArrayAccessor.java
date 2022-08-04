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

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ArrayAccessor
        extends Accessor
{
    private final List<Subscript> subscripts;

    public ArrayAccessor(PathNode base, List<Subscript> subscripts)
    {
        super(base);
        this.subscripts = requireNonNull(subscripts, "subscripts is null");
    }

    @Override
    public <R, C> R accept(JsonPathTreeVisitor<R, C> visitor, C context)
    {
        return visitor.visitArrayAccessor(this, context);
    }

    public List<Subscript> getSubscripts()
    {
        return subscripts;
    }

    public static class Subscript
    {
        private final PathNode from;
        private final Optional<PathNode> to;

        public Subscript(PathNode from)
        {
            this.from = requireNonNull(from, "from is null");
            this.to = Optional.empty();
        }

        public Subscript(PathNode from, PathNode to)
        {
            this.from = requireNonNull(from, "from is null");
            this.to = Optional.of(requireNonNull(to, "to is null"));
        }

        public PathNode getFrom()
        {
            return from;
        }

        public Optional<PathNode> getTo()
        {
            return to;
        }
    }
}
