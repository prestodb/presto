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
package com.facebook.presto.sql.tree;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class ExplainFormat
        extends ExplainOption
{
    public enum Type
    {
        TEXT,
        GRAPHVIZ,
        JSON
    }

    private final Type type;

    public ExplainFormat(Type type)
    {
        this.type = checkNotNull(type, "type is null");
    }

    public Type getType()
    {
        return type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ExplainFormat o = (ExplainFormat) obj;
        return Objects.equals(type, o.type);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .toString();
    }
}
