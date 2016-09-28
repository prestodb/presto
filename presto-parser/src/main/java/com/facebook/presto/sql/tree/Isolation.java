

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
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class Isolation
        extends TransactionMode
{
    public enum Level
    {
        SERIALIZABLE("SERIALIZABLE"),
        REPEATABLE_READ("REPEATABLE READ"),
        READ_COMMITTED("READ COMMITTED"),
        READ_UNCOMMITTED("READ UNCOMMITTED");

        private final String text;

        Level(String text)
        {
            this.text = requireNonNull(text, "text is null");
        }

        public String getText()
        {
            return text;
        }
    }

    private final Level level;

    public Isolation(Level level)
    {
        this(Optional.empty(), level);
    }

    public Isolation(NodeLocation location, Level level)
    {
        this(Optional.of(location), level);
    }

    private Isolation(Optional<NodeLocation> location, Level level)
    {
        super(location);
        this.level = requireNonNull(level, "level is null");
    }

    public Level getLevel()
    {
        return level;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitIsolationLevel(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(level);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Isolation other = (Isolation) obj;
        return this.level == other.level;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("level", level)
                .toString();
    }
}
