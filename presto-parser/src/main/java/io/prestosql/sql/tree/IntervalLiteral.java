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
package io.prestosql.sql.tree;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IntervalLiteral
        extends Literal
{
    public enum Sign
    {
        POSITIVE {
            @Override
            public int multiplier()
            {
                return 1;
            }
        },
        NEGATIVE {
            @Override
            public int multiplier()
            {
                return -1;
            }
        };

        public abstract int multiplier();
    }

    public enum IntervalField
    {
        YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
    }

    private final String value;
    private final Sign sign;
    private final IntervalField startField;
    private final Optional<IntervalField> endField;

    public IntervalLiteral(String value, Sign sign, IntervalField startField)
    {
        this(Optional.empty(), value, sign, startField, Optional.empty());
    }

    public IntervalLiteral(String value, Sign sign, IntervalField startField, Optional<IntervalField> endField)
    {
        this(Optional.empty(), value, sign, startField, endField);
    }

    public IntervalLiteral(NodeLocation location, String value, Sign sign, IntervalField startField, Optional<IntervalField> endField)
    {
        this(Optional.of(location), value, sign, startField, endField);
    }

    private IntervalLiteral(Optional<NodeLocation> location, String value, Sign sign, IntervalField startField, Optional<IntervalField> endField)
    {
        super(location);
        requireNonNull(value, "value is null");
        requireNonNull(sign, "sign is null");
        requireNonNull(startField, "startField is null");
        requireNonNull(endField, "endField is null");

        this.value = value;
        this.sign = sign;
        this.startField = startField;
        this.endField = endField;
    }

    public String getValue()
    {
        return value;
    }

    public Sign getSign()
    {
        return sign;
    }

    public IntervalField getStartField()
    {
        return startField;
    }

    public Optional<IntervalField> getEndField()
    {
        return endField;
    }

    public boolean isYearToMonth()
    {
        return startField == IntervalField.YEAR || startField == IntervalField.MONTH;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitIntervalLiteral(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, sign, startField, endField);
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
        IntervalLiteral other = (IntervalLiteral) obj;
        return Objects.equals(this.value, other.value) &&
                Objects.equals(this.sign, other.sign) &&
                Objects.equals(this.startField, other.startField) &&
                Objects.equals(this.endField, other.endField);
    }
}
