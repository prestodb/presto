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
package com.facebook.presto.split;

import com.facebook.presto.spi.UncertainRangeConstant;
import com.facebook.presto.spi.InConstantRangePredicate;
import com.facebook.presto.spi.InConstantRangePredicate.Type;

public abstract class AbstractUncertainRangeConstant<T>
    implements UncertainRangeConstant
{
    private Type type;

    protected T lowerLimit;
    protected boolean includeLowerBoundary;

    protected T upperLimit;
    protected boolean includeUpperBoundary;

    public AbstractUncertainRangeConstant(
            Type type,
            T lowerLimit, boolean includeLowerBoundary,
            T upperLimit, boolean includeUpperBoundary) {
        this.type = type;
        this.lowerLimit = lowerLimit;
        this.includeLowerBoundary = includeLowerBoundary;
        this.upperLimit = upperLimit;
        this.includeUpperBoundary = includeUpperBoundary;
    }

    public Object getLowerLimit() {
        return lowerLimit;
    }

    public Object getUpperLimit() {
        return upperLimit;
    }

    public Type getType()
    {
        return type;
    }

    public boolean hasLowerLimit() {
        return lowerLimit != null;
    }

    public boolean hasUpperLimit() {
        return upperLimit != null;
    }

    public boolean includesLowerBoundary() {
        return includeLowerBoundary;
    }

    public boolean includesUpperBoundary() {
        return includeUpperBoundary;
    }

    protected abstract AbstractUncertainRangeConstant<T> clone();

    public UncertainRangeConstant withLowerBoundary(boolean include) {
        AbstractUncertainRangeConstant<T> copy = clone();
        copy.includeLowerBoundary = include;
        return copy;
    }

    public UncertainRangeConstant withUpperBoundary(boolean include) {
        AbstractUncertainRangeConstant<T> copy = clone();
        copy.includeUpperBoundary = include;
        return copy;
    }

    public UncertainRangeConstant lowerBound() {
        AbstractUncertainRangeConstant<T> copy = clone();
        copy.lowerLimit = null;
        copy.includeLowerBoundary = false;
        copy.upperLimit = lowerLimit;
        copy.includeUpperBoundary = !includesLowerBoundary();
        return copy;
    }

    public UncertainRangeConstant upperBound() {
        AbstractUncertainRangeConstant<T> copy = clone();
        copy.lowerLimit = upperLimit;
        copy.includeLowerBoundary = !includesUpperBoundary();
        copy.upperLimit = null;
        copy.includeUpperBoundary = false;
        return copy;
    }

    protected abstract boolean valueLessThan(T o1, T o2);

    public UncertainRangeConstant intersection(UncertainRangeConstant other)
    {
        if (!this.getClass().equals(other.getClass())) {
            throw new AssertionError("Litral type doesn't match");
        }
        AbstractUncertainRangeConstant<T> o = (AbstractUncertainRangeConstant<T>) other;

        AbstractUncertainRangeConstant<T> copy = clone();

        if (o.hasLowerLimit()) {
            if (!copy.hasLowerLimit() || valueLessThan(o.lowerLimit, copy.lowerLimit)) {
                copy.lowerLimit = o.lowerLimit;
                copy.includeLowerBoundary = o.includeLowerBoundary;
            } else if (copy.lowerLimit.equals(o.lowerLimit) && !o.includesLowerBoundary()) {
                copy.includeLowerBoundary = false;
            }
        }

        if (o.hasUpperLimit()) {
            if (!copy.hasUpperLimit() || valueLessThan(copy.upperLimit, o.upperLimit)) {
                copy.upperLimit = o.upperLimit;
                copy.includeUpperBoundary = o.includeUpperBoundary;
            } else if (copy.upperLimit.equals(o.upperLimit) && !o.includesUpperBoundary()) {
                copy.includeUpperBoundary = false;
            }
        }

        return copy;
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
        final AbstractUncertainRangeConstant<T> other = (AbstractUncertainRangeConstant<T>) obj;
        return this.type == other.type &&
            (this.lowerLimit == null ? other.lowerLimit == null :
             (this.lowerLimit.equals(other.lowerLimit) && this.includeLowerBoundary == other.includeLowerBoundary)) &&
            (this.upperLimit == null ? other.upperLimit == null :
             (this.upperLimit.equals(other.upperLimit) && this.includeUpperBoundary == other.includeUpperBoundary));
    }
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        if (hasLowerLimit()) {
            if (includesLowerBoundary()) {
                sb.append("[");
            } else {
                sb.append("(");
            }
            sb.append(lowerLimit);
        } else {
            sb.append("(-infinite");
        }
        sb.append(",");
        if (hasUpperLimit()) {
            sb.append(upperLimit);
            if (includesUpperBoundary()) {
                sb.append("]");
            } else {
                sb.append(")");
            }
        } else {
            sb.append("infinite)");
        }
        return sb.toString();
    }
}
