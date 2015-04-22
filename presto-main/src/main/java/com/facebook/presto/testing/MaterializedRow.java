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
package com.facebook.presto.testing;

import com.google.common.base.Preconditions;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Materialize all values in a row
 * Special handling is added for Double types for approximate comparisons
 */
public class MaterializedRow
{
    private final int precision;
    private final List<Object> values;

    public MaterializedRow(int precision, Object... values)
    {
        this(precision, Arrays.asList(checkNotNull(values, "values is null")));
    }

    public MaterializedRow(int precision, List<Object> values)
    {
        checkArgument(precision > 0, "Need at least one digit of precision");
        this.precision = precision;

        this.values = new ArrayList<>(values.size());
        for (Object object : values) {
            if (object instanceof Double || object instanceof Float) {
                this.values.add(new ApproximateDouble(((Number) object).doubleValue(), precision));
            }
            else if (object instanceof Number) {
                this.values.add(((Number) object).longValue());
            }
            else {
                this.values.add(object);
            }
        }
    }

    public int getPrecision()
    {
        return precision;
    }

    public int getFieldCount()
    {
        return values.size();
    }

    public Object getField(int field)
    {
        Preconditions.checkElementIndex(field, values.size());
        Object o = values.get(field);
        return (o instanceof ApproximateDouble) ? ((ApproximateDouble) o).getValue() : o;
    }

    @Override
    public String toString()
    {
        return values.toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        MaterializedRow o = (MaterializedRow) obj;
        return Objects.equals(values, o.values);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(values);
    }

    private static class ApproximateDouble
    {
        private final Double value;
        private final Double normalizedValue;

        private ApproximateDouble(Double value, int precision)
        {
            this.value = value;
            if (value.isNaN() || value.isInfinite()) {
                this.normalizedValue = value;
            }
            else {
                this.normalizedValue = new BigDecimal(value).round(new MathContext(precision)).doubleValue();
            }
        }

        public Double getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return value.toString();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == this) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            ApproximateDouble o = (ApproximateDouble) obj;
            return Objects.equals(normalizedValue, o.normalizedValue);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(normalizedValue);
        }
    }
}
