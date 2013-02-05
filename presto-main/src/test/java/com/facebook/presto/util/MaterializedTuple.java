package com.facebook.presto.util;

import com.facebook.presto.tuple.TupleReadable;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Materialize all values from a Tuple
 * Special handling is added for Double types for approximate comparisons
 */
public class MaterializedTuple
{
    private final List<Object> values;

    public MaterializedTuple(TupleReadable tupleReadable, int precision)
    {
        checkNotNull(tupleReadable, "tupleReadable is null");
        checkArgument(precision > 0, "Need at least one digit of precision");

        List<Object> source = tupleReadable.getTuple().toValues();

        values = new ArrayList<>(source.size());
        for (Object object : source) {
            if (object instanceof Double) {
                values.add(new ApproximateDouble((Double) object, precision));
            }
            else {
                values.add(object);
            }
        }
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
        MaterializedTuple o = (MaterializedTuple) obj;
        return Objects.equal(values, o.values);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(values);
    }

    private static class ApproximateDouble
    {
        private final Double value;
        private final Double normalizedValue;

        private ApproximateDouble(Double value, int precision)
        {
            this.value = value;
            this.normalizedValue = new BigDecimal(value).round(new MathContext(precision)).doubleValue();
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
            return Objects.equal(normalizedValue, o.normalizedValue);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(normalizedValue);
        }
    }
}
