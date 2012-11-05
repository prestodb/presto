package com.facebook.presto.tuple;

import java.util.Comparator;

/**
 * Compares two field-aligned TupleReadables field by field.
 * Ordering preference is given to fields at lower indicies.
 */
public class FieldOrderedTupleComparator
        implements Comparator<TupleReadable>
{
    public static final FieldOrderedTupleComparator INSTANCE = new FieldOrderedTupleComparator();

    @Override
    public int compare(TupleReadable o1, TupleReadable o2)
    {
        for (int field = 0; field < o1.getTupleInfo().getFieldCount(); field++) {
            TupleInfo.Type type = o1.getTupleInfo().getTypes().get(field);
            int comparison;
            switch (type) {
                case FIXED_INT_64:
                    comparison = Long.compare(o1.getLong(field), o2.getLong(field));
                    break;
                case DOUBLE:
                    comparison = Double.compare(o1.getDouble(field), o2.getDouble(field));
                    break;
                case VARIABLE_BINARY:
                    comparison = o1.getSlice(field).compareTo(o2.getSlice(field));
                    break;
                default:
                    throw new AssertionError("unimplemented type: " + type);
            }
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }
}
