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
package com.facebook.presto.accumulo.iterators;

import com.google.common.collect.ImmutableMap;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RowFilter;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SingleColumnValueFilter
        extends RowFilter
        implements OptionDescriber
{
    public enum CompareOp
    {
        LESS, LESS_OR_EQUAL, EQUAL, NOT_EQUAL, GREATER_OR_EQUAL, GREATER
    }

    protected static final String CF = "family";
    protected static final String CQ = "qualifier";
    protected static final String COMPARE_OP = "compareOp";
    protected static final String VALUE = "value";

    private Text columnFamily;
    private Text columnQualifier;
    private Value value;
    private CompareOp compareOp;
    private boolean columnFound = false;

    @Override
    public boolean acceptRow(SortedKeyValueIterator<Key, Value> rowIterator)
            throws IOException
    {
        columnFound = false;
        while (rowIterator.hasTop()) {
            if (!acceptSingleKeyValue(rowIterator.getTopKey(), rowIterator.getTopValue())) {
                return false;
            }
            rowIterator.next();
        }

        return columnFound;
    }

    private boolean acceptSingleKeyValue(Key k, Value v)
    {
        if (k.compareColumnQualifier(columnQualifier) == 0 && k.compareColumnFamily(columnFamily) == 0) {
            columnFound = true;
            switch (compareOp) {
                case LESS:
                    return v.compareTo(value) < 0;
                case LESS_OR_EQUAL:
                    return v.compareTo(value) <= 0;
                case EQUAL:
                    return v.compareTo(value) == 0;
                case NOT_EQUAL:
                    return v.compareTo(value) != 0;
                case GREATER_OR_EQUAL:
                    return v.compareTo(value) >= 0;
                case GREATER:
                    return v.compareTo(value) > 0;
                default:
                    throw new RuntimeException("Unknown Compare op " + compareOp.name());
            }
        }
        return true;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException
    {
        super.init(source, options, env);
        columnFamily = new Text(options.get(CF));
        columnQualifier = new Text(options.get(CQ));
        compareOp = CompareOp.valueOf(options.get(COMPARE_OP));

        try {
            value = new Value(Hex.decodeHex(options.get(VALUE).toCharArray()));
        }
        catch (DecoderException e) {
            // should not occur, as validateOptions tries this same thing
            throw new IllegalArgumentException("Error decoding hex value in option", e);
        }
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env)
    {
        // Create a new SingleColumnValueFilter object based on the parent's
        // deepCopy
        SingleColumnValueFilter copy = new SingleColumnValueFilter();

        // Replicate all of the current options into the copy
        copy.columnFamily = new Text(this.columnFamily);
        copy.columnQualifier = new Text(this.columnQualifier);
        copy.value = new Value(this.value);
        copy.compareOp = this.compareOp;

        // Return the copy
        return copy;
    }

    @Override
    public IteratorOptions describeOptions()
    {
        return new IteratorOptions("singlecolumnvaluefilter", "Filter accepts or rejects each Key/Value pair based on the lexicographic comparison of a value stored in a single column family/qualifier",
                // @formatter:off
        ImmutableMap.<String, String>builder().put(CF, "column family to match on, required").put(CQ, "column qualifier to match on, required").put(COMPARE_OP, "CompareOp enum type for lexicographic comparison, required").put(VALUE, "Hex-encoded bytes of the value for comparison, required").build(),
        // @formatter:on
                null);
    }

    @Override
    public boolean validateOptions(Map<String, String> options)
    {
        checkNotNull(CF, options);
        checkNotNull(CQ, options);
        checkNotNull(COMPARE_OP, options);

        try {
            CompareOp.valueOf(options.get(COMPARE_OP));
        }
        catch (RuntimeException e) {
            throw new IllegalArgumentException("Unknown value of " + COMPARE_OP + ":" + options.get(COMPARE_OP));
        }

        checkNotNull(VALUE, options);

        try {
            new Value(Hex.decodeHex(options.get(VALUE).toCharArray()));
        }
        catch (DecoderException e) {
            throw new IllegalArgumentException("Option " + VALUE + " is not a hex-encoded value: " + options.get(VALUE), e);
        }

        return true;
    }

    private void checkNotNull(String opt, Map<String, String> options)
    {
        if (options.get(opt) == null) {
            throw new IllegalArgumentException("Option " + opt + " is required");
        }
    }

    public static Map<String, String> getProperties(String family, String qualifier, CompareOp op, byte[] value)
    {
        Map<String, String> opts = new HashMap<>();

        opts.put(CF, family);
        opts.put(CQ, qualifier);
        opts.put(COMPARE_OP, op.toString());
        opts.put(VALUE, Hex.encodeHexString(value));

        return opts;
    }

    @Override
    public String toString()
    {
        return String.format("SingleColumnValueFilter{columnFamily=%s,columnQualifier=%s,compareOp=%s,value=%s}", columnFamily, columnQualifier, compareOp, value);
    }
}
