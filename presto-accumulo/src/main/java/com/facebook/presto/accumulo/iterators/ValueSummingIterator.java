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

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.LongCombiner.Type;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner.Encoder;
import org.apache.accumulo.core.iterators.WrappingIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.apache.accumulo.core.iterators.LongCombiner.FIXED_LEN_ENCODER;
import static org.apache.accumulo.core.iterators.LongCombiner.STRING_ENCODER;
import static org.apache.accumulo.core.iterators.LongCombiner.VAR_LEN_ENCODER;

/**
 * An iterator the sums all values, ignoring the keys.  Uses {@link LongCombiner} encoders and expects an option for "type".
 */
public class ValueSummingIterator
        extends WrappingIterator
        implements OptionDescriber
{
    public static final String TYPE = "type";

    private Encoder<Long> encoder = null;
    private Key topKey = null;
    private Value topValue = null;
    private boolean hasTop = false;
    private String type = null;
    private long sum = 0;

    /**
     * A convenience method for setting the long encoding type.
     *
     * @param is IteratorSetting object to configure.
     * @param type LongCombiner.Type specifying the encoding type.
     */
    public static void setEncodingType(IteratorSetting is, Type type)
    {
        is.addOption(TYPE, type.toString());
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException
    {
        super.init(source, options, env);
        validateOptions(options);
        sum = 0;
        hasTop = false;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
            throws IOException
    {
        super.seek(range, columnFamilies, inclusive);

        if (!super.hasTop()) {
            return;
        }

        do {
            topKey = super.getTopKey();
            if (!topKey.isDeleted()) {
                topValue = super.getTopValue();
                sum += encoder.decode(super.getTopValue().get());
            }
            super.next();
        }
        while (super.hasTop());

        topValue = new Value(encoder.encode(sum));
        hasTop = true;
    }

    @Override
    public boolean hasTop()
    {
        return hasTop;
    }

    @Override
    public Key getTopKey()
    {
        return topKey;
    }

    @Override
    public Value getTopValue()
    {
        return topValue;
    }

    @Override
    public void next()
            throws IOException
    {
        hasTop = false;
        sum = 0;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env)
    {
        ValueSummingIterator iterator = new ValueSummingIterator();
        iterator.encoder = encoder;
        iterator.hasTop = true;
        iterator.sum = sum;
        iterator.type = type;

        switch (Type.valueOf(type)) {
            case VARLEN:
                iterator.encoder = VAR_LEN_ENCODER;
                break;
            case FIXEDLEN:
                iterator.encoder = FIXED_LEN_ENCODER;
                break;
            case STRING:
                iterator.encoder = STRING_ENCODER;
                break;
        }

        return iterator;
    }

    @Override
    public OptionDescriber.IteratorOptions describeOptions()
    {
        OptionDescriber.IteratorOptions io = new IteratorOptions("keysummingcombiner", "LongCombiner can interpret Values as Longs in a variety of encodings (variable length, fixed length, or string) before combining", null, null);
        io.addNamedOption(TYPE, "<VARLEN|FIXEDLEN|STRING>");
        return io;
    }

    @Override
    public boolean validateOptions(Map<String, String> options)
    {
        try {
            this.type = options.get(TYPE);
            if (type == null) {
                throw new IllegalArgumentException("no type specified");
            }

            switch (Type.valueOf(type)) {
                case VARLEN:
                    this.encoder = VAR_LEN_ENCODER;
                    break;
                case FIXEDLEN:
                    this.encoder = FIXED_LEN_ENCODER;
                    break;
                case STRING:
                    this.encoder = STRING_ENCODER;
                    break;
            }
        }
        catch (Exception e) {
            throw new IllegalArgumentException("bad encoder option", e);
        }
        return true;
    }
}
