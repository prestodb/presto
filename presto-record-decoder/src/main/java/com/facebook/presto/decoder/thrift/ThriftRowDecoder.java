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
package com.facebook.presto.decoder.thrift;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
import com.google.common.base.Splitter;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import javax.inject.Inject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Thrift specific row decoder
 */
public class ThriftRowDecoder
        implements RowDecoder
{
    public static final String NAME = "thrift";

    @Inject
    public ThriftRowDecoder()
    {
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public boolean decodeRow(byte[] data,
            Map<String, String> dataMap,
            Set<FieldValueProvider> fieldValueProviders,
            List<DecoderColumnHandle> columnHandles,
            Map<DecoderColumnHandle, FieldDecoder<?>> fieldDecoders)
    {
        ThriftGenericRow row = new ThriftGenericRow();
        try {
            TDeserializer deser = new TDeserializer();
            deser.deserialize(row, data);
            row.parse();
        }
        catch (TException e) {
            return true;
        }

        for (DecoderColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                continue;
            }

            @SuppressWarnings("unchecked")
            FieldDecoder<Object> decoder = (FieldDecoder<Object>) fieldDecoders.get(columnHandle);

            if (decoder != null) {
                Object node = locateNode(row.getValues(), columnHandle);
                fieldValueProviders.add(decoder.decode(node, columnHandle));
            }
        }
        return false;
    }

    private static Object locateNode(Map<Short, Object> map, DecoderColumnHandle columnHandle)
    {
        Map<Short, Object> currentLevel = map;
        Object val = null;

        Iterator<String> it = Splitter.on('/').omitEmptyStrings().split(columnHandle.getMapping()).iterator();
        while (it.hasNext()) {
            String pathElement = it.next();
            Short key = Short.valueOf(pathElement);
            val = currentLevel.get(key);

            // could be because of optional fields
            if (val == null) {
                return null;
            }

            if (val instanceof ThriftGenericRow) {
                currentLevel = ((ThriftGenericRow) val).getValues();
            }
            else if (it.hasNext()) {
                throw new IllegalStateException("Invalid thrift field schema");
            }
        }

        return val;
    }
}
