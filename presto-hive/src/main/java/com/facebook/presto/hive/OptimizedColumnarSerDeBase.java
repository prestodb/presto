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
package com.facebook.presto.hive;

import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarStructBase;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

/**
 * This is a copy of org.apache.hadoop.hive.serde2.ColumnarSerDeBase to work around
 * some package-protected fields needed by OptimizedLazyBinaryColumnSerde
 */
public abstract class OptimizedColumnarSerDeBase
        extends AbstractSerDe
{
    // The object for storing row data
    ColumnarStructBase cachedLazyStruct;
    // We need some initial values in case user don't call initialize()
    protected ObjectInspector cachedObjectInspector;

    protected long serializedSize;
    protected SerDeStats stats;
    protected boolean lastOperationSerialize;
    protected boolean lastOperationDeserialize;

    BytesRefArrayWritable serializeCache = new BytesRefArrayWritable();
    BytesRefWritable[] field;
    ByteStream.Output serializeStream = new ByteStream.Output();

    @Override
    public Object deserialize(Writable blob)
            throws SerDeException
    {
        if (!(blob instanceof BytesRefArrayWritable)) {
            throw new SerDeException(getClass().toString()
                    + ": expects BytesRefArrayWritable!");
        }

        BytesRefArrayWritable cols = (BytesRefArrayWritable) blob;
        cachedLazyStruct.init(cols);
        lastOperationSerialize = false;
        lastOperationDeserialize = true;
        return cachedLazyStruct;
    }

    @Override
    public SerDeStats getSerDeStats()
    {
        // must be different
        assert (lastOperationSerialize != lastOperationDeserialize);

        if (lastOperationSerialize) {
            stats.setRawDataSize(serializedSize);
        }
        else {
            stats.setRawDataSize(cachedLazyStruct.getRawDataSerializedSize());
        }
        return stats;
    }

    @Override
    public Class<? extends Writable> getSerializedClass()
    {
        return BytesRefArrayWritable.class;
    }

    protected void initialize(int size)
            throws SerDeException
    {
        field = new BytesRefWritable[size];
        for (int i = 0; i < size; i++) {
            field[i] = new BytesRefWritable();
            serializeCache.set(i, field[i]);
        }

        serializedSize = 0;
        stats = new SerDeStats();
        lastOperationSerialize = false;
        lastOperationDeserialize = false;
    }

    @Override
    public ObjectInspector getObjectInspector()
            throws SerDeException
    {
        return cachedObjectInspector;
    }
}
