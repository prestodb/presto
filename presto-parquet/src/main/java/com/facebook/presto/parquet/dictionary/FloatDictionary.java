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
package com.facebook.presto.parquet.dictionary;

import com.facebook.presto.parquet.DictionaryPage;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.plain.PlainValuesReader.FloatPlainValuesReader;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.sizeOf;

public class FloatDictionary
        extends Dictionary
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FloatDictionary.class).instanceSize();

    private final float[] content;

    public FloatDictionary(DictionaryPage dictionaryPage)
            throws IOException
    {
        super(dictionaryPage.getEncoding());
        content = new float[dictionaryPage.getDictionarySize()];
        FloatPlainValuesReader floatReader = new FloatPlainValuesReader();
        ByteBufferInputStream inputStream = ByteBufferInputStream.wrap(ImmutableList.of(dictionaryPage.getSlice().toByteBuffer()));
        floatReader.initFromPage(dictionaryPage.getDictionarySize(), inputStream);
        for (int i = 0; i < content.length; i++) {
            content[i] = floatReader.readFloat();
        }
    }

    @Override
    public float decodeToFloat(int id)
    {
        return content[id];
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("content", content)
                .toString();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(content);
    }
}
