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
package com.facebook.presto.hive.parquet.dictionary;

import com.facebook.presto.hive.parquet.ParquetDictionaryPage;
import parquet.column.values.plain.PlainValuesReader.LongPlainValuesReader;

import java.io.IOException;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ParquetLongDictionary
        extends ParquetDictionary
{
    private final long[] content;

    public ParquetLongDictionary(ParquetDictionaryPage dictionaryPage)
            throws IOException
    {
        super(dictionaryPage.getEncoding());
        content = new long[dictionaryPage.getDictionarySize()];
        LongPlainValuesReader longReader = new LongPlainValuesReader();
        longReader.initFromPage(dictionaryPage.getDictionarySize(), dictionaryPage.getSlice().getBytes(), 0);
        for (int i = 0; i < content.length; i++) {
            content[i] = longReader.readLong();
        }
    }

    @Override
    public long decodeToLong(int id)
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
}
