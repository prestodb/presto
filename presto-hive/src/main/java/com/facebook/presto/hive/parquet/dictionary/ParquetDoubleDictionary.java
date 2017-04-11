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
import parquet.column.values.plain.PlainValuesReader.DoublePlainValuesReader;

import java.io.IOException;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ParquetDoubleDictionary
        extends ParquetDictionary
{
    private final double[] content;

    public ParquetDoubleDictionary(ParquetDictionaryPage dictionaryPage)
            throws IOException
    {
        super(dictionaryPage.getEncoding());
        content = new double[dictionaryPage.getDictionarySize()];
        DoublePlainValuesReader doubleReader = new DoublePlainValuesReader();
        doubleReader.initFromPage(dictionaryPage.getDictionarySize(), dictionaryPage.getSlice().getBytes(), 0);
        for (int i = 0; i < content.length; i++) {
            content[i] = doubleReader.readDouble();
        }
    }

    @Override
    public double decodeToDouble(int id)
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
