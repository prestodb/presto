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
package com.facebook.presto.parquet.batchreader.dictionary;

import com.facebook.presto.parquet.DictionaryPage;
import com.facebook.presto.parquet.dictionary.Dictionary;

import static com.facebook.presto.parquet.ParquetTimestampUtils.getTimestampMillis;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TimestampDictionary
        extends Dictionary
{
    private final long[] dictionary;

    public TimestampDictionary(DictionaryPage dictionaryPage)
    {
        super(dictionaryPage.getEncoding());
        requireNonNull(dictionaryPage, "dictionaryPage is null");
        checkArgument(dictionaryPage.getDictionarySize() >= 0, "Dictionary size should be greater than or equal zero");
        checkArgument(dictionaryPage.getSlice().length() >= 12 * dictionaryPage.getDictionarySize(), "Dictionary buffer size is less than expected");

        int dictionarySize = dictionaryPage.getDictionarySize();
        byte[] pageBuffer = requireNonNull(dictionaryPage.getSlice(), "dictionary slice is null").getBytes();
        long[] dictionary = new long[dictionarySize];

        int offset = 0;
        for (int i = 0; i < dictionarySize; i++) {
            dictionary[i] = getTimestampMillis(pageBuffer, offset);
            offset += 12;
        }
        this.dictionary = dictionary;
    }

    @Override
    public long decodeToLong(int id)
    {
        return dictionary[id];
    }
}
