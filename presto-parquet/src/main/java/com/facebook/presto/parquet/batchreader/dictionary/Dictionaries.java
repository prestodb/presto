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
import com.facebook.presto.parquet.dictionary.IntegerDictionary;
import com.facebook.presto.parquet.dictionary.LongDictionary;
import com.facebook.presto.spi.PrestoException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.ParquetDecodingException;

import static com.facebook.presto.parquet.ParquetErrorCode.PARQUET_UNSUPPORTED_ENCODING;

public class Dictionaries
{
    private Dictionaries()
    {
    }

    public static Dictionary createDictionary(ColumnDescriptor columnDescriptor, DictionaryPage dictionaryPage)
    {
        try {
            switch (columnDescriptor.getPrimitiveType().getPrimitiveTypeName()) {
                case INT32:
                case FLOAT:
                    return new IntegerDictionary(dictionaryPage);
                case INT64:
                case DOUBLE:
                    return new LongDictionary(dictionaryPage);
                case INT96:
                    return new TimestampDictionary(dictionaryPage);
                case BINARY:
                    return new BinaryBatchDictionary(dictionaryPage);
                case FIXED_LEN_BYTE_ARRAY:
                case BOOLEAN:
                default:
                    break;
            }
        }
        catch (Exception e) {
            throw new ParquetDecodingException("could not decode the dictionary for " + columnDescriptor, e);
        }

        throw new PrestoException(PARQUET_UNSUPPORTED_ENCODING, String.format("Dictionary encoding is not supported: %s", columnDescriptor));
    }
}
