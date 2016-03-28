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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.hive.parquet.dictionary.ParquetBinaryDictionary;
import com.facebook.presto.hive.parquet.dictionary.ParquetDictionary;
import com.facebook.presto.hive.parquet.dictionary.ParquetDictionaryReader;
import com.facebook.presto.hive.parquet.dictionary.ParquetDoubleDictionary;
import com.facebook.presto.hive.parquet.dictionary.ParquetFloatDictionary;
import com.facebook.presto.hive.parquet.dictionary.ParquetIntegerDictionary;
import com.facebook.presto.hive.parquet.dictionary.ParquetLongDictionary;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.column.values.ValuesReader;
import parquet.column.values.bitpacking.ByteBitPackingValuesReader;
import parquet.column.values.boundedint.ZeroIntegerValuesReader;
import parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesReader;
import parquet.column.values.deltastrings.DeltaByteArrayReader;
import parquet.column.values.plain.BinaryPlainValuesReader;
import parquet.column.values.plain.BooleanPlainValuesReader;
import parquet.column.values.plain.FixedLenByteArrayPlainValuesReader;
import parquet.column.values.plain.PlainValuesReader.DoublePlainValuesReader;
import parquet.column.values.plain.PlainValuesReader.FloatPlainValuesReader;
import parquet.column.values.plain.PlainValuesReader.IntegerPlainValuesReader;
import parquet.column.values.plain.PlainValuesReader.LongPlainValuesReader;
import parquet.column.values.rle.RunLengthBitPackingHybridValuesReader;
import parquet.io.ParquetDecodingException;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static parquet.column.values.bitpacking.Packer.BIG_ENDIAN;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;

public enum ParquetEncoding
{
    PLAIN {
        @Override
        public ValuesReader getValuesReader(ColumnDescriptor descriptor, ParquetValuesType valuesType)
        {
            switch (descriptor.getType()) {
                case BOOLEAN:
                    return new BooleanPlainValuesReader();
                case BINARY:
                    return new BinaryPlainValuesReader();
                case FLOAT:
                    return new FloatPlainValuesReader();
                case DOUBLE:
                    return new DoublePlainValuesReader();
                case INT32:
                    return new IntegerPlainValuesReader();
                case INT64:
                    return new LongPlainValuesReader();
                case INT96:
                    return new FixedLenByteArrayPlainValuesReader(INT96_TYPE_LENGTH);
                case FIXED_LEN_BYTE_ARRAY:
                    return new FixedLenByteArrayPlainValuesReader(descriptor.getTypeLength());
                default:
                    throw new ParquetDecodingException("Plain values reader does not support: " + descriptor.getType());
            }
        }

        @Override
        public ParquetDictionary initDictionary(ColumnDescriptor descriptor, ParquetDictionaryPage dictionaryPage)
                throws IOException
        {
            switch (descriptor.getType()) {
                case BINARY:
                    return new ParquetBinaryDictionary(dictionaryPage);
                case FIXED_LEN_BYTE_ARRAY:
                    return new ParquetBinaryDictionary(dictionaryPage, descriptor.getTypeLength());
                case INT96:
                    return new ParquetBinaryDictionary(dictionaryPage, INT96_TYPE_LENGTH);
                case INT64:
                    return new ParquetLongDictionary(dictionaryPage);
                case DOUBLE:
                    return new ParquetDoubleDictionary(dictionaryPage);
                case INT32:
                    return new ParquetIntegerDictionary(dictionaryPage);
                case FLOAT:
                    return new ParquetFloatDictionary(dictionaryPage);
                default:
                    throw new ParquetDecodingException("Dictionary encoding does not support: " + descriptor.getType());
            }
        }
    },

    RLE {
        @Override
        public ValuesReader getValuesReader(ColumnDescriptor descriptor, ParquetValuesType valuesType)
        {
            int bitWidth = BytesUtils.getWidthFromMaxInt(getMaxLevel(descriptor, valuesType));
            if (bitWidth == 0) {
                return new ZeroIntegerValuesReader();
            }
            return new RunLengthBitPackingHybridValuesReader(bitWidth);
        }
    },

    BIT_PACKED {
        @Override
        public ValuesReader getValuesReader(ColumnDescriptor descriptor, ParquetValuesType valuesType)
        {
            return new ByteBitPackingValuesReader(getMaxLevel(descriptor, valuesType), BIG_ENDIAN);
        }
    },

    PLAIN_DICTIONARY {
        @Override
        public ValuesReader getDictionaryBasedValuesReader(ColumnDescriptor descriptor, ParquetValuesType valuesType, ParquetDictionary dictionary)
        {
            return RLE_DICTIONARY.getDictionaryBasedValuesReader(descriptor, valuesType, dictionary);
        }

        @Override
        public ParquetDictionary initDictionary(ColumnDescriptor descriptor, ParquetDictionaryPage dictionaryPage)
                throws IOException
        {
            return PLAIN.initDictionary(descriptor, dictionaryPage);
        }

        @Override
        public boolean usesDictionary()
        {
            return true;
        }
    },

    DELTA_BINARY_PACKED {
        @Override
        public ValuesReader getValuesReader(ColumnDescriptor descriptor, ParquetValuesType valuesType)
        {
            checkArgument(descriptor.getType() == INT32, "Encoding DELTA_BINARY_PACKED is only supported for type INT32");
            return new DeltaBinaryPackingValuesReader();
        }
    },

    DELTA_LENGTH_BYTE_ARRAY {
        @Override
        public ValuesReader getValuesReader(ColumnDescriptor descriptor, ParquetValuesType valuesType)
        {
            checkArgument(descriptor.getType() == BINARY, "Encoding DELTA_LENGTH_BYTE_ARRAY is only supported for type BINARY");
            return new DeltaLengthByteArrayValuesReader();
        }
    },

    DELTA_BYTE_ARRAY {
        @Override
        public ValuesReader getValuesReader(ColumnDescriptor descriptor, ParquetValuesType valuesType)
        {
            checkArgument(
                    descriptor.getType() == BINARY || descriptor.getType() == FIXED_LEN_BYTE_ARRAY,
                    "Encoding DELTA_BYTE_ARRAY is only supported for type BINARY and FIXED_LEN_BYTE_ARRAY");
            return new DeltaByteArrayReader();
        }
    },

    RLE_DICTIONARY {
        @Override
        public ValuesReader getDictionaryBasedValuesReader(ColumnDescriptor descriptor, ParquetValuesType valuesType, ParquetDictionary dictionary)
        {
            return new ParquetDictionaryReader(dictionary);
        }

        @Override
        public ParquetDictionary initDictionary(ColumnDescriptor descriptor, ParquetDictionaryPage dictionaryPage)
                throws IOException
        {
            return PLAIN.initDictionary(descriptor, dictionaryPage);
        }

        @Override
        public boolean usesDictionary()
        {
            return true;
        }
    };

    static final int INT96_TYPE_LENGTH = 12;

    static int getMaxLevel(ColumnDescriptor descriptor, ParquetValuesType valuesType)
    {
        switch (valuesType) {
            case REPETITION_LEVEL:
                return descriptor.getMaxRepetitionLevel();
            case DEFINITION_LEVEL:
                return descriptor.getMaxDefinitionLevel();
            case VALUES:
                if (descriptor.getType() == BOOLEAN) {
                    return 1;
                }
            default:
                throw new ParquetDecodingException("Unsupported Parquet values type: " + valuesType);
        }
    }

    public boolean usesDictionary()
    {
        return false;
    }

    public ParquetDictionary initDictionary(ColumnDescriptor descriptor, ParquetDictionaryPage dictionaryPage)
            throws IOException
    {
        throw new UnsupportedOperationException("Parquet Dictionary encoding is not supported for: " + name());
    }

    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ParquetValuesType valuesType)
    {
        throw new UnsupportedOperationException("Error decoding Parquet values in encoding: " + this.name());
    }

    public ValuesReader getDictionaryBasedValuesReader(ColumnDescriptor descriptor, ParquetValuesType valuesType, ParquetDictionary dictionary)
    {
        throw new UnsupportedOperationException("Parquet Dictionary encoding is not supported for: " + name());
    }
}
