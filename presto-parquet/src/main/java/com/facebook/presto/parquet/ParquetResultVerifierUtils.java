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
package com.facebook.presto.parquet;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.reader.ColumnChunk;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Joiner;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static java.lang.String.format;

public class ParquetResultVerifierUtils
{
    private ParquetResultVerifierUtils()
    {
    }

    public static void verifyColumnChunks(ColumnChunk actual, ColumnChunk expected, boolean isNestedColumn, PrimitiveField field, ParquetDataSourceId sourceId)
    {
        Block actualBlock = actual.getBlock();
        Block expectedBlock = expected.getBlock();
        Type type = field.getType();
        String column = Joiner.on(".").join(field.getDescriptor().getPath());

        if (actualBlock.getPositionCount() != expectedBlock.getPositionCount()) {
            throw new PrestoException(ParquetErrorCode.PARQUET_INCORRECT_DECODING,
                    format("Wrong count: ex=%d, act=%d, col=%s, file=%s", expectedBlock.getPositionCount(), actualBlock.getPositionCount(), column, sourceId));
        }
        for (int position = 0; position < actualBlock.getPositionCount(); position++) {
            Object actualValue;
            Object expectedValue;
            if (type.equals(TIMESTAMP)) {
                actualValue = actualBlock.isNull(position) ? null : type.getLong(actualBlock, position);
                expectedValue = expectedBlock.isNull(position) ? null : type.getLong(expectedBlock, position);
            }
            else {
                actualValue = type.getObjectValue(null, actualBlock, position);
                expectedValue = type.getObjectValue(null, expectedBlock, position);
            }

            if (actualValue == null || expectedValue == null) {
                if (actualValue != expectedValue) {
                    throw new PrestoException(ParquetErrorCode.PARQUET_INCORRECT_DECODING,
                            format("Wrong value: pos=%d, ex=%s, act=%s, col=%s-%s, file=%s", position, expectedValue, actualValue, column, type, sourceId));
                }
            }
            else {
                if (!actualValue.equals(expectedValue)) {
                    throw new PrestoException(ParquetErrorCode.PARQUET_INCORRECT_DECODING,
                            format("Wrong value: pos=%d, ex=%s, act=%s, col=%s-%s, file=%s", position, expectedValue, actualValue, column, type, sourceId));
                }
            }
        }

        if (isNestedColumn) {
            int[] actualRLs = actual.getRepetitionLevels();
            int[] expectedRLs = expected.getRepetitionLevels();
            if (actualRLs.length != expectedRLs.length) {
                throw new PrestoException(ParquetErrorCode.PARQUET_INCORRECT_DECODING,
                        format("Wrong RL count: ex=%s, act=%s, col=%s-%s, file=%s", expectedRLs.length, actualRLs.length, column, type, sourceId));
            }
            for (int i = 0; i < actualRLs.length; i++) {
                if (actualRLs[i] != expectedRLs[i]) {
                    throw new PrestoException(ParquetErrorCode.PARQUET_INCORRECT_DECODING,
                            format("Wrong RL value: pos=%d, ex=%s, act=%s, col=%s-%s, file=%s", i, expectedRLs[i], actualRLs[i], column, type, sourceId));
                }
            }

            int[] actualDLs = actual.getDefinitionLevels();
            int[] expectedDLs = expected.getDefinitionLevels();
            if (actualDLs.length != expectedDLs.length) {
                throw new PrestoException(ParquetErrorCode.PARQUET_INCORRECT_DECODING,
                        format("Wrong DL count: ex=%s, act=%s, col=%s-%s, file=%s", expectedDLs.length, actualDLs.length, column, type, sourceId));
            }
            for (int i = 0; i < actualDLs.length; i++) {
                if (actualDLs[i] != expectedDLs[i]) {
                    throw new PrestoException(ParquetErrorCode.PARQUET_INCORRECT_DECODING,
                            format("Wrong RL value: pos=%d, ex=%s, act=%s, col=%s-%s, file=%s", i, expectedDLs[i], actualDLs[i], column, type, sourceId));
                }
            }
        }
    }
}
