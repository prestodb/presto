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
package com.facebook.presto.orc.reader;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.StreamDescriptor;

import java.util.function.Predicate;

import static java.lang.Math.max;

final class ReaderUtils
{
    private ReaderUtils() {}

    public static void verifyStreamType(StreamDescriptor streamDescriptor, Type actual, Predicate<Type> validTypes)
            throws OrcCorruptionException
    {
        if (validTypes.test(actual)) {
            return;
        }

        throw new OrcCorruptionException(
                streamDescriptor.getOrcDataSourceId(),
                "Can not read SQL type %s from ORC stream %s of type %s",
                actual,
                streamDescriptor.getStreamName(),
                streamDescriptor.getOrcTypeKind());
    }

    public static int minNonNullValueSize(int nonNullCount)
    {
        return max(nonNullCount + 1, 1025);
    }

    public static byte[] unpackByteNulls(byte[] values, boolean[] isNull)
    {
        byte[] result = new byte[isNull.length];

        int position = 0;
        for (int i = 0; i < isNull.length; i++) {
            if (!isNull[i]) {
                result[i] = values[position];
                position++;
            }
        }
        return result;
    }

    public static void unpackByteNulls(byte[] values, boolean[] isNull, int positionCount, int nonNullCount)
    {
        int position = nonNullCount - 1;
        for (int i = positionCount - 1; i >= 0; i--) {
            if (!isNull[i]) {
                values[i] = values[position--];
            }
            else {
                values[i] = 0;
            }
        }
    }

    public static void packByteArrayOffsetsAndNulls(byte[] values, int[] offsets, boolean[] nulls, int[] positions, int positionCount)
    {
        int valuesIndex = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];

            int length = copyValuesRange(values, offsets, valuesIndex, position);

            valuesIndex += length;

            offsets[i + 1] = offsets[i] + length;
            nulls[i] = nulls[position];
        }
    }

    public static void packByteArrayAndOffsets(byte[] values, int[] offsets, int[] positions, int positionCount)
    {
        int valuesIndex = 0;
        for (int i = 0; i < positionCount; i++) {
            int length = copyValuesRange(values, offsets, valuesIndex, positions[i]);
            valuesIndex += length;

            offsets[i + 1] = offsets[i] + length;
        }
    }

    public static short[] unpackShortNulls(short[] values, boolean[] isNull)
    {
        short[] result = new short[isNull.length];

        int position = 0;
        for (int i = 0; i < isNull.length; i++) {
            if (!isNull[i]) {
                result[i] = values[position];
                position++;
            }
        }
        return result;
    }

    public static int[] unpackIntNulls(int[] values, boolean[] isNull)
    {
        int[] result = new int[isNull.length];

        int position = 0;
        for (int i = 0; i < isNull.length; i++) {
            if (!isNull[i]) {
                result[i] = values[position];
                position++;
            }
        }
        return result;
    }

    public static long[] unpackLongNulls(long[] values, boolean[] isNull)
    {
        long[] result = new long[isNull.length];

        int position = 0;
        for (int i = 0; i < isNull.length; i++) {
            if (!isNull[i]) {
                result[i] = values[position];
                position++;
            }
        }
        return result;
    }

    public static void unpackLengthNulls(int[] values, boolean[] isNull, int nonNullCount)
    {
        int nullSuppressedPosition = nonNullCount - 1;
        for (int outputPosition = isNull.length - 1; outputPosition >= 0; outputPosition--) {
            if (isNull[outputPosition]) {
                values[outputPosition] = 0;
            }
            else {
                values[outputPosition] = values[nullSuppressedPosition];
                nullSuppressedPosition--;
            }
        }
    }

    public static void convertLengthVectorToOffsetVector(int[] vector)
    {
        int currentLength = vector[0];
        vector[0] = 0;
        for (int i = 1; i < vector.length; i++) {
            int nextLength = vector[i];
            vector[i] = vector[i - 1] + currentLength;
            currentLength = nextLength;
        }
    }

    public static void convertLengthVectorToOffsetVector(int[] lengthVector, boolean[] isNullVector, int positionCount, int[] offsetVector)
    {
        offsetVector[0] = 0;
        int lengthVectorIndex = 0;
        for (int i = 0; i < positionCount; i++) {
            if (isNullVector[i]) {
                offsetVector[i + 1] = offsetVector[i];
            }
            else {
                offsetVector[i + 1] = offsetVector[i] + lengthVector[lengthVectorIndex++];
            }
        }
    }

    public static void convertLengthVectorToOffsetVector(int[] lengthVector, int positionCount, int[] offsetVector)
    {
        offsetVector[0] = 0;
        for (int i = 0; i < positionCount; i++) {
            offsetVector[i + 1] = offsetVector[i] + lengthVector[i];
        }
    }

    private static int copyValuesRange(byte[] values, int[] offsets, int toIndex, int position)
    {
        int fromIndex = offsets[position];
        int length = offsets[position + 1] - fromIndex;

        if (length > 0 && fromIndex != toIndex) {
            System.arraycopy(values, fromIndex, values, toIndex, length);
        }

        return length;
    }
}
