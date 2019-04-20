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

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.spi.type.Type;

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
            result[i] = values[position];
            if (!isNull[i]) {
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
}
