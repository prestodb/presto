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
package com.facebook.presto.orc.checkpoint;

import com.facebook.presto.orc.checkpoint.Checkpoints.ColumnPositionsList;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * InputStreamCheckpoint is represented as a packed long to avoid object creation in inner loops.
 */
public final class InputStreamCheckpoint
{
    private InputStreamCheckpoint()
    {
    }

    public static long createInputStreamCheckpoint(boolean compressed, ColumnPositionsList positionsList)
    {
        if (!compressed) {
            return createInputStreamCheckpoint(0, positionsList.nextPosition());
        }
        else {
            return createInputStreamCheckpoint(positionsList.nextPosition(), positionsList.nextPosition());
        }
    }

    public static long createInputStreamCheckpoint(int compressedBlockOffset, int decompressedOffset)
    {
        return (((long) compressedBlockOffset) << 32) | decompressedOffset;
    }

    public static int decodeCompressedBlockOffset(long inputStreamCheckpoint)
    {
        return ((int) (inputStreamCheckpoint >> 32));
    }

    public static int decodeDecompressedOffset(long inputStreamCheckpoint)
    {
        // low order bits contain the decompressed offset, so a simple cast here will suffice
        return (int) inputStreamCheckpoint;
    }

    public static String inputStreamCheckpointToString(long inputStreamCheckpoint)
    {
        return toStringHelper(InputStreamCheckpoint.class)
                .add("decompressedOffset", decodeDecompressedOffset(inputStreamCheckpoint))
                .add("compressedBlockOffset", decodeCompressedBlockOffset(inputStreamCheckpoint))
                .toString();
    }
}
