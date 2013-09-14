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
package com.facebook.presto.serde;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

public class BlockEncodings
{
    public static BlockEncoding readBlockEncoding(SliceInput input)
    {
        byte encoding = input.readByte();
        switch (encoding) {
            case 0:
                return new UncompressedBlockEncoding(input);
            case 1:
                return new RunLengthBlockEncoding(input);
            case 2:
                return new DictionaryBlockEncoding(input);
            case 3:
                return new SnappyBlockEncoding(input);
            default:
                throw new IllegalArgumentException("unknown encoding " + encoding);
        }
    }

    public static void writeBlockEncoding(SliceOutput output, BlockEncoding encoding)
    {
        // write encoding id
        if (encoding instanceof UncompressedBlockEncoding) {
            output.writeByte(0);
            UncompressedBlockEncoding.serialize(output, (UncompressedBlockEncoding) encoding);
        }
        else if (encoding instanceof RunLengthBlockEncoding) {
            output.writeByte(1);
            RunLengthBlockEncoding.serialize(output, (RunLengthBlockEncoding) encoding);
        }
        else if (encoding instanceof DictionaryBlockEncoding) {
            output.writeByte(2);
            DictionaryBlockEncoding.serialize(output, (DictionaryBlockEncoding) encoding);
        }
        else if (encoding instanceof SnappyBlockEncoding) {
            output.writeByte(3);
            SnappyBlockEncoding.serialize(output, (SnappyBlockEncoding) encoding);
        }
        else {
            throw new IllegalArgumentException("unknown encoding " + encoding);
        }
    }
}
