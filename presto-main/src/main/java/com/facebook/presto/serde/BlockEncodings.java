/*
 * Copyright 2004-present Facebook. All Rights Reserved.
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
        else {
            throw new IllegalArgumentException("unknown encoding " + encoding);
        }
    }
}
