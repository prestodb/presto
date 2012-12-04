/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

/**
 * Methods for creating and decoding synthetic addresses.
 * A synthetic address is a physical position within an array of Slices.  The address is encoded
 * as a long with the high 32 bits containing the index of the slice in the array and the low 32
 * bits containing an offset with the slice.
 */
public final class SyntheticAddress
{
    private SyntheticAddress()
    {
    }

    public static long encodeSyntheticAddress(int sliceIndex, int sliceOffset)
    {
        return (((long) sliceIndex) << 32) | sliceOffset;
    }

    public static int decodeSliceIndex(long sliceAddress)
    {
        return ((int) (sliceAddress >> 32));
    }

    public static int decodeSliceOffset(long sliceAddress)
    {
        // low order bits contain the raw offset, so a simple cast here will suffice
        return (int) sliceAddress;
    }
}
