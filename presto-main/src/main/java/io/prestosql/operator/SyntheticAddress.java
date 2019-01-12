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
package io.prestosql.operator;

/**
 * Methods for creating and decoding synthetic addresses.
 * A synthetic address is a physical position within an array of Slices.  The address is encoded
 * as a long with the high 32 bits containing the index of the slice in the array and the low 32
 * bits containing an offset within the slice.
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

    public static int decodePosition(long sliceAddress)
    {
        // low order bits contain the raw offset, so a simple cast here will suffice
        return (int) sliceAddress;
    }
}
