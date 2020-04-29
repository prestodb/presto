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
package com.facebook.presto.common.block;

/**
 * Test class which always allocates a new array but keeps track of the number
 * of borrowed arrays.
 */
public class CountingArrayAllocator
        implements ArrayAllocator
{
    private int borrowedIntArrays;
    private int borrowedByteArrays;

    @Override
    public int[] borrowIntArray(int positionCount)
    {
        borrowedIntArrays++;
        return new int[positionCount];
    }

    @Override
    public void returnArray(int[] array)
    {
        borrowedIntArrays--;
    }

    @Override
    public byte[] borrowByteArray(int positionCount)
    {
        borrowedByteArrays++;
        return new byte[positionCount];
    }

    @Override
    public void returnArray(byte[] array)
    {
        borrowedByteArrays--;
    }

    @Override
    public int getBorrowedArrayCount()
    {
        return borrowedIntArrays + borrowedByteArrays;
    }

    @Override
    public long getEstimatedSizeInBytes()
    {
        return 0;
    }
}
