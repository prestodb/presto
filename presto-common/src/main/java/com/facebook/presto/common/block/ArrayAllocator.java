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
 * Manages the creation and return of primitive arrays, to be used within an operator to avoid repeated array allocation.
 * Typically this will be used within {@link BlockFlattener}.
 *
 * The arrays which are returned may have a size which exceeds the specified {@code positionCount}, and they are not
 * guaranteed to be filled with the type's default value.
 */
public interface ArrayAllocator
{
    int[] borrowIntArray(int positionCount);

    void returnArray(int[] array);

    byte[] borrowByteArray(int positionCount);

    void returnArray(byte[] array);

    /**
     * @return the number of borrowed arrays which have not been returned
     */
    int getBorrowedArrayCount();

    long getEstimatedSizeInBytes();
}
