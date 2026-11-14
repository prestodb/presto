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
package com.facebook.presto.lance;

import org.lance.MetadataColumns;

/**
 * Utility for working with Lance row addresses.
 * <p>
 * A Lance row address is a 64-bit value that uniquely identifies a row within a
 * dataset version. It is surfaced by the scanner as the {@code _rowaddr} metadata
 * column when {@code ScanOptions.withRowAddress(true)} is set. The layout is:
 * <ul>
 *     <li>high 32 bits: fragment id</li>
 *     <li>low 32 bits: row index within the fragment</li>
 * </ul>
 * Row-level DELETE/UPDATE decode this address to determine which rows of which
 * fragments to remove via Lance deletion vectors.
 */
public final class RowAddress
{
    public static final String ROW_ADDRESS_COLUMN_NAME = MetadataColumns.ROWADDR;

    private RowAddress() {}

    public static long encode(int fragmentId, int rowIndex)
    {
        return ((long) fragmentId << 32) | (rowIndex & 0xFFFFFFFFL);
    }

    public static int fragmentId(long rowAddress)
    {
        return (int) (rowAddress >>> 32);
    }

    public static int rowIndex(long rowAddress)
    {
        return (int) (rowAddress & 0xFFFFFFFFL);
    }
}
