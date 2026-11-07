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
package com.facebook.presto.ducklake.catalog;

import java.io.Closeable;

/**
 * Streams the rows of one inlined table (see {@link DuckLakeInlinedTable}), opened by {@link
 * DuckLakeCatalog#openInlinedRows}. Column indexes passed to {@link #getObject(int)} refer to the
 * position within the {@code columnNames} list given to {@code openInlinedRows}, not to the
 * physical inlined table's own column order. Consumed only by the page source implemented in
 * Task 4.4; no implementation is provided here.
 */
public interface DuckLakeInlinedRowSource
        extends Closeable
{
    /**
     * Advances to the next row, if any. Must be called once before the first call to {@link
     * #getObject(int)} or {@link #getRowId()}.
     *
     * @return {@code true} if a row is now available, {@code false} if the source is exhausted
     */
    boolean advanceNextRow();

    /**
     * Returns the value of the given column, by position in the {@code columnNames} list passed
     * to {@link DuckLakeCatalog#openInlinedRows}, for the current row.
     */
    Object getObject(int columnIndex);

    /**
     * Returns the row id of the current row.
     */
    long getRowId();
}
