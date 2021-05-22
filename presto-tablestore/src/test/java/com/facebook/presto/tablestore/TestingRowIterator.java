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
package com.facebook.presto.tablestore;

import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.iterator.RowIterator;

public class TestingRowIterator
        implements RowIterator
{
    private final Row[] rows;
    private int index;

    public TestingRowIterator(Row... rows)
    {
        this.rows = rows;
    }

    @Override
    public long getTotalCount()
    {
        return rows.length;
    }

    @Override
    public boolean hasNext()
    {
        return index < rows.length;
    }

    @Override
    public Row next()
    {
        return rows[index++];
    }
}
