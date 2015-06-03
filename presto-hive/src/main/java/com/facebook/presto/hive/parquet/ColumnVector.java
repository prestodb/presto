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
package com.facebook.presto.hive.parquet;

import parquet.column.page.DataPage;
import parquet.column.values.ValuesReader;

public abstract class ColumnVector
{
    protected int numValues;
    protected DataPage[] pages;
    protected ValuesReader[] readers;
    protected boolean[] isNull;

    public ColumnVector()
    {
    }

    public int size()
    {
        return numValues;
    }

    void setNumberOfValues(int numValues)
    {
        this.numValues = numValues;
    }

    void setPages(DataPage[] pages)
    {
        this.pages = pages;
    }

    void setReaders(ValuesReader[] readers)
    {
        this.readers = readers;
    }

    public boolean[] getIsNull()
    {
        isNull = new boolean[numValues];
        return isNull;
    }
}
