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
package com.facebook.presto.parquet.reader;

import org.apache.parquet.column.values.ValuesReader;

public class LevelValuesReader
        implements LevelReader
{
    private final ValuesReader delegate;

    public LevelValuesReader(ValuesReader delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public int readLevel()
    {
        return delegate.readInteger();
    }
}
