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

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.RichColumnDescriptor;
import org.joda.time.DateTimeZone;

import java.util.Optional;

public abstract class AbstractTimestampColumnReader
        extends AbstractColumnReader
{
    protected Optional<DateTimeZone> timezone = Optional.empty();

    protected AbstractTimestampColumnReader(RichColumnDescriptor columnDescriptor)
    {
        super(columnDescriptor);
    }

    @Override
    public ColumnChunk readNext(Optional<DateTimeZone> timezone)
    {
        this.timezone = timezone;
        return super.readNext(timezone);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type type)
    {
        readValue(blockBuilder, type, timezone);
    }

    protected abstract void readValue(BlockBuilder blockBuilder, Type type, Optional<DateTimeZone> timezone);
}
