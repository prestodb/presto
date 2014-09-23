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
package com.facebook.presto.testing;

import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTime;
import com.facebook.presto.spi.type.SqlTimeWithTimeZone;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class MaterializedResult
        implements Iterable<MaterializedRow>
{
    public static final int DEFAULT_PRECISION = 5;

    private final List<MaterializedRow> rows;
    private final List<Type> types;

    public MaterializedResult(List<MaterializedRow> rows, List<? extends Type> types)
    {
        this.rows = ImmutableList.copyOf(checkNotNull(rows, "rows is null"));
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
    }

    public int getRowCount()
    {
        return rows.size();
    }

    @Override
    public Iterator<MaterializedRow> iterator()
    {
        return rows.iterator();
    }

    public List<MaterializedRow> getMaterializedRows()
    {
        return rows;
    }

    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        MaterializedResult o = (MaterializedResult) obj;
        return Objects.equal(types, o.types) &&
                Objects.equal(rows, o.rows);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(rows, types);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("rows", rows)
                .add("types", types)
                .toString();
    }

    public MaterializedResult toJdbcTypes()
    {
        ImmutableList.Builder<MaterializedRow> jdbcRows = ImmutableList.builder();
        for (MaterializedRow row : rows) {
            jdbcRows.add(convertToJdbcTypes(row));
        }
        return new MaterializedResult(jdbcRows.build(), types);
    }

    private static MaterializedRow convertToJdbcTypes(MaterializedRow prestoRow)
    {
        List<Object> jdbcValues = new ArrayList<>();
        for (int field = 0; field < prestoRow.getFieldCount(); field++) {
            Object prestoValue = prestoRow.getField(field);
            Object jdbcValue;
            if (prestoValue instanceof SqlDate) {
                jdbcValue = new Date(((SqlDate) prestoValue).getMillisAtMidnight());
            }
            else if (prestoValue instanceof SqlTime) {
                jdbcValue = new Time(((SqlTime) prestoValue).getMillisUtc());
            }
            else if (prestoValue instanceof SqlTimeWithTimeZone) {
                jdbcValue = new Time(((SqlTimeWithTimeZone) prestoValue).getMillisUtc());
            }
            else if (prestoValue instanceof SqlTimestamp) {
                jdbcValue = new Timestamp(((SqlTimestamp) prestoValue).getMillisUtc());
            }
            else if (prestoValue instanceof SqlTimestampWithTimeZone) {
                jdbcValue = new Timestamp(((SqlTimestampWithTimeZone) prestoValue).getMillisUtc());
            }
            else {
                jdbcValue = prestoValue;
            }
            jdbcValues.add(jdbcValue);
        }
        return new MaterializedRow(prestoRow.getPrecision(), jdbcValues);
    }

    public static MaterializedResult materializeSourceDataStream(ConnectorSession session, Operator operator)
    {
        MaterializedResult.Builder builder = resultBuilder(session, operator.getTypes());
        while (!operator.isFinished()) {
            checkArgument(!operator.needsInput(), "Source data stream should never require input");

            try {
                operator.isBlocked().get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            catch (ExecutionException e) {
                throw Throwables.propagate(e);
            }

            Page outputPage = operator.getOutput();
            if (outputPage == null) {
                break;
            }
            builder.page(outputPage);
        }
        return builder.build();
    }

    public static Builder resultBuilder(ConnectorSession session, Type... types)
    {
        return resultBuilder(session, ImmutableList.copyOf(types));
    }

    public static Builder resultBuilder(ConnectorSession session, Iterable<? extends Type> types)
    {
        return new Builder(session, ImmutableList.copyOf(types));
    }

    public static class Builder
    {
        private final ConnectorSession session;
        private final List<Type> types;
        private final ImmutableList.Builder<MaterializedRow> rows = ImmutableList.builder();

        Builder(ConnectorSession session, List<Type> types)
        {
            this.session = session;
            this.types = ImmutableList.copyOf(types);
        }

        public Builder row(Object... values)
        {
            rows.add(new MaterializedRow(DEFAULT_PRECISION, values));
            return this;
        }

        public Builder pages(Iterable<Page> pages)
        {
            for (Page page : pages) {
                this.page(page);
            }

            return this;
        }

        public Builder page(Page page)
        {
            checkNotNull(page, "page is null");
            checkArgument(page.getChannelCount() == types.size(), "Expected a page with %s columns, but got %s columns", types.size(), page.getChannelCount());

            for (int position = 0; position < page.getPositionCount(); position++) {
                List<Object> values = new ArrayList<>(page.getChannelCount());
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    Type type = types.get(channel);
                    Block block = page.getBlock(channel);
                    values.add(type.getObjectValue(session, block, position));
                }
                values = Collections.unmodifiableList(values);

                rows.add(new MaterializedRow(DEFAULT_PRECISION, values));
            }
            return this;
        }

        public MaterializedResult build()
        {
            return new MaterializedResult(rows.build(), types);
        }
    }
}
