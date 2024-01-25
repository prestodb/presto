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
package com.facebook.presto.iceberg.function.changelog;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import io.airlift.slice.Slice;
import org.apache.iceberg.ChangelogOperation;

import java.util.Optional;

import static com.facebook.presto.spi.function.SqlFunctionVisibility.HIDDEN;

@AggregationFunction(value = "apply_changelog", visibility = HIDDEN)
public class ApplyChangelogFunction
{
    public static final String NAME = "apply_changelog";

    private ApplyChangelogFunction() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @TypeParameter("T") Type type,
            @AggregationState ApplyChangelogState state,
            @SqlType("bigint") long ordinal,
            @SqlType("varchar") Slice operation,
            @BlockPosition @SqlType("T") Block block,
            @BlockIndex int index)
    {
        ChangelogRecord record = state.getRecord().orElseGet(() -> new ChangelogRecord(type));
        record.add((int) ordinal, operation, block.getSingleValueBlock(index));
        state.setRecord(record);
    }

    @CombineFunction
    public static void combine(@AggregationState ApplyChangelogState state,
            @AggregationState ApplyChangelogState otherState)
    {
        ChangelogRecord record;
        if (!state.getRecord().isPresent() && !otherState.getRecord().isPresent()) {
            return;
        }
        else if (!state.getRecord().isPresent() && otherState.getRecord().isPresent()) {
            record = otherState.getRecord().get();
        }
        else if (state.getRecord().isPresent() && !otherState.getRecord().isPresent()) {
            record = state.getRecord().get();
        }
        else {
            record = state.getRecord().get().merge(otherState.getRecord().get());
        }
        state.setRecord(record);
    }

    @OutputFunction("T")
    public static void output(
            @TypeParameter("T") Type elementType,
            @AggregationState ApplyChangelogState state,
            BlockBuilder out)
    {
        Optional<ChangelogRecord> record = state.getRecord();
        if (!record.isPresent()) {
            out.appendNull();
            return;
        }

        if (ChangelogOperation.valueOf(record.get().getLastOperation().toStringUtf8().toUpperCase()).equals(ChangelogOperation.DELETE)) {
            out.appendNull();
        }
        else {
            elementType.appendTo(record.get().getRow(), 0, out);
        }
    }
}
