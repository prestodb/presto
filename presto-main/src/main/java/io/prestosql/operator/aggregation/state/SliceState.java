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
package io.prestosql.operator.aggregation.state;

import io.airlift.slice.Slice;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorState;
import io.prestosql.spi.type.Type;

public interface SliceState
        extends AccumulatorState
{
    Slice getSlice();

    void setSlice(Slice value);

    static void write(Type type, SliceState state, BlockBuilder out)
    {
        if (state.getSlice() == null) {
            out.appendNull();
        }
        else {
            type.writeSlice(out, state.getSlice());
        }
    }
}
