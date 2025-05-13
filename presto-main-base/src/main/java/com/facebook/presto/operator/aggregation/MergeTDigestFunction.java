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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.state.StatisticalDigestState;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.type.StandardTypes.TDIGEST;
import static com.facebook.presto.operator.aggregation.StatisticalDigestFactory.createStatisticalTDigest;
import static com.facebook.presto.operator.aggregation.state.StatisticalDigestStateFactory.createTDigestFactory;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.EXPERIMENTAL;
import static com.facebook.presto.tdigest.TDigest.createTDigest;
import static com.facebook.presto.util.Reflection.methodHandle;

public class MergeTDigestFunction
        extends MergeStatisticalDigestFunction
{
    public static final MergeTDigestFunction MERGE = new MergeTDigestFunction();
    public static final String NAME = "merge";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(MergeTDigestFunction.class, "input", Type.class, StatisticalDigestState.class, Block.class, int.class);

    private MergeTDigestFunction()
    {
        super(NAME, TDIGEST, createTDigestFactory(), EXPERIMENTAL);
    }

    @Override
    public String getDescription()
    {
        return "Merges the input t-digests into a single t-digest";
    }

    public static void input(Type type, StatisticalDigestState state, Block value, int index)
    {
        merge(state, createStatisticalTDigest(createTDigest(type.getSlice(value, index))));
    }

    @Override
    protected MethodHandle getInputFunction()
    {
        return INPUT_FUNCTION;
    }
}
