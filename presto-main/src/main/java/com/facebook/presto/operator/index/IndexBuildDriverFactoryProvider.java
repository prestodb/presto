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
package com.facebook.presto.operator.index;

import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.operator.index.PageBufferOperator.PageBufferOperatorFactory;
import static com.facebook.presto.operator.index.PagesIndexBuilderOperator.PagesIndexBuilderOperatorFactory;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class IndexBuildDriverFactoryProvider
{
    private final int outputOperatorId;
    private final boolean inputDriver;
    private final List<OperatorFactory> coreOperatorFactories;
    private final List<Type> outputTypes;

    public IndexBuildDriverFactoryProvider(int outputOperatorId, boolean inputDriver, List<OperatorFactory> coreOperatorFactories)
    {
        this.outputOperatorId = outputOperatorId;
        this.inputDriver = inputDriver;
        checkNotNull(coreOperatorFactories, "coreOperatorFactories is null");
        checkArgument(!coreOperatorFactories.isEmpty(), "coreOperatorFactories is empty");
        this.coreOperatorFactories = ImmutableList.copyOf(coreOperatorFactories);
        this.outputTypes = ImmutableList.copyOf(this.coreOperatorFactories.get(this.coreOperatorFactories.size() - 1).getTypes());
    }

    public List<Type> getOutputTypes()
    {
        return outputTypes;
    }

    public DriverFactory create(IndexSnapshotBuilder indexSnapshotBuilder)
    {
        checkArgument(indexSnapshotBuilder.getOutputTypes().equals(outputTypes));
        return new DriverFactory(inputDriver, false, ImmutableList.<OperatorFactory>builder()
                .addAll(coreOperatorFactories)
                .add(new PagesIndexBuilderOperatorFactory(outputOperatorId, indexSnapshotBuilder))
                .build());
    }

    public DriverFactory create(PageBuffer pageBuffer)
    {
        return new DriverFactory(inputDriver, false, ImmutableList.<OperatorFactory>builder()
                .addAll(coreOperatorFactories)
                .add(new PageBufferOperatorFactory(outputOperatorId, pageBuffer))
                .build());
    }
}
