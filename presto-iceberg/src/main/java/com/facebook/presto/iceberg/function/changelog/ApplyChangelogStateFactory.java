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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.TypeParameter;

import static java.util.Objects.requireNonNull;

public class ApplyChangelogStateFactory
        implements AccumulatorStateFactory<ApplyChangelogState>
{
    private final Type type;

    public ApplyChangelogStateFactory(@TypeParameter("T") Type type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public ApplyChangelogState createSingleState()
    {
        return new ApplyChangelogState.Single(type);
    }

    @Override
    public Class<? extends ApplyChangelogState> getSingleStateClass()
    {
        return ApplyChangelogState.Single.class;
    }

    @Override
    public ApplyChangelogState.Grouped createGroupedState()
    {
        return new ApplyChangelogState.Grouped(type);
    }

    @Override
    public Class<? extends ApplyChangelogState> getGroupedStateClass()
    {
        return ApplyChangelogState.Grouped.class;
    }
}
