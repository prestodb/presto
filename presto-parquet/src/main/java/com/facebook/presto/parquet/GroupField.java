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
package com.facebook.presto.parquet;

import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class GroupField
        extends Field
{
    private final ImmutableList<Optional<Field>> children;

    public GroupField(Type type, int repetitionLevel, int definitionLevel, boolean required, boolean pushedDownSubfield, ImmutableList<Optional<Field>> children)
    {
        super(type, repetitionLevel, definitionLevel, required, pushedDownSubfield);
        this.children = requireNonNull(children, "children is required");
    }

    public List<Optional<Field>> getChildren()
    {
        return children;
    }
}
