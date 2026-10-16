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
    private final Optional<PrimitiveField> levelSourceField;

    public GroupField(Type type, int repetitionLevel, int definitionLevel, boolean required, ImmutableList<Optional<Field>> children)
    {
        this(type, repetitionLevel, definitionLevel, required, children, Optional.empty());
    }

    public GroupField(Type type, int repetitionLevel, int definitionLevel, boolean required, ImmutableList<Optional<Field>> children, Optional<PrimitiveField> levelSourceField)
    {
        super(type, repetitionLevel, definitionLevel, required);
        this.children = requireNonNull(children, "children is required");
        this.levelSourceField = requireNonNull(levelSourceField, "levelSourceField is required");
    }

    public List<Optional<Field>> getChildren()
    {
        return children;
    }

    /**
     * An existing leaf column under this struct whose definition and repetition levels can be
     * read to determine which struct positions are null and how many elements an enclosing
     * collection contains — used when no projected child column is present in the file.
     */
    public Optional<PrimitiveField> getLevelSourceField()
    {
        return levelSourceField;
    }
}
