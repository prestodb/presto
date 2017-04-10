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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.FieldReference;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class InputReferenceExtractor
        extends DefaultExpressionTraversalVisitor<Void, Void>
{
    private final ImmutableSet.Builder<Integer> inputChannels = ImmutableSet.builder();

    @Override
    protected Void visitFieldReference(FieldReference node, Void context)
    {
        inputChannels.add(node.getFieldIndex());
        return null;
    }

    public Set<Integer> getInputChannels()
    {
        return inputChannels.build();
    }
}
