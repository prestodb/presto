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
package com.facebook.presto.sql.tree;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A reference to an execution engine {@link Input}.
 * <p/>
 * This is used to replace a {@link QualifiedNameReference} with a direct reference to the physical
 * channel and field to avoid unnecessary lookups in a symbol->input map during evaluation
 */
public class InputReference
        extends Expression
{
    private final Input input;

    public InputReference(Input input)
    {
        checkNotNull(input, "input is null");

        this.input = input;
    }

    public Input getInput()
    {
        return input;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitInputReference(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InputReference that = (InputReference) o;

        if (!input.equals(that.input)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return input.hashCode();
    }
}
