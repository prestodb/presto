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

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class Approximate
        extends Node
{
    private final String confidence;

    public Approximate(NodeLocation location, String confidence)
    {
        super(Optional.of(location));
        this.confidence = requireNonNull(confidence, "confidence is null");
    }

    public String getConfidence()
    {
        return confidence;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitApproximate(this, context);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Approximate o = (Approximate) obj;
        return Objects.equals(confidence, o.confidence);
    }

    @Override
    public int hashCode()
    {
        return confidence.hashCode();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("confidence", confidence)
                .toString();
    }
}
