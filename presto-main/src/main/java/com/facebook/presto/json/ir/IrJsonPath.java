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
package com.facebook.presto.json.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class IrJsonPath
{
    private final boolean lax;
    private final IrPathNode root;

    @JsonCreator
    public IrJsonPath(@JsonProperty("lax") boolean lax, @JsonProperty("root") IrPathNode root)
    {
        this.lax = lax;
        this.root = requireNonNull(root, "root is null");
    }

    @JsonProperty
    public boolean isLax()
    {
        return lax;
    }

    @JsonProperty
    public IrPathNode getRoot()
    {
        return root;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        IrJsonPath other = (IrJsonPath) obj;
        return this.lax == other.lax &&
                Objects.equals(this.root, other.root);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lax, root);
    }
}
