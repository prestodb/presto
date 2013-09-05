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
package com.facebook.presto;

import com.facebook.presto.operator.Page;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Objects;

import java.util.List;

public final class UnpartitionedPagePartitionFunction
        implements PagePartitionFunction
{
    @JsonCreator
    public UnpartitionedPagePartitionFunction()
    {
    }

    @Override
    public List<Page> partition(List<Page> pages)
    {
        return pages;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(getClass());
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
        final UnpartitionedPagePartitionFunction other = (UnpartitionedPagePartitionFunction) obj;
        return Objects.equal(this.getClass(), other.getClass());
    }

    @Override
    public String toString()
    {
        return "unpartitioned";
    }
}
