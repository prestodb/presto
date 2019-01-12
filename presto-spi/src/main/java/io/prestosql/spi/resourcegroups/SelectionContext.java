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
package io.prestosql.spi.resourcegroups;

import static java.util.Objects.requireNonNull;

public final class SelectionContext<T>
{
    private final ResourceGroupId resourceGroupId;
    private final T context;

    public SelectionContext(ResourceGroupId resourceGroupId, T context)
    {
        this.resourceGroupId = requireNonNull(resourceGroupId, "resourceGroupId is null");
        this.context = requireNonNull(context, "context is null");
    }

    public ResourceGroupId getResourceGroupId()
    {
        return resourceGroupId;
    }

    public T getContext()
    {
        return context;
    }
}
