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
package com.facebook.presto.spi.resourceGroups;

import java.util.Optional;

/**
 * The engine calls the {@link #match(SelectionCriteria)} method whenever a query is submitted,
 * and receives a {@link com.facebook.presto.spi.resourceGroups.SelectionContext} in return which
 * contains a fully-qualified {@link com.facebook.presto.spi.resourceGroups.ResourceGroupId},
 * and a manager-specific data structure of type {@code C}.
 * <p>
 * At a later time, the engine may decide to construct a resource group with that ID. To do so,
 * it will walk the tree to find the right position for the group, and then create it. It also
 * creates any necessary parent groups. Every time the engine creates a group it will
 * immediately call the configure method with the original context, allowing the manager to
 * set the required properties on the group.
 *
 * @param <C> the type of the manager-specific data structure
 */
public interface ResourceGroupConfigurationManager<C>
{
    /**
     * Implementations may retain a reference to the group, and re-configure it asynchronously.
     * This method is called, once, when the group is created.
     */
    void configure(ResourceGroup group, SelectionContext<C> criteria);

    /**
     * This method is called for every query that is submitted, so it should be fast.
     */
    Optional<SelectionContext<C>> match(SelectionCriteria criteria);
}
