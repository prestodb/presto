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
package com.facebook.presto.functionNamespace.execution.thrift;

import com.facebook.drift.client.address.AddressSelector;
import com.facebook.drift.transport.client.Address;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ContextualAddressSelector<T extends Address>
        implements AddressSelector<T>
{
    private final Map<String, AddressSelector<T>> delegates;

    public ContextualAddressSelector(Map<String, AddressSelector<T>> delegates)
    {
        this.delegates = requireNonNull(delegates, "delegates is null");
    }

    @Override
    public Optional<T> selectAddress(Optional<String> context)
    {
        return selectAddress(context, ImmutableSet.of());
    }

    @Override
    public Optional<T> selectAddress(Optional<String> context, Set<T> attempted)
    {
        checkArgument(context.isPresent(), "context is empty");
        return delegates.get(context.get()).selectAddress(Optional.empty(), attempted);
    }
}
