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
import com.facebook.drift.client.address.SimpleAddressSelector;
import com.facebook.drift.client.address.SimpleAddressSelector.SimpleAddress;
import com.facebook.drift.client.address.SimpleAddressSelectorConfig;
import com.facebook.drift.client.guice.AbstractAnnotatedProvider;
import com.facebook.drift.client.guice.AddressSelectorBinder;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;

import java.lang.annotation.Annotation;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class ContextualSimpleAddressSelectorBinder
        implements AddressSelectorBinder
{
    private final Map<String, SimpleAddressSelectorConfig> addresses;

    public static AddressSelectorBinder contextualSimpleAddressSelector(Map<String, SimpleAddressSelectorConfig> addresses)
    {
        return new ContextualSimpleAddressSelectorBinder(addresses);
    }

    private ContextualSimpleAddressSelectorBinder(Map<String, SimpleAddressSelectorConfig> addresses)
    {
        this.addresses = ImmutableMap.copyOf(requireNonNull(addresses, "addresses is null"));
    }

    @Override
    public void bind(Binder binder, Annotation annotation, String prefix)
    {
        binder.bind(AddressSelector.class)
                .annotatedWith(annotation)
                .toProvider(new SimpleAddressSelectorProvider(annotation, addresses));
    }

    private static class SimpleAddressSelectorProvider
            extends AbstractAnnotatedProvider<AddressSelector<SimpleAddress>>
    {
        private final Map<String, SimpleAddressSelectorConfig> addresses;
        protected SimpleAddressSelectorProvider(Annotation annotation, Map<String, SimpleAddressSelectorConfig> addresses)
        {
            super(annotation);
            this.addresses = addresses;
        }

        @Override
        protected AddressSelector<SimpleAddress> get(Injector injector, Annotation annotation)
        {
            return new ContextualAddressSelector(addresses.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> new SimpleAddressSelector(entry.getValue()))));
        }
    }
}
