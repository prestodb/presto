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
package io.prestosql.plugin.thrift.location;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.airlift.drift.client.address.AddressSelector;
import io.airlift.drift.client.address.SimpleAddressSelector;
import io.airlift.drift.client.address.SimpleAddressSelectorConfig;
import io.airlift.drift.client.guice.AbstractAnnotatedProvider;
import io.airlift.drift.client.guice.AddressSelectorBinder;

import java.lang.annotation.Annotation;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class ExtendedSimpleAddressSelectorBinder
        implements AddressSelectorBinder
{
    public static AddressSelectorBinder extendedSimpleAddressSelector()
    {
        return new ExtendedSimpleAddressSelectorBinder();
    }

    @Override
    public void bind(Binder binder, Annotation annotation, String prefix)
    {
        configBinder(binder).bindConfig(SimpleAddressSelectorConfig.class, annotation, prefix);

        binder.bind(AddressSelector.class)
                .annotatedWith(annotation)
                .toProvider(new ExtendedSimpleAddressSelectorProvider(annotation));
    }

    private static class ExtendedSimpleAddressSelectorProvider
            extends AbstractAnnotatedProvider<AddressSelector<?>>
    {
        public ExtendedSimpleAddressSelectorProvider(Annotation annotation)
        {
            super(annotation);
        }

        @Override
        protected AddressSelector<?> get(Injector injector, Annotation annotation)
        {
            return new ExtendedSimpleAddressSelector(new SimpleAddressSelector(
                    injector.getInstance(Key.get(SimpleAddressSelectorConfig.class, annotation))));
        }
    }
}
