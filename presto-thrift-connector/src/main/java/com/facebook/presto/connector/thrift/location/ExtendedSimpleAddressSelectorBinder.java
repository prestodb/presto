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
package com.facebook.presto.connector.thrift.location;

import com.facebook.drift.client.address.AddressSelector;
import com.facebook.drift.client.address.SimpleAddressSelector;
import com.facebook.drift.client.address.SimpleAddressSelectorConfig;
import com.facebook.drift.client.guice.AddressSelectorBinder;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import jakarta.inject.Inject;

import javax.inject.Provider;

import java.lang.annotation.Annotation;
import java.util.Objects;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

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

    private abstract static class AbstractAnnotatedProvider<T>
            implements Provider<T>
    {
        private final Annotation annotation;
        private Injector injector;

        protected AbstractAnnotatedProvider(Annotation annotation)
        {
            this.annotation = requireNonNull(annotation, "annotation is null");
        }

        @Inject
        public final void setInjector(Injector injector)
        {
            this.injector = injector;
        }

        @Override
        public final T get()
        {
            checkState(injector != null, "injector was not set");
            return get(injector, annotation);
        }

        protected abstract T get(Injector injector, Annotation annotation);

        @Override
        public final boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            AbstractAnnotatedProvider<?> other = (AbstractAnnotatedProvider<?>) obj;
            return Objects.equals(this.annotation, other.annotation);
        }

        @Override
        public final int hashCode()
        {
            return Objects.hash(annotation);
        }
    }

    private static class ExtendedSimpleAddressSelectorProvider
            extends ExtendedSimpleAddressSelectorBinder.AbstractAnnotatedProvider<AddressSelector<?>>
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
