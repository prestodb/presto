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
package com.facebook.presto.verifier.event;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.airlift.event.client.EventClient;
import com.facebook.presto.verifier.framework.VerifierConfig;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets.SetView;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.difference;
import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class EventClientModule
        extends AbstractConfigurationAwareModule
{
    private static final Map<String, Class<? extends EventClient>> DEFAULT_EVENT_CLIENTS = ImmutableMap.<String, Class<? extends EventClient>>builder()
            .put("json", JsonEventClient.class)
            .put("human-readable", HumanReadableEventClient.class)
            .build();

    private final Set<String> supportedEventClientTypes;

    public EventClientModule(Set<String> customEventClientType)
    {
        this.supportedEventClientTypes = ImmutableSet.<String>builder()
                .addAll(DEFAULT_EVENT_CLIENTS.keySet())
                .addAll(customEventClientType)
                .build();
    }

    @Override
    protected void setup(Binder binder)
    {
        Set<String> eventClientTypes = buildConfigObject(VerifierConfig.class).getEventClients();
        SetView<String> unsupportedEventClients = difference(eventClientTypes, supportedEventClientTypes);
        checkArgument(unsupportedEventClients.isEmpty(), "Unsupported EventClient: %s", unsupportedEventClients);

        Multibinder<EventClient> eventClientBinder = newSetBinder(binder, EventClient.class);
        for (Map.Entry<String, Class<? extends EventClient>> entry : DEFAULT_EVENT_CLIENTS.entrySet()) {
            if (eventClientTypes.contains(entry.getKey())) {
                eventClientBinder.addBinding().to(entry.getValue()).in(SINGLETON);
            }
        }
    }
}
