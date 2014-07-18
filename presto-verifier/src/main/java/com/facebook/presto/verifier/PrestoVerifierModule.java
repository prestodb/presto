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
package com.facebook.presto.verifier;

import com.facebook.presto.guice.AbstractConfigurationAwareModule;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.event.client.EventClient;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.event.client.EventBinder.eventBinder;

public class PrestoVerifierModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        bindConfig(binder).to(VerifierConfig.class);
        eventBinder(binder).bindEventClient(VerifierQueryEvent.class);

        String eventClientType = buildConfigObject(VerifierConfig.class).getEventClient();
        Class<? extends EventClient> eventClientClass = getEventClientClass(eventClientType);
        newSetBinder(binder, EventClient.class).addBinding().to(eventClientClass).in(Scopes.SINGLETON);
    }

    private static Class<? extends EventClient> getEventClientClass(String eventClientType)
    {
        switch (eventClientType) {
            case "human-readable":
                return HumanReadableEventClient.class;
            case "file":
                return JsonEventClient.class;
            default:
                throw new IllegalArgumentException(String.format("Unsupported event client: %s", eventClientType));
        }
    }
}
