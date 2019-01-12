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
package io.prestosql.verifier;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.event.client.EventClient;

import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.event.client.EventBinder.eventBinder;

public class PrestoVerifierModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(VerifierConfig.class);
        eventBinder(binder).bindEventClient(VerifierQueryEvent.class);

        Multibinder<String> supportedClients = newSetBinder(binder, String.class, SupportedEventClients.class);
        supportedClients.addBinding().toInstance("human-readable");
        supportedClients.addBinding().toInstance("file");
        Set<String> eventClientTypes = buildConfigObject(VerifierConfig.class).getEventClients();
        bindEventClientClasses(eventClientTypes, newSetBinder(binder, EventClient.class));
    }

    private static void bindEventClientClasses(Set<String> eventClientTypes, Multibinder<EventClient> multibinder)
    {
        for (String eventClientType : eventClientTypes) {
            if (eventClientType.equals("human-readable")) {
                multibinder.addBinding().to(HumanReadableEventClient.class).in(Scopes.SINGLETON);
            }
            else if (eventClientType.equals("file")) {
                multibinder.addBinding().to(JsonEventClient.class).in(Scopes.SINGLETON);
            }
        }
    }
}
