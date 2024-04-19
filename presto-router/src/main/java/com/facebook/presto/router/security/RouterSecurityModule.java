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
package com.facebook.presto.router.security;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.airlift.http.server.TheServlet;
import com.facebook.presto.server.InternalCommunicationModule;
import com.facebook.presto.server.security.AuthenticationFilter;
import com.facebook.presto.server.security.ServerSecurityModule;
import com.google.inject.Binder;
import com.google.inject.Scopes;

import javax.servlet.Filter;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class RouterSecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new InternalCommunicationModule());
        install(new ServerSecurityModule());
        newSetBinder(binder, Filter.class, TheServlet.class).addBinding()
                .to(AuthenticationFilter.class).in(Scopes.SINGLETON);
    }
}
