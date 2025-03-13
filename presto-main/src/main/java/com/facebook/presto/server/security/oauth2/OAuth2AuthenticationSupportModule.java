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
package com.facebook.presto.server.security.oauth2;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.google.inject.Binder;
import com.google.inject.Scopes;

import static com.facebook.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class OAuth2AuthenticationSupportModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(OAuth2TokenExchange.class).in(Scopes.SINGLETON);
        binder.bind(OAuth2TokenHandler.class).to(OAuth2TokenExchange.class).in(Scopes.SINGLETON);
        jaxrsBinder(binder).bind(OAuth2TokenExchangeResource.class);
        install(new OAuth2ServiceModule());
    }
}
