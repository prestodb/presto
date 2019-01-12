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
package io.prestosql.plugin.password;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.security.PasswordAuthenticatorFactory;

public class PasswordAuthenticatorPlugin
        implements Plugin
{
    @Override
    public Iterable<PasswordAuthenticatorFactory> getPasswordAuthenticatorFactories()
    {
        return ImmutableList.<PasswordAuthenticatorFactory>builder()
                .add(new LdapAuthenticatorFactory())
                .build();
    }
}
