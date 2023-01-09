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
package com.facebook.presto.pinot.auth.none;

import com.facebook.presto.pinot.auth.PinotAuthenticationProvider;
import com.facebook.presto.spi.ConnectorSession;

import java.util.Optional;

public class PinotEmptyAuthenticationProvider
        implements PinotAuthenticationProvider
{
    private static final PinotEmptyAuthenticationProvider INSTANCE = new PinotEmptyAuthenticationProvider();

    public static PinotEmptyAuthenticationProvider instance()
    {
        return INSTANCE;
    }

    private PinotEmptyAuthenticationProvider() {}

    @Override
    public Optional<String> getAuthenticationToken()
    {
        return Optional.empty();
    }

    @Override
    public Optional<String> getAuthenticationToken(ConnectorSession session)
    {
        return Optional.empty();
    }
}
