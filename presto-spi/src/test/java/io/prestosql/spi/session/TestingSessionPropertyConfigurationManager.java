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
package io.prestosql.spi.session;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TestingSessionPropertyConfigurationManager
        implements SessionPropertyConfigurationManager
{
    private final Map<String, String> systemProperties;
    private final Map<String, Map<String, String>> catalogProperties;

    public TestingSessionPropertyConfigurationManager(Map<String, String> systemProperties, Map<String, Map<String, String>> catalogProperties)
    {
        this.systemProperties = requireNonNull(systemProperties, "systemProperties is null");
        this.catalogProperties = requireNonNull(catalogProperties, "catalogProperties is null");
    }

    @Override
    public Map<String, String> getSystemSessionProperties(SessionConfigurationContext context)
    {
        return systemProperties;
    }

    @Override
    public Map<String, Map<String, String>> getCatalogSessionProperties(SessionConfigurationContext context)
    {
        return catalogProperties;
    }
}
