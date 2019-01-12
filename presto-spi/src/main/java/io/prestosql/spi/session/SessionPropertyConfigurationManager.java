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

/**
 * This interface is used to provide default session property overrides for
 * sessions, thus providing a way to dynamically configure default session
 * properties according to request's user, source, or other characteristics
 * identified by client tags. The returned properties override the default
 * values and not the final values, thus any user-provided values will override
 * the values returned here.
 */
public interface SessionPropertyConfigurationManager
{
    Map<String, String> getSystemSessionProperties(SessionConfigurationContext context);

    Map<String, Map<String, String>> getCatalogSessionProperties(SessionConfigurationContext context);
}
