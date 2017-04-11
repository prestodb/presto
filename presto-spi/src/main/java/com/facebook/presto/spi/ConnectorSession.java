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
package com.facebook.presto.spi;

import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.TimeZoneKey;

import java.util.Locale;
import java.util.Map;

public interface ConnectorSession
{
    String getQueryId();

    default String getUser()
    {
        return getIdentity().getUser();
    }

    Identity getIdentity();

    TimeZoneKey getTimeZoneKey();

    Locale getLocale();

    long getStartTime();

    <T> T getProperty(String name, Class<T> type);

    Map<String, String> getProperties();
}
