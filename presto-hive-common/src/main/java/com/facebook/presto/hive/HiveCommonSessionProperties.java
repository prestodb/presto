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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;

public class HiveCommonSessionProperties
{
    public static final String RANGE_FILTERS_ON_SUBSCRIPTS_ENABLED = "range_filters_on_subscripts_enabled";
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public HiveCommonSessionProperties(HiveCommonClientConfig hiveClientConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        RANGE_FILTERS_ON_SUBSCRIPTS_ENABLED,
                        "Experimental: enable pushdown of range filters on subscripts (a[2] = 5) into ORC column readers",
                        hiveClientConfig.isRangeFiltersOnSubscriptsEnabled(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isRangeFiltersOnSubscriptsEnabled(ConnectorSession session)
    {
        return session.getProperty(RANGE_FILTERS_ON_SUBSCRIPTS_ENABLED, Boolean.class);
    }
}
