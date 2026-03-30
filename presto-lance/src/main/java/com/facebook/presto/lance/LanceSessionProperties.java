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
package com.facebook.presto.lance;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;

public final class LanceSessionProperties
{
    public static final String FILTER_PUSHDOWN_ENABLED = "filter_pushdown_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public LanceSessionProperties()
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(booleanProperty(
                        FILTER_PUSHDOWN_ENABLED,
                        "Enable SQL predicate pushdown to Lance DataFusion scanner",
                        true,
                        false))
                .build();
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isFilterPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(FILTER_PUSHDOWN_ENABLED, Boolean.class);
    }
}
