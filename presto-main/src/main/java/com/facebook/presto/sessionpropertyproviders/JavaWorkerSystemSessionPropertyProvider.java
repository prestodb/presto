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
package com.facebook.presto.sessionpropertyproviders;

import com.facebook.presto.Session;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.session.SessionPropertyMetadata;
import com.facebook.presto.spi.session.SystemSessionPropertyProvider;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.isSpillEnabled;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;

public class JavaWorkerSystemSessionPropertyProvider
        implements SystemSessionPropertyProvider
{
    public static final String TOPN_SPILL_ENABLED = "topn_spill_enabled";
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public JavaWorkerSystemSessionPropertyProvider(FeaturesConfig featuresConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        TOPN_SPILL_ENABLED,
                        "Enable topN spilling if spill_enabled",
                        featuresConfig.isTopNSpillEnabled(),
                        false));
    }
    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isTopNSpillEnabled(Session session)
    {
        return session.getSystemProperty(TOPN_SPILL_ENABLED, Boolean.class) && isSpillEnabled(session);
    }
}
