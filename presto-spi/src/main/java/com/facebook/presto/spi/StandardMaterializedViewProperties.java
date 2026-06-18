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

import com.facebook.presto.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public final class StandardMaterializedViewProperties
{
    public static final String CROSS_CATALOG_MATERIALIZED_VIEWS_ENABLED = "cross_catalog_materialized_views_enabled";

    private static final List<PropertyMetadata<?>> STANDARD_MV_PROPERTIES = unmodifiableList(asList(
            booleanProperty(
                    CROSS_CATALOG_MATERIALIZED_VIEWS_ENABLED,
                    "Enable cross-catalog materialized views. When true, allows materialized views to reference tables from different catalogs. Default is false.",
                    false,
                    false)));
    private StandardMaterializedViewProperties()
    {
    }
    public static List<PropertyMetadata<?>> getStandardMvProperties()
    {
        return STANDARD_MV_PROPERTIES;
    }

    public static boolean isCrossCatalogEnabled(Map<String, Object> materializedViewProperties)
    {
        if (materializedViewProperties == null) {
            return false;
        }
        return Boolean.TRUE.equals(materializedViewProperties.get(CROSS_CATALOG_MATERIALIZED_VIEWS_ENABLED));
    }
}
