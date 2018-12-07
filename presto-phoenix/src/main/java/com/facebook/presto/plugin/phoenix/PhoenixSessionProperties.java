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
package com.facebook.presto.plugin.phoenix;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Class contains all session-based properties for the Phoenix connector.
 * Use SHOW SESSION to view all available properties in the Presto CLI.
 * <p>
 * Can set the property using:
 * <p>
 * SET SESSION &lt;property&gt; = &lt;value&gt;;
 */
public final class PhoenixSessionProperties
{
    private static final String DUPLICATE_KEY_UPDATE_COLUMNS = "duplicate_key_update_columns";
    private static final Splitter DUPLICATE_KEY_UPDATE_COLUMNS_SPLITTER = Splitter.on(" and ").trimResults();

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public PhoenixSessionProperties()
    {
        sessionProperties = ImmutableList.of(
                stringProperty(
                        DUPLICATE_KEY_UPDATE_COLUMNS,
                        "A comma-delimited list of Presto columns that the row will be updated.",
                        null,
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static List<String> getDuplicateKeyUpdateColumns(ConnectorSession session)
    {
        requireNonNull(session);

        String value = session.getProperty(DUPLICATE_KEY_UPDATE_COLUMNS, String.class);
        if (value == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(DUPLICATE_KEY_UPDATE_COLUMNS_SPLITTER.split(value.toLowerCase(ENGLISH)));
    }
}
