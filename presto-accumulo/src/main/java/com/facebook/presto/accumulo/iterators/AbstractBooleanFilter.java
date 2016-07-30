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
package com.facebook.presto.accumulo.iterators;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RowFilter;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

public abstract class AbstractBooleanFilter
        extends RowFilter
{
    private static final String FILTER_JAVA_CLASS_NAME = "abstract.boolean.filter.java.class.name";
    private static final Logger LOG = Logger.get(AbstractBooleanFilter.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected final List<RowFilter> filters = new ArrayList<>();

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException
    {
        super.init(source, options, env);
        for (Entry<String, String> entry : options.entrySet()) {
            try {
                Map<String, String> props = OBJECT_MAPPER.readValue(entry.getValue(), new TypeReference<Map<String, String>>() {});
                String clazz = props.remove(FILTER_JAVA_CLASS_NAME);
                RowFilter filter = (RowFilter) Class.forName(clazz).newInstance();
                filter.init(this, props, env);
                filters.add(filter);
                LOG.debug("%s: Added Filter %s", super.toString(), filter);
            }
            catch (Exception ex) {
                throw new IllegalArgumentException("Failed to deserialize Filter information from JSON value " + entry.getValue(), ex);
            }
        }
    }

    protected static IteratorSetting combineFilters(Class<? extends AbstractBooleanFilter> clazz, int priority, IteratorSetting... configs)
    {
        if (configs == null || configs.length == 0) {
            throw new IllegalArgumentException("Iterator configs are null or empty");
        }

        Map<String, String> props = new HashMap<>();
        for (IteratorSetting setting : configs) {
            if (props.containsKey(setting.getName())) {
                throw new IllegalArgumentException("Destination config already has config for filter called " + setting.getName());
            }

            Map<String, String> propCopy = new HashMap<>(setting.getOptions());
            propCopy.put(FILTER_JAVA_CLASS_NAME, setting.getIteratorClass());

            try {
                props.put(setting.getName(), OBJECT_MAPPER.writeValueAsString(propCopy));
            }
            catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Failed to encode map as json string", e);
            }
        }

        return new IteratorSetting(priority, UUID.randomUUID().toString(), clazz, props);
    }

    @Override
    public String toString()
    {
        return StringUtils.join(filters, ',');
    }
}
