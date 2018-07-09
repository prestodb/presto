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
package com.facebook.presto.elasticsearch.metadata;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * SQL-related information about an index field with date type
 */
public class DateEsField
        extends EsField
{
    public static final List<String> DEFAULT_FORMAT = Arrays.asList("strict_date_optional_time", "epoch_millis");
    private final List<String> formats;

    public DateEsField(String name, Map<String, EsField> properties, boolean hasDocValues, String... formats)
    {
        super(name, DataType.DATE, properties, hasDocValues);
        this.formats = (formats == null || formats.length == 0) ? DEFAULT_FORMAT : Arrays.asList(formats);
    }

    @Override
    public int getPrecision()
    {
        // same as Long
        // TODO: based this on format string
        return 19;
    }

    public List<String> getFormats()
    {
        return formats;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        DateEsField dateField = (DateEsField) o;
        return Objects.equals(formats, dateField.formats);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), formats);
    }
}
