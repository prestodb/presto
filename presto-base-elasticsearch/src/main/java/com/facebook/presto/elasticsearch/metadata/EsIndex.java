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

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class EsIndex
{
    private final String name;
    private final Map<String, EsField> mapping;

    public EsIndex(String name, Map<String, EsField> mapping)
    {
        this.name = requireNonNull(name, "name is null");
        this.mapping = requireNonNull(mapping, "mapping is null");
    }

    public String name()
    {
        return name;
    }

    public Map<String, EsField> mapping()
    {
        return mapping;
    }

    @Override
    public String toString()
    {
        return name;
    }
}
