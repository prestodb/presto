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
package com.facebook.presto.elasticsearch6;

import com.facebook.presto.elasticsearch.ElasticsearchPlugin;
import com.facebook.presto.elasticsearch6.functions.MatchQueryFunction;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class Elasticsearch6Plugin
        extends ElasticsearchPlugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(MatchQueryFunction.class)
                .build();
    }

    public Elasticsearch6Plugin()
    {
        super("elasticsearch6", new Elasticsearch6Module());
    }
}
