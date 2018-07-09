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
package com.facebook.presto.elasticsearch5;

import com.facebook.presto.elasticsearch.ElasticsearchPlugin;
import com.facebook.presto.elasticsearch5.functions.MatchQueryFunction;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class Elasticsearch5Plugin
        extends ElasticsearchPlugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(MatchQueryFunction.class)
                .build();
    }

    public Elasticsearch5Plugin()
    {
        super("elasticsearch5", new Elasticsearch5Module());
    }
}
