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
package com.facebook.presto.analyzer.crux;

import com.facebook.presto.spi.analyzer.QueryPreparer;
import com.facebook.presto.sql.analyzer.AnalyzerType;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;

import static com.facebook.presto.sql.analyzer.AnalyzerType.CRUX;
import static com.google.inject.Scopes.SINGLETON;

public class CruxAnalyzerModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        MapBinder<AnalyzerType, QueryPreparer> queryPreparersByType = MapBinder.newMapBinder(binder, AnalyzerType.class, QueryPreparer.class);
        queryPreparersByType.addBinding(CRUX).to(CruxQueryPreparer.class).in(SINGLETON);
    }
}
