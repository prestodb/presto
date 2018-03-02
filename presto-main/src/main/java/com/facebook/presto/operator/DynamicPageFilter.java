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
package com.facebook.presto.operator;

import com.facebook.presto.operator.project.DictionaryAwarePageFilter;
import com.facebook.presto.operator.project.InputChannels;
import com.facebook.presto.operator.project.PageFilter;
import com.facebook.presto.operator.project.SelectedPositions;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.sql.relational.RowExpression;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class DynamicPageFilter
        implements PageFilter, DynamicFilterCollector.DFCollectorCallback
{
    private final AtomicReference<PageFilter> filter;
    private final DynamicFilterCollector dfCollector;
    private final Optional<String> classNameSuffix;
    private final PageFunctionCompiler pageFunctionCompiler;

    public DynamicPageFilter(Optional<PageFilter> filter, DynamicFilterCollector collector, PageFunctionCompiler compiler, Optional<String> className)
    {
        this.filter = requireNonNull(filter, "filter is null")
                .map(pageFilter -> {
                    if (pageFilter.getInputChannels().size() == 1 && pageFilter.isDeterministic()) {
                        return new AtomicReference(new DictionaryAwarePageFilter(pageFilter));
                    }
                    else {
                        return new AtomicReference(pageFilter);
                    }
                }).get();
        this.dfCollector = collector;
        this.pageFunctionCompiler = compiler;
        this.classNameSuffix = className;
        dfCollector.addListener(this);
        dfCollector.collectDynamicSummaryAsync();
    }

    @Override
    public boolean isDeterministic()
    {
        return filter.get().isDeterministic();
    }

    @Override
    public InputChannels getInputChannels()
    {
        return filter.get().getInputChannels();
    }

    @Override
    public SelectedPositions filter(ConnectorSession session, Page page)
    {
        return filter.get().filter(session, page);
    }

    @Override
    public void onSuccess(RowExpression combinedResult)
    {
        Supplier<PageFilter> filterFunctionSupplier = pageFunctionCompiler.compileFilter(combinedResult, classNameSuffix);
        PageFilter pageFilter = filterFunctionSupplier.get();
        if (pageFilter.getInputChannels().size() == 1 && pageFilter.isDeterministic()) {
            filter.set(new DictionaryAwarePageFilter(pageFilter));
        }
        else {
            filter.set(pageFilter);
        }
    }
}
