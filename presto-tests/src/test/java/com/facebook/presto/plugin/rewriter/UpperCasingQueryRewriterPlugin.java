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

package com.facebook.presto.plugin.rewriter;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.rewriter.QueryRewriter;
import com.facebook.presto.spi.rewriter.QueryRewriterInput;
import com.facebook.presto.spi.rewriter.QueryRewriterOutput;
import com.facebook.presto.spi.rewriter.QueryRewriterProvider;
import com.facebook.presto.spi.rewriter.QueryRewriterProviderFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.IS_QUERY_REWRITER_PLUGIN_SUCCEEDED;

/**
 * Rewrites each query to upper case
 */
public class UpperCasingQueryRewriterPlugin
        implements QueryRewriter, Plugin, QueryRewriterProvider, QueryRewriterProviderFactory
{
    private static final Logger log = Logger.get(UpperCasingQueryRewriterPlugin.class);
    private List<EventListener> eventListener;

    @Override
    public QueryRewriterOutput rewriteSQL(QueryRewriterInput queryRewriterInput)
    {
        log.debug("Upper casing query writer");
        QueryRewriterOutput.Builder outputBuilder = new QueryRewriterOutput.Builder();
        outputBuilder.setQueryId(queryRewriterInput.getQueryId());
        outputBuilder.setOriginalQuery(queryRewriterInput.getQuery());
        outputBuilder.setRewrittenQuery(queryRewriterInput.getQuery().toUpperCase());
        outputBuilder.setSessionProperties(ImmutableMap.of(IS_QUERY_REWRITER_PLUGIN_SUCCEEDED, "true"));
        return outputBuilder.build();
    }

    @Override
    public QueryRewriter getQueryRewriter()
    {
        return this;
    }

    @Override
    public String getName()
    {
        return "UpperCasingQueryRewriterPlugin";
    }

    @Override
    public QueryRewriterProvider create(List<EventListener> eventListener, Map<String, String> config)
    {
        this.eventListener = eventListener;
        return this;
    }

    @Override
    public Iterable<QueryRewriterProviderFactory> getQueryRewriterProviderFactories()
    {
        return ImmutableList.of(this);
    }
}
