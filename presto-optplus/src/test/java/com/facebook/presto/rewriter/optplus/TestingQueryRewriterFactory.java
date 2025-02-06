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
package com.facebook.presto.rewriter.optplus;

import com.facebook.presto.rewriter.QueryRewriterFactory;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.rewriter.QueryRewriterProvider;
import com.facebook.presto.spi.rewriter.QueryRewriterProviderFactory;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static org.testng.Assert.assertTrue;

public class TestingQueryRewriterFactory
        implements QueryRewriterProviderFactory
{
    @Override
    public String getName()
    {
        return this.getClass().getName();
    }

    @Override
    public QueryRewriterProvider create(List<EventListener> eventListener, Map<String, String> config)
    {
        try {
            QueryRewriterProvider queryRewriterProvider = new QueryRewriterFactory().create(eventListener, config);
            assertTrue(queryRewriterProvider instanceof PassThroughQueryRewriter);
            ((PassThroughQueryRewriter) queryRewriterProvider).setByPassAdminCheck(true);
            return queryRewriterProvider;
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
