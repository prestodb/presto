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
package com.facebook.presto.rewriter;

import com.facebook.presto.rewriter.optplus.TestingQueryRewriterFactory;
import com.facebook.presto.rewriter.password.PluginPasswordAuthenticatorFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.rewriter.QueryRewriterProviderFactory;
import com.facebook.presto.spi.security.PasswordAuthenticatorFactory;
import com.google.common.collect.ImmutableList;

public class TestingRewriterPlugin
        implements Plugin
{
    @Override
    public Iterable<PasswordAuthenticatorFactory> getPasswordAuthenticatorFactories()
    {
        return ImmutableList.of(new PluginPasswordAuthenticatorFactory());
    }

    @Override
    public Iterable<QueryRewriterProviderFactory> getQueryRewriterProviderFactories()
    {
        return ImmutableList.of(new TestingQueryRewriterFactory());
    }
}
