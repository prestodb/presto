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
package com.facebook.presto.verifier.rewrite;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.verifier.annotation.ForControl;
import com.facebook.presto.verifier.annotation.ForTest;
import com.facebook.presto.verifier.framework.VerifierConfig;
import com.google.inject.Binder;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.presto.verifier.rewrite.VerificationQueryRewriterFactory.VERIFICATION_QUERY_REWRITER_FACTORY;

public class VerificationQueryRewriterModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        String queryRewriterFactoryType = buildConfigObject(VerifierConfig.class).getQueryRewriterFactory();

        configBinder(binder).bindConfig(QueryRewriteConfig.class, ForControl.class, "control");
        configBinder(binder).bindConfig(QueryRewriteConfig.class, ForTest.class, "test");

        if (VERIFICATION_QUERY_REWRITER_FACTORY.equals(queryRewriterFactoryType)) {
            binder.bind(QueryRewriterFactory.class).to(VerificationQueryRewriterFactory.class);
        }
    }
}
