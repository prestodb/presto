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
package com.facebook.presto.verifier.prestoaction;

import com.facebook.presto.verifier.annotation.ForControl;
import com.facebook.presto.verifier.annotation.ForTest;
import com.google.inject.Binder;
import com.google.inject.Module;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.inject.Scopes.SINGLETON;
import static java.util.Objects.requireNonNull;

public class VerificationPrestoActionModule
        implements Module
{
    private final SqlExceptionClassifier exceptionClassifier;

    public VerificationPrestoActionModule(SqlExceptionClassifier exceptionClassifier)
    {
        this.exceptionClassifier = requireNonNull(exceptionClassifier, "exceptionClassifier is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(PrestoClusterConfig.class, ForControl.class, "control");
        configBinder(binder).bindConfig(PrestoClusterConfig.class, ForTest.class, "test");

        binder.bind(PrestoActionFactory.class).to(RoutingPrestoActionFactory.class).in(SINGLETON);
        binder.bind(SqlExceptionClassifier.class).toInstance(exceptionClassifier);
    }
}
