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
package com.facebook.presto.testing;

import com.facebook.presto.eventlistener.EventListenerConfig;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.server.GracefulShutdownHandler;
import com.facebook.presto.server.security.PrestoAuthenticatorManager;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.storage.TempStorageManager;
import com.facebook.presto.transaction.TransactionManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

import static java.util.Objects.requireNonNull;

public class TestingPrestoServerModule
        implements Module
{
    private final boolean loadDefaultSystemAccessControl;

    public TestingPrestoServerModule(boolean loadDefaultSystemAccessControl)
    {
        this.loadDefaultSystemAccessControl = loadDefaultSystemAccessControl;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(PrestoAuthenticatorManager.class).in(Scopes.SINGLETON);
        binder.bind(TestingEventListenerManager.class).in(Scopes.SINGLETON);
        binder.bind(TestingTempStorageManager.class).in(Scopes.SINGLETON);
        binder.bind(EventListenerManager.class).to(TestingEventListenerManager.class).in(Scopes.SINGLETON);
        binder.bind(EventListenerConfig.class).in(Scopes.SINGLETON);
        binder.bind(TempStorageManager.class).to(TestingTempStorageManager.class).in(Scopes.SINGLETON);
        binder.bind(AccessControl.class).to(AccessControlManager.class).in(Scopes.SINGLETON);
        binder.bind(GracefulShutdownHandler.class).in(Scopes.SINGLETON);
        binder.bind(ProcedureTester.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public AccessControlManager createAccessControlManager(TransactionManager transactionManager)
    {
        requireNonNull(transactionManager, "transactionManager is null");
        return new TestingAccessControlManager(transactionManager, loadDefaultSystemAccessControl);
    }
}
