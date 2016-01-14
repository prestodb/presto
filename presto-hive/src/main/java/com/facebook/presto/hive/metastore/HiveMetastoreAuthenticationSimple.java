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
package com.facebook.presto.hive.metastore;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class HiveMetastoreAuthenticationSimple
        implements HiveMetastoreAuthentication
{
    public static class Module
            implements com.google.inject.Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(HiveMetastoreAuthentication.class)
                    .to(HiveMetastoreAuthenticationSimple.class)
                    .in(Scopes.SINGLETON);
        }
    }

    @Override
    public TTransport createAuthenticatedTransport(TTransport rawTransport, String hiveMetastoreHost)
            throws TTransportException
    {
        return rawTransport;
    }
}
