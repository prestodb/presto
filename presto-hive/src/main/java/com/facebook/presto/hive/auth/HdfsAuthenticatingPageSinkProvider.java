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

package com.facebook.presto.hive.auth;

import com.facebook.presto.hive.HivePageSinkProvider;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.google.inject.Inject;

public class HdfsAuthenticatingPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final HadoopKerberosImpersonatingAuthentication authentication;
    private final HivePageSinkProvider targetConnectorPageSinkProvider;

    @Inject
    public HdfsAuthenticatingPageSinkProvider(HadoopKerberosImpersonatingAuthentication authentication, HivePageSinkProvider targetConnectorPageSinkProvider)
    {
        this.authentication = authentication;
        this.targetConnectorPageSinkProvider = targetConnectorPageSinkProvider;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        return authentication.doAs(session.getUser(), () -> {
                    ConnectorPageSink targetPageSink = targetConnectorPageSinkProvider.createPageSink(session, outputTableHandle);
                    return new HdfsAuthenticatingPageSink(session, authentication, targetPageSink);
                }
        );
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        return authentication.doAs(session.getUser(), () -> {
                    ConnectorPageSink targetPageSink = targetConnectorPageSinkProvider.createPageSink(session, insertTableHandle);
                    return new HdfsAuthenticatingPageSink(session, authentication, targetPageSink);
                }
        );
    }
}
