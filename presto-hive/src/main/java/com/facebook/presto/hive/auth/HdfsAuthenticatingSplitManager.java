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

import com.facebook.presto.hive.HiveSplitManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

public class HdfsAuthenticatingSplitManager
        implements ConnectorSplitManager
{
    private final HadoopKerberosImpersonatingAuthentication authentication;
    private final HiveSplitManager targetSplitManager;

    @Inject
    public HdfsAuthenticatingSplitManager(HadoopKerberosImpersonatingAuthentication authentication, HiveSplitManager targetSplitManager)
    {
        this.authentication = authentication;
        this.targetSplitManager = targetSplitManager;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        return authentication.doAs(session.getUser(), () -> targetSplitManager.getSplits(transactionHandle, session, layout));
    }
}
