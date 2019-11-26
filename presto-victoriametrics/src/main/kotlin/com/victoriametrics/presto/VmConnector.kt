/**
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
package com.victoriametrics.presto

import com.facebook.airlift.log.Logger
import com.facebook.presto.spi.connector.Connector
import com.facebook.presto.spi.connector.ConnectorTransactionHandle
import com.facebook.presto.spi.transaction.IsolationLevel
import com.victoriametrics.presto.model.VmTransactionHandle
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class VmConnector
@Inject constructor(
        private val recordSetProvider: VmRecordSetProvider,
        private val splitManager: VmSplitManager,
        private val metadata: VmMetadata
) : Connector {
    companion object {
        val log = Logger.get(VmConnector::class.java)!!
    }

    override fun beginTransaction(isolationLevel: IsolationLevel, readOnly: Boolean): ConnectorTransactionHandle {
        // require(readOnly) { "Only read-only transactions supported" }
        return VmTransactionHandle()
    }

    override fun getMetadata(transactionHandle: ConnectorTransactionHandle): VmMetadata {
        return metadata
    }

    override fun getSplitManager() = splitManager

    override fun getRecordSetProvider() = recordSetProvider
}
