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
package com.facebook.presto.split;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.InsertTableHandle;
import com.facebook.presto.metadata.OutputTableHandle;
import com.facebook.presto.spi.ConnectorMergeSink;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.MergeHandle;
import com.facebook.presto.spi.PageSinkContext;

public interface PageSinkProvider
{
    ConnectorPageSink createPageSink(Session session, OutputTableHandle tableHandle, PageSinkContext pageSinkContext);

    ConnectorPageSink createPageSink(Session session, InsertTableHandle tableHandle, PageSinkContext pageSinkContext);

    /*
     * Used to write the result of SQL MERGE to an existing table
     */
    ConnectorMergeSink createMergeSink(Session session, MergeHandle mergeHandle);
}
