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
package io.prestosql.operator.index;

import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.RecordSet;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class IndexSplit
        implements ConnectorSplit
{
    private final RecordSet keyRecordSet;

    public IndexSplit(RecordSet keyRecordSet)
    {
        this.keyRecordSet = requireNonNull(keyRecordSet, "keyRecordSet is null");
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getInfo()
    {
        return null;
    }

    public RecordSet getKeyRecordSet()
    {
        return keyRecordSet;
    }
}
