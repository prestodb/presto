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
package com.facebook.presto.iceberg;

import com.facebook.presto.spi.ConnectorPageSource;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ConnectorPageSourceWithRowPositions
{
    private final ConnectorPageSource delegate;
    private final Optional<Long> startRowPosition;
    private final Optional<Long> endRowPosition;

    public ConnectorPageSourceWithRowPositions(
            ConnectorPageSource delegate,
            Optional<Long> startRowPosition,
            Optional<Long> endRowPosition)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.startRowPosition = requireNonNull(startRowPosition, "startRowPosition is null");
        this.endRowPosition = requireNonNull(endRowPosition, "endRowPosition is null");
    }

    public ConnectorPageSource getDelegate()
    {
        return delegate;
    }

    public Optional<Long> getStartRowPosition()
    {
        return startRowPosition;
    }

    public Optional<Long> getEndRowPosition()
    {
        return endRowPosition;
    }
}
