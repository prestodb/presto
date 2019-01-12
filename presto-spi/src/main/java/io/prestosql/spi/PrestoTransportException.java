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
package io.prestosql.spi;

public class PrestoTransportException
        extends PrestoException
{
    private final HostAddress remoteHost;

    public PrestoTransportException(ErrorCodeSupplier errorCode, HostAddress remoteHost, String message)
    {
        this(errorCode, remoteHost, message, null);
    }

    public PrestoTransportException(ErrorCodeSupplier errorCodeSupplier, HostAddress remoteHost, String message, Throwable cause)
    {
        super(errorCodeSupplier, message, cause);
        this.remoteHost = remoteHost;
    }

    public HostAddress getRemoteHost()
    {
        return remoteHost;
    }
}
