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
package com.facebook.presto.thrift.api.udf;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.spi.page.SerializedPage;

import javax.annotation.Nullable;

import java.util.Objects;

import static com.facebook.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static com.facebook.presto.thrift.api.udf.ThriftUdfPageFormat.PRESTO_SERIALIZED;
import static com.facebook.presto.thrift.api.udf.ThriftUdfPageFormat.PRESTO_THRIFT;

@ThriftStruct("UdfPage")
public class ThriftUdfPage
{
    private final ThriftUdfPageFormat pageFormat;
    private final PrestoThriftPage thriftPage;
    private final ThriftSerializedPage prestoPage;

    @ThriftConstructor
    public ThriftUdfPage(
            ThriftUdfPageFormat pageFormat,
            @Nullable PrestoThriftPage thriftPage,
            @Nullable ThriftSerializedPage prestoPage)
    {
        this.pageFormat = pageFormat;
        this.thriftPage = thriftPage;
        this.prestoPage = prestoPage;
    }

    @ThriftField(value = 1)
    public ThriftUdfPageFormat getPageFormat()
    {
        return pageFormat;
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public PrestoThriftPage getThriftPage()
    {
        return thriftPage;
    }

    @Nullable
    @ThriftField(value = 3, requiredness = OPTIONAL)
    public ThriftSerializedPage getPrestoPage()
    {
        return prestoPage;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ThriftUdfPage other = (ThriftUdfPage) obj;
        return Objects.equals(this.pageFormat, other.pageFormat) &&
                Objects.equals(this.thriftPage, other.thriftPage) &&
                Objects.equals(this.prestoPage, other.prestoPage);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(pageFormat, thriftPage, prestoPage);
    }

    public static ThriftUdfPage thriftPage(PrestoThriftPage thriftPage)
    {
        return new ThriftUdfPage(PRESTO_THRIFT, thriftPage, null);
    }

    public static ThriftUdfPage prestoPage(SerializedPage prestoPage)
    {
        return new ThriftUdfPage(PRESTO_SERIALIZED, null, new ThriftSerializedPage(prestoPage));
    }
}
