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
package com.facebook.presto.plugin.openlineage;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public enum OpenLineageJobInterpolatedValues
{
    QUERY_ID(jobContext -> jobContext.getQueryMetadata().getQueryId()),
    SOURCE(jobContext -> jobContext.getQueryContext().getSource().orElse("")),
    CLIENT_IP(jobContext -> jobContext.getQueryContext().getRemoteClientAddress().orElse("")),
    USER(jobContext -> jobContext.getQueryContext().getUser());

    private final Function<OpenLineageJobContext, String> valueProvider;

    OpenLineageJobInterpolatedValues(Function<OpenLineageJobContext, String> valueProvider)
    {
        this.valueProvider = requireNonNull(valueProvider, "valueProvider is null");
    }

    public String value(OpenLineageJobContext context)
    {
        return valueProvider.apply(context);
    }
}
