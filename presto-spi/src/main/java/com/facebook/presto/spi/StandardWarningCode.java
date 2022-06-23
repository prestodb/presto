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
package com.facebook.presto.spi;

public enum StandardWarningCode
        implements WarningCodeSupplier
{
    TOO_MANY_STAGES(0x0000_0001),
    PARSER_WARNING(0x0000_0002),
    PERFORMANCE_WARNING(0x0000_0003),
    SEMANTIC_WARNING(0x0000_0004),
    REDUNDANT_ORDER_BY(0x0000_0005),
    PARTIAL_RESULT_WARNING(0x0000_0006),
    MULTIPLE_ORDER_BY(0x0000_0007),
    DEFAULT_SAMPLE_FUNCTION(0x0000_0008),
    SAMPLED_FIELDS(0x0000_0009),
    /**/;
    private final WarningCode warningCode;

    StandardWarningCode(int code)
    {
        warningCode = new WarningCode(code, name());
    }

    @Override
    public WarningCode toWarningCode()
    {
        return warningCode;
    }
}
