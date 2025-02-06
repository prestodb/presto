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
package com.facebook.presto.rewriter.optplus;

public enum OptimizerStatus
{
    OPTIMIZER_STARTED("Optimisation STARTED"),
    OPTIMIZER_REWRITTEN("Optimisation REWRITTEN"),
    OPTIMIZER_FALLBACK("Optimisation FALLBACK"),
    OPTIMIZER_GUIDELINE_RECEIVED("Optimisation OPTGUIDELINES RECEIVED"),
    OPTIMIZER_GUIDELINE_APPLIED("Optimisation OPTGUIDELINES APPLIED"),
    OPTIMIZER_COMPLETED("Optimisation COMPLETED");
    private final String status;

    public String getStatus()
    {
        return status;
    }

    private OptimizerStatus(String status)
    {
        this.status = status;
    }
}
