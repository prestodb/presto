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

import * as dagreD3 from "dagre-d3-es";
import * as d3 from "d3";

// DagreD3 Graph-related functions
// ===============================

export function initializeGraph(): any
{
    return new dagreD3.graphlib.Graph({compound: true})
        .setGraph({rankdir: 'BT'})
        .setDefaultEdgeLabel(function () { return {}; });
}

export function initializeSvg(selector: any): any
{
    const svg = d3.select(selector);
    svg.append("g");

    return svg;
}
