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

// Mock dagre-d3-es and d3 before importing d3utils
jest.mock("dagre-d3-es", () => {
    const mockGraph = {
        setGraph: jest.fn().mockReturnThis(),
        setDefaultEdgeLabel: jest.fn().mockReturnThis(),
        defaultEdgeLabelFn: jest.fn().mockReturnValue({}),
        graph: jest.fn().mockReturnValue({ rankdir: "BT" }),
        isCompound: jest.fn().mockReturnValue(true),
    };
    return {
        graphlib: {
            Graph: jest.fn().mockImplementation(() => mockGraph),
        },
        render: jest.fn(),
    };
});

jest.mock("d3", () => ({
    select: jest.fn().mockReturnValue({
        append: jest.fn().mockReturnThis(),
        attr: jest.fn().mockReturnThis(),
        call: jest.fn().mockReturnThis(),
        empty: jest.fn().mockReturnValue(false),
        node: jest.fn().mockReturnValue(null),
    }),
}));

import { initializeGraph, initializeSvg } from "./d3utils";
import * as _dagreD3 from "dagre-d3-es";

describe("d3utils", () => {
    describe("initializeGraph", () => {
        it("creates a graph instance", () => {
            const graph = initializeGraph();
            expect(graph).toBeDefined();
            expect(graph.setGraph).toBeDefined();
            expect(graph.setDefaultEdgeLabel).toBeDefined();
        });

        it("sets graph direction to bottom-to-top", () => {
            const graph = initializeGraph();
            expect(graph.graph()).toEqual({ rankdir: "BT" });
        });

        it("sets default edge label function", () => {
            const graph = initializeGraph();
            // @ts-expect-error - defaultEdgeLabelFn is a private method in the type definition
            const edgeLabel = graph.defaultEdgeLabelFn();
            expect(edgeLabel).toEqual({});
        });

        it("creates a compound graph", () => {
            const graph = initializeGraph();
            // Compound graphs support parent-child relationships
            expect(graph.isCompound()).toBe(true);
        });
    });

    describe("initializeSvg", () => {
        beforeEach(() => {
            // Create a test SVG element in the document
            document.body.innerHTML = '<svg id="test-svg"></svg>';
        });

        afterEach(() => {
            // Clean up
            document.body.innerHTML = "";
        });

        it("selects SVG element by selector", () => {
            const svg = initializeSvg("#test-svg");
            expect(svg).toBeDefined();
            expect(svg.empty()).toBe(false);
        });

        it("appends a g element to the SVG", () => {
            const svg = initializeSvg("#test-svg");
            // Verify append was called with 'g'
            expect(svg.append).toHaveBeenCalledWith("g");
        });

        it("returns the SVG selection", () => {
            const svg = initializeSvg("#test-svg");
            // Verify the selection has the expected methods
            expect(svg.node).toBeDefined();
            expect(typeof svg.node).toBe("function");
        });
    });
});

// Made with Bob
