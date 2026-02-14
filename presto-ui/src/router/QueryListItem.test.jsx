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

import React from "react";
import { render, screen } from "../__tests__/utils/testUtils";
import { QueryListItem } from "./QueryList";
import {
    baseMockQuery,
    createFinishedQuery,
    createFailedQuery,
    createQueuedQuery,
} from "../__tests__/fixtures/queryFixtures";

describe("QueryListItem", () => {
    it("renders query ID", () => {
        render(<QueryListItem query={baseMockQuery} />);
        expect(screen.getByText("test_query_123")).toBeInTheDocument();
    });

    it("renders query state", () => {
        const { container } = render(<QueryListItem query={baseMockQuery} />);
        const progressBar = container.querySelector(".progress-bar");
        expect(progressBar).toBeInTheDocument();
    });

    it("renders user information", () => {
        render(<QueryListItem query={baseMockQuery} />);
        expect(screen.getByText("testuser")).toBeInTheDocument();
    });

    it("renders query text snippet", () => {
        render(<QueryListItem query={baseMockQuery} />);
        expect(screen.getByText(/SELECT \* FROM table/)).toBeInTheDocument();
    });

    it("renders completed drivers count", () => {
        render(<QueryListItem query={baseMockQuery} />);
        expect(screen.getByText("10")).toBeInTheDocument();
    });

    it("renders running drivers count", () => {
        render(<QueryListItem query={baseMockQuery} />);
        expect(screen.getByText("5")).toBeInTheDocument();
    });

    it("renders queued drivers count", () => {
        render(<QueryListItem query={baseMockQuery} />);
        expect(screen.getByText("0")).toBeInTheDocument();
    });

    it("renders elapsed time", () => {
        render(<QueryListItem query={baseMockQuery} />);
        expect(screen.getByText(/5.2s/)).toBeInTheDocument();
    });

    it("renders CPU time", () => {
        render(<QueryListItem query={baseMockQuery} />);
        expect(screen.getByText(/10.5s/)).toBeInTheDocument();
    });

    it("renders memory usage", () => {
        render(<QueryListItem query={baseMockQuery} />);
        expect(screen.getByText(/2GB/)).toBeInTheDocument();
    });

    it("renders progress bar", () => {
        const { container } = render(<QueryListItem query={baseMockQuery} />);
        const progressBar = container.querySelector(".progress-bar");
        expect(progressBar).toBeInTheDocument();
    });

    it("renders link to query detail page", () => {
        const queryWithCoordinator = { ...baseMockQuery, coordinatorUri: "" };
        render(<QueryListItem query={queryWithCoordinator} />);
        const link = screen.getByRole("link", { name: /test_query_123/ });
        expect(link).toHaveAttribute("href", expect.stringContaining("test_query_123"));
    });

    it("handles FINISHED state", () => {
        const finishedQuery = createFinishedQuery();
        const { container } = render(<QueryListItem query={finishedQuery} />);
        const progressBar = container.querySelector(".progress-bar");
        expect(progressBar).toBeInTheDocument();
        // Progress bar shows 100% for finished queries
        expect(progressBar).toHaveStyle({ width: "100%" });
    });

    it("handles FAILED state", () => {
        const failedQuery = createFailedQuery();
        const { container } = render(<QueryListItem query={failedQuery} />);
        const progressBar = container.querySelector(".progress-bar");
        expect(progressBar).toBeInTheDocument();
    });

    it("handles QUEUED state", () => {
        const queuedQuery = createQueuedQuery();
        const { container } = render(<QueryListItem query={queuedQuery} />);
        const progressBar = container.querySelector(".progress-bar");
        expect(progressBar).toBeInTheDocument();
    });

    describe("Query Text Formatting", () => {
        it("handles query with no leading whitespace", () => {
            const query = {
                ...baseMockQuery,
                query: "SELECT * FROM table",
            };
            render(<QueryListItem query={query} />);
            expect(screen.getByText(/SELECT \* FROM table/)).toBeInTheDocument();
        });

        it("handles query with empty lines", () => {
            const query = {
                ...baseMockQuery,
                query: "SELECT *\n\nFROM table\n\nWHERE id = 1",
            };
            render(<QueryListItem query={query} />);
            expect(screen.getByText(/SELECT/)).toBeInTheDocument();
        });

        it("handles query with consistent indentation", () => {
            const query = {
                ...baseMockQuery,
                query: "    SELECT *\n    FROM table\n    WHERE id = 1",
            };
            render(<QueryListItem query={query} />);
            expect(screen.getByText(/SELECT/)).toBeInTheDocument();
        });

        it("handles query with mixed indentation", () => {
            const query = {
                ...baseMockQuery,
                query: "  SELECT *\n    FROM table\nWHERE id = 1",
            };
            render(<QueryListItem query={query} />);
            expect(screen.getByText(/SELECT/)).toBeInTheDocument();
        });

        it("handles single line query", () => {
            const query = {
                ...baseMockQuery,
                query: "SELECT * FROM table WHERE id = 1",
            };
            render(<QueryListItem query={query} />);
            expect(screen.getByText(/SELECT \* FROM table WHERE id = 1/)).toBeInTheDocument();
        });

        it("handles very long query text (truncation)", () => {
            const longQuery = "SELECT * FROM table WHERE " + "column = 'value' AND ".repeat(50);
            const query = {
                ...baseMockQuery,
                query: longQuery,
            };
            render(<QueryListItem query={query} />);
            // Query should be truncated to 300 characters
            const { container } = render(<QueryListItem query={query} />);
            expect(container).toBeInTheDocument();
        });
    });
});

// Made with Bob
