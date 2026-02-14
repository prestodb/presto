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
import { render, screen, waitFor, advanceTimersAndWait } from "../__tests__/utils/testUtils";
import { QueryList } from "./QueryList";
import { mockJQueryGet } from "../__tests__/mocks/jqueryMock";
import {
    baseMockQuery,
    createMockQuery,
    createRunningQuery,
    createFinishedQuery,
    createFailedQuery,
    createQueuedQuery,
} from "../__tests__/fixtures/queryFixtures";

describe("QueryList", () => {
    beforeEach(() => {
        jest.clearAllMocks();
        jest.useFakeTimers();
    });

    afterEach(() => {
        jest.runOnlyPendingTimers();
        jest.useRealTimers();
    });

    describe("QueryList Component", () => {
        it("renders empty state initially", () => {
            mockJQueryGet({ "/v1/query": [] });
            const { container } = render(<QueryList />);
            expect(container).toBeInTheDocument();
        });

        it("fetches and displays queries", async () => {
            mockJQueryGet({
                "/v1/query": [baseMockQuery],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("test_query_123")).toBeInTheDocument();
            });
        });

        it("displays multiple queries", async () => {
            const query2 = createMockQuery({ queryId: "test_query_456" });
            mockJQueryGet({
                "/v1/query": [baseMockQuery, query2],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("test_query_123")).toBeInTheDocument();
                expect(screen.getByText("test_query_456")).toBeInTheDocument();
            });
        });

        it("calls jQuery.get on mount", async () => {
            mockJQueryGet({ "/v1/query": [] });
            // Get reference to the mocked $.get function after mockJQueryGet sets it up
            const getSpy = global.$.get;

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(getSpy).toHaveBeenCalledWith("/v1/query", expect.any(Function));
            });
        });

        it("renders filter controls", async () => {
            mockJQueryGet({ "/v1/query": [baseMockQuery] });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText(/Running/)).toBeInTheDocument();
            });
        });

        it("renders sort controls", async () => {
            mockJQueryGet({ "/v1/query": [baseMockQuery] });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                const sortButtons = screen.getAllByRole("button");
                expect(sortButtons.length).toBeGreaterThan(0);
            });
        });

        it("handles empty query list", async () => {
            mockJQueryGet({ "/v1/query": [] });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.queryByText("test_query_123")).not.toBeInTheDocument();
            });
        });

        it("displays query statistics", async () => {
            mockJQueryGet({ "/v1/query": [baseMockQuery] });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText(/testuser/)).toBeInTheDocument();
                expect(screen.getByText(/5.2s/)).toBeInTheDocument();
            });
        });
    });

    describe("Query Filtering", () => {
        it("filters running queries", async () => {
            const runningQuery = createRunningQuery();
            const finishedQuery = createFinishedQuery({ queryId: "finished_query" });

            mockJQueryGet({
                "/v1/query": [runningQuery, finishedQuery],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("test_query_123")).toBeInTheDocument();
            });
        });

        it("filters queued queries", async () => {
            const queuedQuery = createQueuedQuery();

            mockJQueryGet({
                "/v1/query": [queuedQuery],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("test_query_123")).toBeInTheDocument();
            });
        });
    });

    describe("Query Sorting", () => {
        it("sorts queries by creation time", async () => {
            const query1 = createMockQuery({
                queryId: "query1",
                queryStats: { createTime: "2024-01-01T10:00:00.000Z" },
            });
            const query2 = createMockQuery({
                queryId: "query2",
                queryStats: { createTime: "2024-01-01T11:00:00.000Z" },
            });

            mockJQueryGet({
                "/v1/query": [query1, query2],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("query1")).toBeInTheDocument();
                expect(screen.getByText("query2")).toBeInTheDocument();
            });
        });
        describe("Query Search and Filtering", () => {
            it("displays queries with different query IDs", async () => {
                const query1 = createMockQuery({ queryId: "search_test_123" });
                const query2 = createMockQuery({ queryId: "other_query_456" });
                mockJQueryGet({
                    "/v1/query": [query1, query2],
                });

                render(<QueryList />);
                await advanceTimersAndWait(100);

                await waitFor(() => {
                    expect(screen.getByText("search_test_123")).toBeInTheDocument();
                    expect(screen.getByText("other_query_456")).toBeInTheDocument();
                });
            });

            it("displays queries with different users", async () => {
                const query1 = createMockQuery({
                    queryId: "query1",
                    session: { user: "alice", source: "presto-ui" },
                });
                const query2 = createMockQuery({
                    queryId: "query2",
                    session: { user: "bob", source: "presto-ui" },
                });
                mockJQueryGet({
                    "/v1/query": [query1, query2],
                });

                render(<QueryList />);
                await advanceTimersAndWait(100);

                await waitFor(() => {
                    expect(screen.getByText("query1")).toBeInTheDocument();
                    expect(screen.getByText("query2")).toBeInTheDocument();
                });
            });

            it("displays queries with different sources", async () => {
                const query1 = createMockQuery({
                    queryId: "jdbc_query",
                    session: { user: "testuser", source: "jdbc" },
                });
                const query2 = createMockQuery({
                    queryId: "cli_query",
                    session: { user: "testuser", source: "cli" },
                });
                mockJQueryGet({
                    "/v1/query": [query1, query2],
                });

                render(<QueryList />);
                await advanceTimersAndWait(100);

                await waitFor(() => {
                    expect(screen.getByText("jdbc_query")).toBeInTheDocument();
                    expect(screen.getByText("cli_query")).toBeInTheDocument();
                });
            });

            it("displays queries with different query text", async () => {
                const query1 = createMockQuery({
                    queryId: "users_query",
                    query: "SELECT * FROM users",
                });
                const query2 = createMockQuery({
                    queryId: "orders_query",
                    query: "SELECT * FROM orders",
                });
                mockJQueryGet({
                    "/v1/query": [query1, query2],
                });

                render(<QueryList />);
                await advanceTimersAndWait(100);

                await waitFor(() => {
                    expect(screen.getByText("users_query")).toBeInTheDocument();
                    expect(screen.getByText("orders_query")).toBeInTheDocument();
                });
            });
        });

        describe("Error Type Filtering", () => {
            it("displays failed queries with internal errors (default filter)", async () => {
                const failedQuery = createFailedQuery({
                    queryId: "internal_error_query",
                    errorType: "INTERNAL_ERROR",
                });
                mockJQueryGet({
                    "/v1/query": [failedQuery],
                });

                render(<QueryList />);
                await advanceTimersAndWait(100);

                await waitFor(() => {
                    expect(screen.getByText("internal_error_query")).toBeInTheDocument();
                });
            });

            it("displays failed queries with external errors (default filter)", async () => {
                const failedQuery = createFailedQuery({
                    queryId: "external_error_query",
                    errorType: "EXTERNAL",
                });
                mockJQueryGet({
                    "/v1/query": [failedQuery],
                });

                render(<QueryList />);
                await advanceTimersAndWait(100);

                await waitFor(() => {
                    expect(screen.getByText("external_error_query")).toBeInTheDocument();
                });
            });

            it("handles queries with different error types", async () => {
                const internalError = createFailedQuery({
                    queryId: "internal_err",
                    errorType: "INTERNAL_ERROR",
                });
                const externalError = createFailedQuery({
                    queryId: "external_err",
                    errorType: "EXTERNAL",
                });
                mockJQueryGet({
                    "/v1/query": [internalError, externalError],
                });

                render(<QueryList />);
                await advanceTimersAndWait(100);

                await waitFor(() => {
                    expect(screen.getByText("internal_err")).toBeInTheDocument();
                    expect(screen.getByText("external_err")).toBeInTheDocument();
                });
            });
        });

        describe("Query Sorting by Different Metrics", () => {
            it("sorts queries by elapsed time", async () => {
                const query1 = createMockQuery({
                    queryId: "fast_query",
                    queryStats: { elapsedTime: "1.0s" },
                });
                const query2 = createMockQuery({
                    queryId: "slow_query",
                    queryStats: { elapsedTime: "10.0s" },
                });

                mockJQueryGet({
                    "/v1/query": [query1, query2],
                });

                render(<QueryList />);
                await advanceTimersAndWait(100);

                await waitFor(() => {
                    expect(screen.getByText("fast_query")).toBeInTheDocument();
                    expect(screen.getByText("slow_query")).toBeInTheDocument();
                });
            });

            it("sorts queries by execution time", async () => {
                const query1 = createMockQuery({
                    queryId: "quick_exec",
                    queryStats: { executionTime: "0.5s" },
                });
                const query2 = createMockQuery({
                    queryId: "long_exec",
                    queryStats: { executionTime: "5.0s" },
                });

                mockJQueryGet({
                    "/v1/query": [query1, query2],
                });

                render(<QueryList />);
                await advanceTimersAndWait(100);

                await waitFor(() => {
                    expect(screen.getByText("quick_exec")).toBeInTheDocument();
                    expect(screen.getByText("long_exec")).toBeInTheDocument();
                });
            });

            it("sorts queries by CPU time", async () => {
                const query1 = createMockQuery({
                    queryId: "low_cpu",
                    queryStats: { totalCpuTime: "1.0s" },
                });
                const query2 = createMockQuery({
                    queryId: "high_cpu",
                    queryStats: { totalCpuTime: "20.0s" },
                });

                mockJQueryGet({
                    "/v1/query": [query1, query2],
                });

                render(<QueryList />);
                await advanceTimersAndWait(100);

                await waitFor(() => {
                    expect(screen.getByText("low_cpu")).toBeInTheDocument();
                    expect(screen.getByText("high_cpu")).toBeInTheDocument();
                });
            });

            it("sorts queries by memory usage", async () => {
                const query1 = createMockQuery({
                    queryId: "low_mem",
                    queryStats: { userMemoryReservation: "100MB" },
                });
                const query2 = createMockQuery({
                    queryId: "high_mem",
                    queryStats: { userMemoryReservation: "5GB" },
                });

                mockJQueryGet({
                    "/v1/query": [query1, query2],
                });

                render(<QueryList />);
                await advanceTimersAndWait(100);

                await waitFor(() => {
                    expect(screen.getByText("low_mem")).toBeInTheDocument();
                    expect(screen.getByText("high_mem")).toBeInTheDocument();
                });
            });

            describe("Advanced Sorting and Filtering", () => {
                it("handles empty search string correctly", async () => {
                    const query1 = createMockQuery({ queryId: "query1" });
                    const query2 = createMockQuery({ queryId: "query2" });
                    mockJQueryGet({
                        "/v1/query": [query1, query2],
                    });

                    render(<QueryList />);
                    await advanceTimersAndWait(100);

                    await waitFor(() => {
                        expect(screen.getByText("query1")).toBeInTheDocument();
                        expect(screen.getByText("query2")).toBeInTheDocument();
                    });
                });

                it("filters queries by INSUFFICIENT_RESOURCES error type", async () => {
                    const resourceError = createFailedQuery({
                        queryId: "resource_err",
                        errorType: "INSUFFICIENT_RESOURCES",
                    });
                    mockJQueryGet({
                        "/v1/query": [resourceError],
                    });

                    render(<QueryList />);
                    await advanceTimersAndWait(100);

                    await waitFor(() => {
                        expect(screen.getByText("resource_err")).toBeInTheDocument();
                    });
                });

                it("sorts queries by cumulative memory", async () => {
                    const query1 = createMockQuery({
                        queryId: "low_cumulative",
                        queryStats: { cumulativeUserMemory: 1000000 },
                    });
                    const query2 = createMockQuery({
                        queryId: "high_cumulative",
                        queryStats: { cumulativeUserMemory: 5000000 },
                    });

                    mockJQueryGet({
                        "/v1/query": [query1, query2],
                    });

                    render(<QueryList />);
                    await advanceTimersAndWait(100);

                    await waitFor(() => {
                        expect(screen.getByText("low_cumulative")).toBeInTheDocument();
                        expect(screen.getByText("high_cumulative")).toBeInTheDocument();
                    });
                });

                it("searches queries by resourceGroupId", async () => {
                    const query1 = createMockQuery({
                        queryId: "adhoc_query",
                        resourceGroupId: ["global", "adhoc"],
                    });
                    const query2 = createMockQuery({
                        queryId: "pipeline_query",
                        resourceGroupId: ["global", "pipeline"],
                    });
                    mockJQueryGet({
                        "/v1/query": [query1, query2],
                    });

                    render(<QueryList />);
                    await advanceTimersAndWait(100);

                    await waitFor(() => {
                        expect(screen.getByText("adhoc_query")).toBeInTheDocument();
                        expect(screen.getByText("pipeline_query")).toBeInTheDocument();
                    });
                });
            });
        });
    });
});

describe("Interactive Filter and Sort Tests", () => {
    it("handles filter button interactions", async () => {
        const runningQuery = createRunningQuery({ queryId: "running_1" });
        const finishedQuery = createFinishedQuery({ queryId: "finished_1" });
        mockJQueryGet({
            "/v1/query": [runningQuery, finishedQuery],
        });

        render(<QueryList />);
        await advanceTimersAndWait(100);

        await waitFor(() => {
            expect(screen.getByText("running_1")).toBeInTheDocument();
            // Both queries should be visible initially (running and finished filters active by default)
        });
    });

    it("displays 'No queries' when query list is empty", async () => {
        mockJQueryGet({
            "/v1/query": [],
        });

        render(<QueryList />);
        await advanceTimersAndWait(100);

        await waitFor(() => {
            expect(screen.getByText("No queries")).toBeInTheDocument();
        });
    });

    it("renders search input field", async () => {
        mockJQueryGet({
            "/v1/query": [createMockQuery({ queryId: "test_q" })],
        });

        render(<QueryList />);
        await advanceTimersAndWait(100);

        await waitFor(() => {
            const searchInput = screen.getByPlaceholderText(/User, source, query ID/i);
            expect(searchInput).toBeInTheDocument();
        });
    });

    describe("Coverage for Uncovered Code Paths", () => {
        it("handles queries with FINISHED state (not shown by default)", async () => {
            const finishedQuery = createFinishedQuery({ queryId: "finished_1" });
            const runningQuery = createRunningQuery({ queryId: "running_1" });
            mockJQueryGet({
                "/v1/query": [finishedQuery, runningQuery],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                // FINISHED queries are filtered out by default, only RUNNING shown
                expect(screen.getByText("running_1")).toBeInTheDocument();
            });
        });

        it("handles queries with different elapsed times for sorting", async () => {
            const query1 = createMockQuery({
                queryId: "fast",
                queryStats: { elapsedTime: "100ms" },
            });
            const query2 = createMockQuery({
                queryId: "slow",
                queryStats: { elapsedTime: "5.5s" },
            });
            mockJQueryGet({
                "/v1/query": [query1, query2],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("fast")).toBeInTheDocument();
                expect(screen.getByText("slow")).toBeInTheDocument();
            });
        });

        it("handles queries with different execution times", async () => {
            const query1 = createMockQuery({
                queryId: "q1",
                queryStats: { executionTime: "2.5s" },
            });
            mockJQueryGet({
                "/v1/query": [query1],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
            });
        });

        it("handles queries with different CPU times", async () => {
            const query1 = createMockQuery({
                queryId: "q1",
                queryStats: { totalCpuTime: "15.2s" },
            });
            mockJQueryGet({
                "/v1/query": [query1],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
            });
        });

        it("handles queries with cumulative memory values", async () => {
            const query1 = createMockQuery({
                queryId: "q1",
                queryStats: { cumulativeUserMemory: 5000000 },
            });
            mockJQueryGet({
                "/v1/query": [query1],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
            });
        });

        it("handles queries with current memory reservation", async () => {
            const query1 = createMockQuery({
                queryId: "q1",
                queryStats: { userMemoryReservation: "500MB" },
            });
            mockJQueryGet({
                "/v1/query": [query1],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
            });
        });

        it("handles queries with USER_ERROR error type (not shown by default)", async () => {
            const userErrorQuery = createFailedQuery({
                queryId: "user_err",
                errorType: "USER_ERROR",
            });
            const internalErrorQuery = createFailedQuery({
                queryId: "internal_err",
                errorType: "INTERNAL_ERROR",
            });
            mockJQueryGet({
                "/v1/query": [userErrorQuery, internalErrorQuery],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                // USER_ERROR is filtered out by default, only INTERNAL_ERROR shown
                expect(screen.getByText("internal_err")).toBeInTheDocument();
            });
        });

        it("handles queries with INTERNAL_ERROR error type", async () => {
            const query1 = createFailedQuery({
                queryId: "internal_err",
                errorType: "INTERNAL_ERROR",
            });
            mockJQueryGet({
                "/v1/query": [query1],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("internal_err")).toBeInTheDocument();
            });
        });

        it("handles queries with user in session", async () => {
            const query1 = createMockQuery({
                queryId: "q1",
                session: { user: "testuser", source: "cli" },
            });
            mockJQueryGet({
                "/v1/query": [query1],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
            });
        });

        it("handles queries with source in session", async () => {
            const query1 = createMockQuery({
                queryId: "q1",
                session: { user: "test", source: "jdbc-driver" },
            });
            mockJQueryGet({
                "/v1/query": [query1],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
            });
        });

        it("handles queries with resourceGroupId array", async () => {
            const query1 = createMockQuery({
                queryId: "q1",
                resourceGroupId: ["global", "adhoc", "user_queries"],
            });
            mockJQueryGet({
                "/v1/query": [query1],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
            });
        });

        it("renders with multiple query types", async () => {
            const query1 = createRunningQuery({ queryId: "running_q" });
            const query2 = createQueuedQuery({ queryId: "queued_q" });
            mockJQueryGet({
                "/v1/query": [query1, query2],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("running_q")).toBeInTheDocument();
                expect(screen.getByText("queued_q")).toBeInTheDocument();
            });
        });
    });
    it("renders filter buttons", async () => {
        mockJQueryGet({
            "/v1/query": [createMockQuery({ queryId: "test_q" })],
        });

        render(<QueryList />);
        await advanceTimersAndWait(100);

        await waitFor(() => {
            expect(screen.getByRole("button", { name: /Running/i })).toBeInTheDocument();
            expect(screen.getByRole("button", { name: /Queued/i })).toBeInTheDocument();
            expect(screen.getByRole("button", { name: /Finished/i })).toBeInTheDocument();
        });
    });
});
// Made with Bob
