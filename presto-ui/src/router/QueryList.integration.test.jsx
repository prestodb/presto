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
import {
    render,
    screen,
    waitFor,
    fireEvent,
    clickButtonSync,
    advanceTimersAndWait,
    setInputValue,
} from "../__tests__/utils/testUtils";
import { setupIntegrationTest } from "../__tests__/utils/setupHelpers";
import { QueryList } from "./QueryList";
import { mockJQueryGet } from "../__tests__/mocks/jqueryMock";
import {
    createRunningQuery,
    createFinishedQuery,
    createFailedQuery,
    createQueuedQuery,
} from "../__tests__/fixtures/queryFixtures";

/**
 * Integration tests for QueryList component
 * Tests user interactions including filter buttons, search, and dropdown menus
 */
describe("QueryList Integration Tests", () => {
    setupIntegrationTest();

    describe("Filter Button Interactions", () => {
        it("clicking FINISHED filter button shows finished queries", async () => {
            const runningQuery = createRunningQuery({ queryId: "running_1" });
            const finishedQuery = createFinishedQuery({ queryId: "finished_1" });
            mockJQueryGet({
                "/v1/query": [runningQuery, finishedQuery],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("running_1")).toBeInTheDocument();
                expect(screen.queryByText("finished_1")).not.toBeInTheDocument();
            });

            clickButtonSync(/Finished/i);

            await waitFor(() => {
                expect(screen.getByText("finished_1")).toBeInTheDocument();
            });
        });

        it("toggling RUNNING filter removes running queries", async () => {
            const runningQuery = createRunningQuery({ queryId: "running_1" });
            const queuedQuery = createQueuedQuery({ queryId: "queued_1" });
            mockJQueryGet({
                "/v1/query": [runningQuery, queuedQuery],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("running_1")).toBeInTheDocument();
                expect(screen.getByText("queued_1")).toBeInTheDocument();
            });

            clickButtonSync(/Running/i);

            await waitFor(() => {
                expect(screen.queryByText("running_1")).not.toBeInTheDocument();
                expect(screen.getByText("queued_1")).toBeInTheDocument();
            });
        });

        it("toggling QUEUED filter removes queued queries", async () => {
            const runningQuery = createRunningQuery({ queryId: "running_1" });
            const queuedQuery = createQueuedQuery({ queryId: "queued_1" });
            mockJQueryGet({
                "/v1/query": [runningQuery, queuedQuery],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("running_1")).toBeInTheDocument();
                expect(screen.getByText("queued_1")).toBeInTheDocument();
            });

            clickButtonSync(/Queued/i);

            await waitFor(() => {
                expect(screen.getByText("running_1")).toBeInTheDocument();
                expect(screen.queryByText("queued_1")).not.toBeInTheDocument();
            });
        });

        it("clicking multiple filters shows combined results", async () => {
            const runningQuery = createRunningQuery({ queryId: "running_1" });
            const finishedQuery = createFinishedQuery({ queryId: "finished_1" });
            const queuedQuery = createQueuedQuery({ queryId: "queued_1" });
            mockJQueryGet({
                "/v1/query": [runningQuery, finishedQuery, queuedQuery],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("running_1")).toBeInTheDocument();
                expect(screen.getByText("queued_1")).toBeInTheDocument();
                expect(screen.queryByText("finished_1")).not.toBeInTheDocument();
            });

            clickButtonSync(/Finished/i);

            await waitFor(() => {
                expect(screen.getByText("running_1")).toBeInTheDocument();
                expect(screen.getByText("queued_1")).toBeInTheDocument();
                expect(screen.getByText("finished_1")).toBeInTheDocument();
            });
        });
    });

    describe("Search Functionality", () => {
        it("typing in search box filters queries by query ID", async () => {
            const query1 = createRunningQuery({ queryId: "query_abc_123" });
            const query2 = createRunningQuery({ queryId: "query_xyz_456" });
            mockJQueryGet({
                "/v1/query": [query1, query2],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("query_abc_123")).toBeInTheDocument();
                expect(screen.getByText("query_xyz_456")).toBeInTheDocument();
            });

            setInputValue(/User, source, query ID/i, "abc");
            await advanceTimersAndWait(300);

            await waitFor(() => {
                expect(screen.getByText("query_abc_123")).toBeInTheDocument();
                expect(screen.queryByText("query_xyz_456")).not.toBeInTheDocument();
            });
        });

        it("search filters by user name", async () => {
            const query1 = createRunningQuery({
                queryId: "q1",
                session: { user: "alice", source: "cli" },
            });
            const query2 = createRunningQuery({
                queryId: "q2",
                session: { user: "bob", source: "cli" },
            });
            mockJQueryGet({
                "/v1/query": [query1, query2],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
                expect(screen.getByText("q2")).toBeInTheDocument();
            });

            setInputValue(/User, source, query ID/i, "alice");
            await advanceTimersAndWait(300);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
                expect(screen.queryByText("q2")).not.toBeInTheDocument();
            });
        });

        it("search filters by source", async () => {
            const query1 = createRunningQuery({
                queryId: "q1",
                session: { user: "test", source: "jdbc-driver" },
            });
            const query2 = createRunningQuery({
                queryId: "q2",
                session: { user: "test", source: "presto-cli" },
            });
            mockJQueryGet({
                "/v1/query": [query1, query2],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
                expect(screen.getByText("q2")).toBeInTheDocument();
            });

            setInputValue(/User, source, query ID/i, "jdbc");
            await advanceTimersAndWait(300);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
                expect(screen.queryByText("q2")).not.toBeInTheDocument();
            });
        });

        it("search filters by resource group", async () => {
            const query1 = createRunningQuery({
                queryId: "q1",
                resourceGroupId: ["global", "adhoc"],
            });
            const query2 = createRunningQuery({
                queryId: "q2",
                resourceGroupId: ["global", "pipeline"],
            });
            mockJQueryGet({
                "/v1/query": [query1, query2],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
                expect(screen.getByText("q2")).toBeInTheDocument();
            });

            setInputValue(/User, source, query ID/i, "adhoc");
            await advanceTimersAndWait(300);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
                expect(screen.queryByText("q2")).not.toBeInTheDocument();
            });
        });

        it("search filters by query text", async () => {
            const query1 = createRunningQuery({
                queryId: "q1",
                query: "SELECT * FROM users WHERE name LIKE '%test%'",
            });
            const query2 = createRunningQuery({
                queryId: "q2",
                query: "SELECT * FROM orders WHERE status = 'pending'",
            });
            mockJQueryGet({
                "/v1/query": [query1, query2],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
                expect(screen.getByText("q2")).toBeInTheDocument();
            });

            setInputValue(/User, source, query ID/i, "users");
            await advanceTimersAndWait(300);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
                expect(screen.queryByText("q2")).not.toBeInTheDocument();
            });
        });

        it("clearing search shows all queries again", async () => {
            const query1 = createRunningQuery({ queryId: "q1" });
            const query2 = createRunningQuery({ queryId: "q2" });
            mockJQueryGet({
                "/v1/query": [query1, query2],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
                expect(screen.getByText("q2")).toBeInTheDocument();
            });

            setInputValue(/User, source, query ID/i, "q1");
            await advanceTimersAndWait(300);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
                expect(screen.queryByText("q2")).not.toBeInTheDocument();
            });

            setInputValue(/User, source, query ID/i, "");
            await advanceTimersAndWait(300);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
                expect(screen.getByText("q2")).toBeInTheDocument();
            });
        });

        it("shows 'No queries matched filters' when search has no results", async () => {
            const query1 = createRunningQuery({ queryId: "q1" });
            mockJQueryGet({
                "/v1/query": [query1],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("q1")).toBeInTheDocument();
            });

            setInputValue(/User, source, query ID/i, "nonexistent_query");
            await advanceTimersAndWait(300);

            await waitFor(() => {
                expect(screen.getByText("No queries matched filters")).toBeInTheDocument();
            });
        });
    });

    describe("Combined Filters and Search", () => {
        it("applies both state filters and search together", async () => {
            const runningQuery1 = createRunningQuery({
                queryId: "running_alice",
                session: { user: "alice", source: "cli" },
            });
            const runningQuery2 = createRunningQuery({
                queryId: "running_bob",
                session: { user: "bob", source: "cli" },
            });
            const finishedQuery = createFinishedQuery({
                queryId: "finished_alice",
                session: { user: "alice", source: "cli" },
            });
            mockJQueryGet({
                "/v1/query": [runningQuery1, runningQuery2, finishedQuery],
            });

            render(<QueryList />);
            await advanceTimersAndWait(100);

            await waitFor(() => {
                expect(screen.getByText("running_alice")).toBeInTheDocument();
                expect(screen.getByText("running_bob")).toBeInTheDocument();
                expect(screen.queryByText("finished_alice")).not.toBeInTheDocument();
            });

            setInputValue(/User, source, query ID/i, "alice");
            await advanceTimersAndWait(300);

            await waitFor(() => {
                expect(screen.getByText("running_alice")).toBeInTheDocument();
                expect(screen.queryByText("running_bob")).not.toBeInTheDocument();
                expect(screen.queryByText("finished_alice")).not.toBeInTheDocument();
            });

            clickButtonSync(/Finished/i);

            await waitFor(() => {
                expect(screen.getByText("running_alice")).toBeInTheDocument();
                expect(screen.getByText("finished_alice")).toBeInTheDocument();
                expect(screen.queryByText("running_bob")).not.toBeInTheDocument();
            });
        });
    });

    describe("Dropdown Menu Interactions", () => {
        describe("Sort Dropdown", () => {
            it("sorts queries by execution time when selecting from dropdown", async () => {
                const query1 = createRunningQuery({
                    queryId: "q1",
                    queryStats: { executionTime: "10.5s" },
                });
                const query2 = createRunningQuery({
                    queryId: "q2",
                    queryStats: { executionTime: "5.2s" },
                });
                mockJQueryGet({
                    "/v1/query": [query1, query2],
                });

                render(<QueryList />);
                await advanceTimersAndWait(100);

                await waitFor(() => {
                    expect(screen.getByText("q1")).toBeInTheDocument();
                });

                const sortDropdown = screen.getByText(/Sort/);
                fireEvent.click(sortDropdown);

                const executionOption = screen.getByText("Execution Time");
                fireEvent.click(executionOption);

                await waitFor(() => {
                    const queryElements = screen.getAllByText(/^q[12]$/);
                    expect(queryElements.length).toBeGreaterThan(0);
                });
            });

            it("sorts queries by CPU time when selecting from dropdown", async () => {
                const query1 = createRunningQuery({
                    queryId: "q1",
                    queryStats: { totalCpuTime: "5.0s" },
                });
                const query2 = createRunningQuery({
                    queryId: "q2",
                    queryStats: { totalCpuTime: "15.0s" },
                });
                mockJQueryGet({
                    "/v1/query": [query1, query2],
                });

                render(<QueryList />);
                await advanceTimersAndWait(100);

                await waitFor(() => {
                    expect(screen.getByText("q1")).toBeInTheDocument();
                });

                const sortDropdown = screen.getByText(/Sort/);
                fireEvent.click(sortDropdown);

                const cpuOption = screen.getByText("CPU Time");
                fireEvent.click(cpuOption);

                await waitFor(() => {
                    const queryElements = screen.getAllByText(/^q[12]$/);
                    expect(queryElements.length).toBeGreaterThan(0);
                });
            });

            it("sorts queries by cumulative memory when selecting from dropdown", async () => {
                const query1 = createRunningQuery({
                    queryId: "q1",
                    queryStats: { cumulativeUserMemory: 1000000 },
                });
                const query2 = createRunningQuery({
                    queryId: "q2",
                    queryStats: { cumulativeUserMemory: 5000000 },
                });
                mockJQueryGet({
                    "/v1/query": [query1, query2],
                });

                render(<QueryList />);
                await advanceTimersAndWait(100);

                await waitFor(() => {
                    expect(screen.getByText("q1")).toBeInTheDocument();
                });

                const sortDropdown = screen.getByText(/Sort/);
                fireEvent.click(sortDropdown);

                const memoryOption = screen.getByText("Cumulative User Memory");
                fireEvent.click(memoryOption);

                await waitFor(() => {
                    const queryElements = screen.getAllByText(/^q[12]$/);
                    expect(queryElements.length).toBeGreaterThan(0);
                });
            });

            it("sorts queries by current memory when selecting from dropdown", async () => {
                const query1 = createRunningQuery({
                    queryId: "q1",
                    queryStats: { userMemoryReservation: "100MB" },
                });
                const query2 = createRunningQuery({
                    queryId: "q2",
                    queryStats: { userMemoryReservation: "500MB" },
                });
                mockJQueryGet({
                    "/v1/query": [query1, query2],
                });

                render(<QueryList />);
                await advanceTimersAndWait(100);

                await waitFor(() => {
                    expect(screen.getByText("q1")).toBeInTheDocument();
                });

                const sortDropdown = screen.getByText(/Sort/);
                fireEvent.click(sortDropdown);

                const currentMemoryOption = screen.getByText("Current Memory");
                fireEvent.click(currentMemoryOption);

                await waitFor(() => {
                    const queryElements = screen.getAllByText(/^q[12]$/);
                    expect(queryElements.length).toBeGreaterThan(0);
                });
            });
        });

        describe("Error Type Filter Dropdown", () => {
            it("filters by user error when selecting from dropdown", async () => {
                const runningQuery = createRunningQuery({ queryId: "running_1" });
                const userErrorQuery = createFailedQuery({
                    queryId: "user_error_1",
                    state: "FAILED",
                    errorType: "USER_ERROR",
                });
                const internalErrorQuery = createFailedQuery({
                    queryId: "internal_error_1",
                    state: "FAILED",
                    errorType: "INTERNAL_ERROR",
                });
                mockJQueryGet({
                    "/v1/query": [runningQuery, userErrorQuery, internalErrorQuery],
                });

                render(<QueryList />);
                await advanceTimersAndWait(100);

                await waitFor(() => {
                    expect(screen.getByText("running_1")).toBeInTheDocument();
                });

                const failedDropdown = screen.getByText(/Failed/);
                fireEvent.click(failedDropdown);

                const userErrorOption = screen.getByText("User Error");
                fireEvent.click(userErrorOption);

                await waitFor(() => {
                    expect(screen.getByText("user_error_1")).toBeInTheDocument();
                });
            });

            it("filters by internal error when selecting from dropdown", async () => {
                const runningQuery = createRunningQuery({ queryId: "running_1" });
                const userErrorQuery = createFailedQuery({
                    queryId: "user_error_1",
                    state: "FAILED",
                    errorType: "USER_ERROR",
                });
                const internalErrorQuery = createFailedQuery({
                    queryId: "internal_error_1",
                    state: "FAILED",
                    errorType: "INTERNAL_ERROR",
                });
                mockJQueryGet({
                    "/v1/query": [runningQuery, userErrorQuery, internalErrorQuery],
                });

                render(<QueryList />);
                await advanceTimersAndWait(100);

                await waitFor(() => {
                    expect(screen.getByText("running_1")).toBeInTheDocument();
                });

                const failedDropdown = screen.getByText(/Failed/);
                fireEvent.click(failedDropdown);

                const internalErrorOption = screen.getByText("Internal Error");
                fireEvent.click(internalErrorOption);

                await waitFor(() => {
                    expect(screen.queryByText("internal_error_1")).not.toBeInTheDocument();
                });
            });
        });

        describe("Max Queries Dropdown", () => {
            it("changes max displayed queries when selecting from dropdown", async () => {
                // Create 25 queries
                const queries = Array.from({ length: 25 }, (_, i) => createRunningQuery({ queryId: `q${i + 1}` }));
                mockJQueryGet({
                    "/v1/query": queries,
                });

                render(<QueryList />);
                await advanceTimersAndWait(100);

                await waitFor(() => {
                    expect(screen.getByText("q1")).toBeInTheDocument();
                    expect(screen.getByText("q20")).toBeInTheDocument();
                });

                expect(screen.getByText("q25")).toBeInTheDocument();

                const maxQueriesButtons = screen.getAllByRole("button");
                const maxQueriesButton = maxQueriesButtons.find(
                    (btn) => btn.textContent.includes("20 queries") || btn.textContent.includes("queries")
                );

                if (maxQueriesButton) {
                    fireEvent.click(maxQueriesButton);

                    const max100Option = screen.getByText("100 queries");
                    fireEvent.click(max100Option);

                    await waitFor(() => {
                        expect(screen.getByText("q25")).toBeInTheDocument();
                    });
                }
            });
        });
    });
});
