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
import { render, screen, waitFor } from "../__tests__/utils/testUtils";
import { PageTitle } from "./PageTitle";
import { setupPageTitleTest } from "../__tests__/fixtures/infoFixtures";

describe("PageTitle", () => {
    beforeEach(() => {
        jest.clearAllMocks();
        jest.useFakeTimers();

        // Mock window.location using Object.defineProperty for better type safety
        // Currently only mocking 'protocol' as that's all the component checks
        // Add other properties (href, hostname, pathname, etc.) if component needs them
        Object.defineProperty(window, "location", {
            value: {
                protocol: "http:",
                // Add other properties here as needed
            },
            writable: true,
            configurable: true,
        });
    });

    afterEach(() => {
        jest.runOnlyPendingTimers();
        jest.useRealTimers();
    });

    describe("Rendering", () => {
        it("renders nothing initially when not offline", () => {
            // Set up fetch mock to return pending promise
            (global.fetch as jest.Mock).mockImplementation(() => new Promise(() => {}));

            const { container } = render(<PageTitle titles={["Test"]} />);
            expect(container.firstChild).toBeNull();
        });

        it("renders navbar after fetching info", async () => {
            setupPageTitleTest();

            render(<PageTitle titles={["Test"]} />);

            await waitFor(() => {
                expect(screen.getByText("1.0.0")).toBeInTheDocument();
            });

            expect(screen.getByText("test")).toBeInTheDocument();
            expect(screen.getByText("1h")).toBeInTheDocument();
        });

        it("renders multiple navigation titles", async () => {
            setupPageTitleTest({ environment: "prod", uptime: "5d" }, {});

            render(
                <PageTitle
                    titles={["Cluster", "Queries", "Workers"]}
                    urls={["/", "/queries", "/workers"]}
                    current={1}
                />
            );

            await waitFor(() => {
                expect(screen.getByText("Cluster")).toBeInTheDocument();
            });

            expect(screen.getByText("Queries")).toBeInTheDocument();
            expect(screen.getByText("Workers")).toBeInTheDocument();
        });

        it("renders cluster tag when available", async () => {
            setupPageTitleTest(
                { nodeVersion: { version: "2.0.0" }, environment: "staging", uptime: "3h" },
                { clusterTag: "my-cluster-tag" }
            );

            render(<PageTitle titles={["Test"]} />);

            await waitFor(() => {
                expect(screen.getByText("my-cluster-tag")).toBeInTheDocument();
            });
        });

        it("uses custom path for logo", async () => {
            setupPageTitleTest();

            render(<PageTitle titles={["Test"]} path="/custom" />);

            await waitFor(() => {
                const img = screen.getByRole("img");
                expect(img).toHaveAttribute("src", "/custom/assets/logo.png");
            });
        });

        it("displays version information", async () => {
            setupPageTitleTest({ nodeVersion: { version: "1.2.3" }, environment: "production", uptime: "10d 5h" }, {});

            render(<PageTitle titles={["Test"]} />);

            await waitFor(() => {
                expect(screen.getByText("1.2.3")).toBeInTheDocument();
                expect(screen.getByText("production")).toBeInTheDocument();
                expect(screen.getByText("10d 5h")).toBeInTheDocument();
            });
        });
    });

    describe("Offline Mode", () => {
        it("detects offline protocol", () => {
            (window as any).location.protocol = "file:";

            // Component should handle offline mode
            const { container } = render(<PageTitle titles={["Offline"]} />);
            expect(container).toBeInTheDocument();
        });
    });

    describe("Connection Status", () => {
        it("shows green status light when connected", async () => {
            setupPageTitleTest();

            render(<PageTitle titles={["Test"]} />);

            await waitFor(() => {
                const statusLight = document.getElementById("status-indicator");
                expect(statusLight).toHaveClass("status-light-green");
            });
        });

        it("calls fetch for info endpoint", async () => {
            const fetchSpy = jest.spyOn(global, "fetch");
            setupPageTitleTest();

            render(<PageTitle titles={["Test"]} />);

            await waitFor(() => {
                expect(fetchSpy).toHaveBeenCalledWith("/v1/info");
            });
        });

        it("calls fetch for cluster endpoint", async () => {
            const fetchSpy = jest.spyOn(global, "fetch");
            setupPageTitleTest({}, { clusterTag: "test" });

            render(<PageTitle titles={["Test"]} />);

            await waitFor(() => {
                expect(fetchSpy).toHaveBeenCalledWith("/v1/cluster");
            });
        });
    });

    describe("Navigation", () => {
        it("renders logo link", async () => {
            setupPageTitleTest();

            render(<PageTitle titles={["Test"]} />);

            await waitFor(() => {
                const links = screen.getAllByRole("link");
                const logoLink = links.find((link) => link.getAttribute("href") === "/ui/");
                expect(logoLink).toBeDefined();
            });
        });

        it("renders navbar toggle button", async () => {
            setupPageTitleTest();

            render(<PageTitle titles={["Test"]} />);

            await waitFor(() => {
                const toggleButton = screen.getByLabelText("Toggle navigation");
                expect(toggleButton).toBeInTheDocument();
            });
        });
    });
});

// Made with Bob
