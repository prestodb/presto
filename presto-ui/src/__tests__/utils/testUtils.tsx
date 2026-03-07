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

/**
 * Enhanced Test Utilities
 * Provides common helpers for testing React components
 */

import { waitFor, screen, fireEvent } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

// ============================================================================
// Loading State Helpers
// ============================================================================

/**
 * Wait for loading indicator to disappear
 * @example
 * render(<Component />);
 * await waitForLoadingToFinish();
 */
export const waitForLoadingToFinish = () =>
    waitFor(() => expect(screen.queryByText(/loading/i)).not.toBeInTheDocument());

/**
 * Assert that loading indicator is present
 * @example
 * render(<Component />);
 * expectLoading();
 */
export const expectLoading = () => expect(screen.getByText(/loading/i)).toBeInTheDocument();

// ============================================================================
// Text Content Matchers
// ============================================================================

/**
 * Find element by exact text content (async)
 * Useful when text is split across multiple elements
 * @example
 * const element = await findByTextContent('Total: 100');
 */
export const findByTextContent = (text: string) =>
    screen.findByText((content, element) => {
        return element?.textContent === text;
    });

/**
 * Get element by exact text content (sync)
 * @example
 * const element = getByTextContent('Total: 100');
 */
export const getByTextContent = (text: string) =>
    screen.getByText((content, element) => {
        return element?.textContent === text;
    });

/**
 * Query element by exact text content (returns null if not found)
 * @example
 * const element = queryByTextContent('Total: 100');
 * expect(element).toBeNull();
 */
export const queryByTextContent = (text: string) =>
    screen.queryByText((content, element) => {
        return element?.textContent === text;
    });

// ============================================================================
// Dropdown Interaction Helpers
// ============================================================================

/**
 * Click a dropdown and select an option by text
 * @example
 * await clickDropdownOption('Sort By', 'Name');
 */
export const clickDropdownOption = async (dropdownText: string, optionText: string) => {
    const dropdown = screen.getByText(dropdownText);
    await userEvent.click(dropdown);
    const option = screen.getByText(optionText);
    await userEvent.click(option);
};

/**
 * Select dropdown option by role
 * @example
 * await selectDropdownByRole('combobox', 'Option 1');
 */
export const selectDropdownByRole = async (role: string, optionText: string) => {
    const dropdown = screen.getByRole(role);
    await userEvent.click(dropdown);
    const option = screen.getByText(optionText);
    await userEvent.click(option);
};

// ============================================================================
// Form Interaction Helpers
// ============================================================================

/**
 * Type into an input field by placeholder (uses userEvent for realistic typing)
 * @example
 * await typeIntoInput(/search/i, 'test query');
 */
export const typeIntoInput = async (placeholder: string | RegExp, text: string) => {
    const input = screen.getByPlaceholderText(placeholder);
    await userEvent.clear(input);
    await userEvent.type(input, text);
    return input;
};

/**
 * Set input value directly (faster, uses fireEvent.change)
 * Use for tests that don't need realistic typing simulation
 * @example
 * setInputValue(/search/i, 'test query');
 */
export const setInputValue = (placeholder: string | RegExp, value: string) => {
    const input = screen.getByPlaceholderText(placeholder);
    fireEvent.change(input, { target: { value } });
    return input;
};

/**
 * Click a button by accessible name (uses userEvent for realistic interaction)
 * @example
 * await clickButton(/submit/i);
 */
export const clickButton = async (text: string | RegExp) => {
    const button = screen.getByRole("button", { name: text });
    await userEvent.click(button);
    return button;
};

/**
 * Click a button synchronously (uses fireEvent, faster)
 * Use for tests that don't need realistic click simulation
 * @example
 * clickButtonSync(/submit/i);
 */
export const clickButtonSync = (text: string | RegExp) => {
    const button = screen.getByRole("button", { name: text });
    fireEvent.click(button);
    return button;
};

/**
 * Type into input without clearing first
 * @example
 * await appendToInput(/search/i, ' more text');
 */
export const appendToInput = async (placeholder: string | RegExp, text: string) => {
    const input = screen.getByPlaceholderText(placeholder);
    await userEvent.type(input, text);
    return input;
};

// ============================================================================
// Timer Helpers
// ============================================================================

/**
 * Advance timers and wait for updates
 * Useful for debounced operations
 *
 * IMPORTANT: Requires Jest fake timers to be enabled.
 * Use setupFakeTimers() or setupIntegrationTest() in your test suite.
 *
 * @param ms - Milliseconds to advance
 * @throws Error with helpful message if fake timers are not enabled
 * @example
 * describe('MyComponent', () => {
 *     setupFakeTimers(); // or setupIntegrationTest()
 *
 *     it('handles debounced input', async () => {
 *         await typeIntoInput(/search/i, 'test');
 *         await advanceTimersAndWait(300); // Wait for debounce
 *     });
 * });
 */
export const advanceTimersAndWait = async (ms: number) => {
    try {
        jest.advanceTimersByTime(ms);
    } catch (error) {
        // Provide a clearer error when fake timers are not enabled
        const originalMessage = error instanceof Error ? error.message : String(error);
        throw new Error(
            [
                "advanceTimersAndWait requires Jest fake timers to be enabled.",
                "Make sure to call setupFakeTimers() or setupIntegrationTest() before using this helper.",
                `Original Jest error: ${originalMessage}`,
            ].join(" ")
        );
    }
    await waitFor(() => {}, { timeout: 0 });
};

/**
 * Simulate user typing with debounce
 * @example
 * const input = screen.getByPlaceholderText(/search/i);
 * await typeWithDebounce(input, 'test', 300);
 */
export const typeWithDebounce = async (input: HTMLElement, text: string, debounceMs = 300) => {
    await userEvent.type(input, text);
    jest.advanceTimersByTime(debounceMs);
    await waitFor(() => {}, { timeout: 0 });
};

// ============================================================================
// Interaction Helpers
// ============================================================================

/**
 * Click element and wait for specific text to appear
 * @example
 * await clickAndWait(button, /success/i);
 */
export const clickAndWait = async (element: HTMLElement, waitForText?: string | RegExp) => {
    await userEvent.click(element);
    if (waitForText) {
        await screen.findByText(waitForText);
    }
};

/**
 * Click element and wait for it to disappear
 * @example
 * await clickAndWaitForRemoval(deleteButton);
 */
export const clickAndWaitForRemoval = async (element: HTMLElement) => {
    await userEvent.click(element);
    await waitFor(() => expect(element).not.toBeInTheDocument());
};

// ============================================================================
// Query Helpers
// ============================================================================

/**
 * Get all elements matching text pattern
 * @example
 * const items = getAllByTextPattern(/query_\d+/);
 */
export const getAllByTextPattern = (pattern: RegExp) => screen.getAllByText(pattern);

/**
 * Check if element with text exists
 * @example
 * expect(hasText('Success')).toBe(true);
 */
export const hasText = (text: string | RegExp) => screen.queryByText(text) !== null;

// ============================================================================
// Exports
// ============================================================================

// Export all from @testing-library/react
export * from "@testing-library/react";

// Export userEvent for convenience
export { userEvent };

// Made with Bob
