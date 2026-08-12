import js from "@eslint/js";
import globals from "globals";
import prettierEslint from "eslint-plugin-prettier/recommended";
import reactPlugin from "@eslint-react/eslint-plugin";
import reactHooks from "eslint-plugin-react-hooks";
import tseslint from "@typescript-eslint/eslint-plugin";
import tsparser from "@typescript-eslint/parser";

export default [
    js.configs.recommended,
    {
        files: ["**/*.{jsx,tsx}"],
        ...reactPlugin.configs.recommended,
        rules: {
            ...reactPlugin.configs.recommended.rules,
            "@eslint-react/no-missing-key": "warn",
        },
    },
    {
        files: ["**/*.{js,jsx,ts,tsx}"],
        plugins: {
            "react-hooks": reactHooks,
        },
        rules: {
            "react-hooks/rules-of-hooks": "error",
            "react-hooks/exhaustive-deps": "warn",
        },
    },
    {
        ignores: [
            "**/vendor/**",
            "**/node_modules/**",
            "**/sql-parser/**",
            "webpack.config.js",
            "jest.config.js",
            "coverage/**",
        ],
    },
    {
        languageOptions: {
            globals: {
                ...globals.browser,
                ...globals.jquery,
                hljs: "readonly",
            },
        },
        rules: {
            // pre-existing issues kept as warnings to avoid breaking changes
            "no-prototype-builtins": "warn",
            "no-useless-assignment": "warn",
            // React 17+ uses automatic JSX transform; 'React' import is not needed
            // but many files still have it — ignore until imports are cleaned up
            "no-unused-vars": ["error", { varsIgnorePattern: "^React$" }],
            // new ESLint 10 built-in rule — treat as warning for existing code
            "preserve-caught-error": "warn",
        },
    },
    // React JSX language options
    {
        files: ["**/*.jsx"],
        languageOptions: {
            parserOptions: {
                ecmaVersion: "latest",
                sourceType: "module",
                ecmaFeatures: {
                    jsx: true,
                },
            },
        },
    },
    // TypeScript
    {
        files: ["**/*.ts", "**/*.tsx"],
        languageOptions: {
            parser: tsparser,
            parserOptions: {
                ecmaVersion: "latest",
                sourceType: "module",
                ecmaFeatures: {
                    jsx: true,
                },
            },
        },
        plugins: {
            "@typescript-eslint": tseslint,
        },
        rules: {
            ...tseslint.configs.recommended.rules,
            "no-unused-vars": "off",
            "@typescript-eslint/no-explicit-any": "warn",
            "@typescript-eslint/no-unused-vars": [
                "error",
                {
                    argsIgnorePattern: "^_",
                    varsIgnorePattern: "^_",
                },
            ],
        },
    },
    // Test files
    {
        files: ["**/*.test.{js,jsx,ts,tsx}", "**/*.spec.{js,jsx,ts,tsx}", "**/setupTests.ts", "**/__tests__/**"],
        languageOptions: {
            globals: {
                ...globals.jest,
            },
        },
        rules: {
            "@typescript-eslint/no-explicit-any": "off",
            "no-undef": "off",
        },
    },
    prettierEslint,
];
