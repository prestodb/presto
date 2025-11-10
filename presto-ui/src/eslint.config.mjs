import js from "@eslint/js";
import globals from "globals";
import prettierEslint from "eslint-plugin-prettier/recommended";
import react from "eslint-plugin-react";
import reactHooks from "eslint-plugin-react-hooks";
import tseslint from "@typescript-eslint/eslint-plugin";
import tsparser from "@typescript-eslint/parser";

export default [
    js.configs.recommended,
    reactHooks.configs["recommended-latest"],
    {
        ignores: ["**/vendor/**", "**/node_modules/**", "**/sql-parser/**", "webpack.config.js"],
    },
    {
        languageOptions: {
            globals: {
                ...globals.browser,
                ...globals.jquery,
                hljs: "readonly",
            },
        },
    },
    // React (JSX files)
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
        plugins: {
            react,
        },
        rules: {
            ...react.configs.recommended.rules,
            ["react/prop-types"]: "warn",
            ["react/no-deprecated"]: "warn",
            ["no-prototype-builtins"]: "warn",
        },
        settings: {
            react: {
                version: "detect",
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
            react,
        },
        rules: {
            ...tseslint.configs.recommended.rules,
            ...react.configs.recommended.rules,
            "@typescript-eslint/no-explicit-any": "warn",
            "@typescript-eslint/no-unused-vars": "warn",
            "react/prop-types": "off", // TypeScript handles prop validation
            "react/react-in-jsx-scope": "off", // Not needed with React 17+
        },
        settings: {
            react: {
                version: "detect",
            },
        },
    },
    prettierEslint,
];
