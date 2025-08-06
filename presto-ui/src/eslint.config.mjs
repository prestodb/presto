import ftFlow from "eslint-plugin-ft-flow";
import js from "@eslint/js";
import hermes from "hermes-eslint";
import globals from "globals";
import prettierEslint from "eslint-plugin-prettier/recommended";
import react from "eslint-plugin-react";
import reactHooks from "eslint-plugin-react-hooks";

export default [
  js.configs.recommended,
  prettierEslint,
  {
    ignores: [
      "**/vendor/**",
      "**/node_modules/**",
      "**/sql-parser/**",
      "webpack.config.js",
    ],
  },
  {
    languageOptions: {
      globals: {
        ...globals.browser,
        ...globals.jquery,
      },
    },
  },
  // Flow
  {
    languageOptions: {
      parser: hermes,
    },
    plugins: {
      "ft-flow": ftFlow,
    },
    settings: {
      flowtype: {
        onlyFilesWithFlowAnnotation: true,
      },
    },
    rules: {
      ...ftFlow.configs.recommended.rules,
    },
  },
  // React
  {
    files: ["**/*.jsx", "**/use*.js"],
    plugins: {
      react,
      "react-hooks": reactHooks,
    },
    rules: {
      ...react.configs.recommended.rules,
      ...reactHooks.configs.recommended.rules,
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
];
