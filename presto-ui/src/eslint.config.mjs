import ftFlow from "eslint-plugin-ft-flow";
import js from "@eslint/js";
import hermes from "hermes-eslint";
import globals from "globals";
import eslintPluginPrettierRecommended from "eslint-plugin-prettier/recommended";
import react from "eslint-plugin-react";
import reactHooks from "eslint-plugin-react-hooks";

export default [
  js.configs.recommended,
  eslintPluginPrettierRecommended,
  {
    // Note: there should be no other properties in this object
    ignores: [
      "**/vendor/**",
      "**/node_modules/**",
      "**/sql-parser/**",
      "webpack.config.js",
    ],
  },
  // Flow stuff
  {
    languageOptions: {
      parser: hermes,
    },
    plugins: {
      ftFlow,
    },
    settings: {
      flowtype: {
        onlyFilesWithFlowAnnotation: true,
      },
    },
  },
  // React stuff
  {
    languageOptions: {
      globals: {
        ...globals.browser,
        ...globals.jquery,
      },
    },
    plugins: {
      react,
      "react-hooks": reactHooks,
    },
    rules: {
      ...react.configs.recommended.rules,
      // ...prettier.rules,
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
  // Prettier Stuff
  {},
];
