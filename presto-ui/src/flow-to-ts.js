#!/usr/bin/env node

/**
 * Flow to TypeScript Migration Helper Script
 *
 * This script helps convert Flow files to TypeScript by:
 * 1. Renaming .js/.jsx files to .ts/.tsx
 * 2. Performing common type conversion patterns
 * 3. Updating imports
 *
 * Usage: node flow-to-ts.js <file-path>
 * Example: node flow-to-ts.js components/MyComponent.jsx
 */

const fs = require("fs");
const path = require("path");

/**
 * Convert Flow type annotations to TypeScript
 */
function convertFlowToTypeScript(content) {
    let result = content;

    // Remove @flow comment
    result = result.replace(/\/\/\s*@flow\s*\n/g, "");

    // Convert optional types: ?Type -> Type | null | undefined
    // Handle simple cases like ?string, ?number, ?boolean
    result = result.replace(/:\s*\?([A-Z][a-zA-Z0-9_<>[\]]*)/g, ": $1 | null | undefined");
    result = result.replace(/:\s*\?([a-z][a-z]*)/g, ": $1 | null | undefined");

    // Convert Type | void -> Type | undefined
    result = result.replace(/\|\s*void\b/g, "| undefined");

    // Convert $ReadOnly<T> -> Readonly<T>
    result = result.replace(/\$ReadOnly</g, "Readonly<");

    // Convert $ReadOnlyArray<T> -> ReadonlyArray<T>
    result = result.replace(/\$ReadOnlyArray</g, "ReadonlyArray<");

    // Convert $Keys<T> -> keyof T
    result = result.replace(/\$Keys<([^>]+)>/g, "keyof $1");

    // Convert $Values<T> -> T[keyof T]
    result = result.replace(/\$Values<([^>]+)>/g, "$1[keyof $1]");

    // Convert exact object types: {| ... |} -> { ... }
    result = result.replace(/\{\|/g, "{");
    result = result.replace(/\|\}/g, "}");

    // Convert Flow utility types
    result = result.replace(/\$Shape</g, "Partial<");
    result = result.replace(/\$Diff<([^,]+),\s*([^>]+)>/g, "Omit<$1, keyof $2>");
    result = result.replace(/\$PropertyType<([^,]+),\s*'([^']+)'>/g, "$1['$2']");

    // Convert Flow built-in types to TypeScript equivalents
    // Use number for browser timers (setTimeout/setInterval return number in browsers)
    result = result.replace(/\bTimeoutID\b/g, "number");
    result = result.replace(/\bIntervalID\b/g, "number");
    result = result.replace(/\bAnimationFrameID\b/g, "number");

    // Convert type ... = to type ... =
    // (No change needed, just ensure consistency)

    // Convert function type syntax: (param: Type) => ReturnType
    // This is already compatible, but we can ensure arrow function types are clean

    // Convert class property types (already compatible)

    // Convert React.Component<Props, State> (already compatible)

    // Handle mixed type
    result = result.replace(/:\s*mixed\b/g, ": unknown");

    return result;
}

/**
 * Determine if a file should be renamed to .ts or .tsx
 */
function shouldBeTsx(content) {
    // Check if file contains JSX syntax
    return /(<[A-Z][a-zA-Z0-9]*[\s\/>])|(<\/[A-Z][a-zA-Z0-9]*>)/.test(content);
}

/**
 * Convert a single file from Flow to TypeScript
 */
function convertFile(filePath) {
    if (!fs.existsSync(filePath)) {
        console.error(`Error: File not found: ${filePath}`);
        process.exit(1);
    }

    const ext = path.extname(filePath);
    if (![".js", ".jsx"].includes(ext)) {
        console.error(`Error: File must be .js or .jsx file: ${filePath}`);
        process.exit(1);
    }

    console.log(`Converting ${filePath}...`);

    // Read file content
    const content = fs.readFileSync(filePath, "utf8");

    // Convert Flow types to TypeScript
    const convertedContent = convertFlowToTypeScript(content);

    // Determine new extension
    const isTsx = shouldBeTsx(convertedContent);
    const newExt = isTsx ? ".tsx" : ".ts";
    const newFilePath = filePath.replace(/\.(js|jsx)$/, newExt);

    // Write converted content to new file
    fs.writeFileSync(newFilePath, convertedContent);

    // Delete old file if new file is different
    if (newFilePath !== filePath) {
        fs.unlinkSync(filePath);
    }

    console.log(`✓ Converted to ${newFilePath}`);
    console.log(`\nNote: Please review the converted file and fix any remaining issues:`);
    console.log(`  - Check for any type errors: yarn typecheck`);
    console.log(`  - Verify imports are correct`);
    console.log(`  - Fix any complex type conversions that weren't handled automatically`);
}

// Main execution
if (require.main === module) {
    const args = process.argv.slice(2);

    if (args.length === 0) {
        console.log("Usage: node flow-to-ts.js <file-path>");
        console.log("Example: node flow-to-ts.js components/MyComponent.jsx");
        process.exit(1);
    }

    const filePath = args[0];
    convertFile(filePath);
}

module.exports = { convertFlowToTypeScript, convertFile };
