import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { builtinModules } from 'node:module';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '..');
const srcDir = path.join(root, 'src');
const entry = path.join(srcDir, 'app.js');
const nodeBuiltins = new Set([
  ...builtinModules,
  ...builtinModules.map((name) => `node:${name}`),
]);
const visited = new Set();
const issues = [];

function extractImports(source) {
  const imports = [];
  const staticImportPattern = /import\s+(?:[^'"]+\s+from\s+)?['"]([^'"]+)['"]/g;
  const dynamicImportPattern = /import\s*\(\s*['"]([^'"]+)['"]\s*\)/g;
  for (const pattern of [staticImportPattern, dynamicImportPattern]) {
    let match;
    while ((match = pattern.exec(source))) imports.push(match[1]);
  }
  return imports;
}

function checkModule(filePath) {
  const normalizedPath = path.normalize(filePath);
  if (visited.has(normalizedPath)) return;
  visited.add(normalizedPath);

  const source = fs.readFileSync(normalizedPath, 'utf8');
  for (const specifier of extractImports(source)) {
    if (nodeBuiltins.has(specifier)) {
      issues.push({
        file: path.relative(root, normalizedPath),
        import: specifier,
        reason: 'Browser code cannot import Node built-ins.',
      });
      continue;
    }

    if (!specifier.startsWith('.')) {
      issues.push({
        file: path.relative(root, normalizedPath),
        import: specifier,
        reason: 'Browser code should not use bare package imports in this no-bundler prototype.',
      });
      continue;
    }

    const resolved = path.normalize(path.resolve(path.dirname(normalizedPath), specifier));
    if (!resolved.startsWith(srcDir)) {
      issues.push({
        file: path.relative(root, normalizedPath),
        import: specifier,
        reason: 'Browser code must stay inside src/; shared Node scripts are not browser-safe.',
      });
      continue;
    }

    checkModule(resolved);
  }
}

checkModule(entry);

const result = {
  checkedAt: new Date().toISOString(),
  entry: path.relative(root, entry),
  visited: [...visited].map((filePath) => path.relative(root, filePath)).sort(),
  passed: issues.length === 0,
  issues,
};

console.log(JSON.stringify(result, null, 2));
if (!result.passed) process.exit(1);
