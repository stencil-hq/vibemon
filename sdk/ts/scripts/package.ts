import { copyFile, mkdir, mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const sdkRoot = fileURLToPath(new URL("../", import.meta.url));
const distDir = join(sdkRoot, "dist");
const manifestPath = join(sdkRoot, "package.json");
const expectedPackageName = "@stencil-hq/vibemon";
const expectedFiles = ["dist/**/*.js", "dist/**/*.d.ts", "LICENSE-MIT", "LICENSE-APACHE"];
const expectedExports = [".", "./freestyle", "./functions", "./function-values"];

type ExportTarget = { types: string; import: string };
type PackageManifest = {
  name: string;
  version: string;
  private?: boolean;
  files?: string[];
  exports?: Record<string, ExportTarget>;
  scripts?: Record<string, string>;
};

function fail(message: string): never {
  throw new Error(`package validation failed: ${message}`);
}

async function run(command: string[], cwd = sdkRoot): Promise<void> {
  const child = Bun.spawn(command, {
    cwd,
    stdin: "inherit",
    stdout: "inherit",
    stderr: "inherit",
  });
  const exitCode = await child.exited;
  if (exitCode !== 0) {
    throw new Error(`${command.join(" ")} exited with status ${exitCode}`);
  }
}

async function listFiles(directory: string): Promise<string[]> {
  const files: string[] = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await listFiles(path)));
    } else if (entry.isFile()) {
      files.push(path);
    }
  }
  return files;
}

async function validatePayload(manifest: PackageManifest): Promise<void> {
  if (manifest.name !== expectedPackageName) {
    fail(`package name must be exactly ${expectedPackageName}`);
  }
  if (manifest.private) fail("manifest must not be private");
  if (JSON.stringify(manifest.files) !== JSON.stringify(expectedFiles)) {
    fail(`files must be exactly ${JSON.stringify(expectedFiles)}`);
  }
  if (manifest.scripts && Object.keys(manifest.scripts).some((name) => name.includes("publish"))) {
    fail("publish scripts are not allowed");
  }

  const exports = manifest.exports ?? fail("exports are missing");
  if (JSON.stringify(Object.keys(exports)) !== JSON.stringify(expectedExports)) {
    fail(`exports must be exactly ${expectedExports.join(", ")}`);
  }

  for (const subpath in exports) {
    const target = exports[subpath];
    for (const condition of ["import", "types"] as const) {
      const value = target[condition];
      if (!value?.startsWith("./dist/")) {
        fail(`${subpath} ${condition} must point into dist`);
      }
      if (!(await Bun.file(resolve(sdkRoot, value)).exists())) {
        fail(`${subpath} ${condition} target ${value} was not emitted`);
      }
    }
  }

  const emitted = await listFiles(distDir);
  const unexpected = emitted
    .map((path) => relative(distDir, path).split(sep).join("/"))
    .filter((path) => !path.endsWith(".js") && !path.endsWith(".d.ts"));
  if (unexpected.length > 0) {
    fail(`unexpected dist files: ${unexpected.join(", ")}`);
  }
}

async function validateArchive(archivePath: string): Promise<void> {
  const compressed = new Uint8Array(await Bun.file(archivePath).arrayBuffer());
  const tar = Bun.gunzipSync(compressed);
  const decoder = new TextDecoder();
  const actual: string[] = [];

  for (let offset = 0; offset + 512 <= tar.byteLength; ) {
    const header = tar.subarray(offset, offset + 512);
    if (header.every((byte) => byte === 0)) break;

    const readField = (start: number, end: number): string =>
      decoder.decode(header.subarray(start, end)).replace(/\0.*$/, "").trim();
    const name = readField(0, 100);
    const prefix = readField(345, 500);
    const size = Number.parseInt(readField(124, 136) || "0", 8);
    if (!Number.isFinite(size)) fail(`invalid tar size for ${name}`);
    const type = header[156];
    if (type === 0 || type === 48) actual.push(prefix ? `${prefix}/${name}` : name);
    offset += 512 + Math.ceil(size / 512) * 512;
  }

  const emitted = (await listFiles(distDir)).map(
    (path) => `package/dist/${relative(distDir, path).split(sep).join("/")}`,
  );
  const expected = [
    "package/package.json",
    "package/LICENSE-MIT",
    "package/LICENSE-APACHE",
    ...emitted,
  ].sort();
  actual.sort();

  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    fail(`packed files differ: expected ${expected.join(", ")}, got ${actual.join(", ")}`);
  }
}

async function validateInstalledPackage(
  archivePath: string,
  manifest: PackageManifest,
): Promise<void> {
  const consumerDirectory = await mkdtemp(join(tmpdir(), "vmon-sdk-consumer-"));
  try {
    await Bun.write(
      join(consumerDirectory, "package.json"),
      `${JSON.stringify({ private: true, type: "module" }, null, 2)}\n`,
    );
    await run(["bun", "add", "--offline", archivePath], consumerDirectory);

    const specifiers = expectedExports.map((subpath) =>
      subpath === "." ? manifest.name : `${manifest.name}/${subpath.slice(2)}`,
    );
    await Bun.write(
      join(consumerDirectory, "validate.mjs"),
      `for (const specifier of ${JSON.stringify(specifiers)}) {
  // The package entry point is selected from the manifest at runtime.
  const namespace = await import(specifier);
  const undefinedExports = [];
  let exportCount = 0;
  for (const name in namespace) {
    exportCount += 1;
    if (namespace[name] === undefined) undefinedExports.push(name);
  }
  if (exportCount === 0) throw new Error(\`\${specifier} has no runtime exports\`);
  if (undefinedExports.length > 0) {
    throw new Error(\`\${specifier} has undefined exports: \${undefinedExports.join(", ")}\`);
  }
}\n`,
    );
    await run(["bun", "run", "validate.mjs"], consumerDirectory);
  } finally {
    await rm(consumerDirectory, { recursive: true, force: true });
  }
}

async function rewriteRelativeModuleSpecifiers(): Promise<void> {
  const javascriptFiles = (await listFiles(distDir)).filter((path) => path.endsWith(".js"));

  for (const path of javascriptFiles) {
    const source = await Bun.file(path).text();
    const rewritten = source.replace(
      /\b(from\s+|import\s*\(\s*|import\s+)(["'])(\.\.?\/[^"']+)\2/g,
      (_match, prefix: string, quote: string, specifier: string) => {
        const javascriptSpecifier = specifier.endsWith(".ts")
          ? `${specifier.slice(0, -3)}.js`
          : /\.[cm]?js$|\.json$|\.node$/.test(specifier)
            ? specifier
            : `${specifier}.js`;
        return `${prefix}${quote}${javascriptSpecifier}${quote}`;
      },
    );
    await Bun.write(path, rewritten);

    for (const match of rewritten.matchAll(
      /\b(?:from\s+|import\s*\(\s*|import\s+)(["'])(\.\.?\/[^"']+)\1/g,
    )) {
      const specifier = match[2];
      if (!specifier.endsWith(".js")) {
        fail(`${relative(distDir, path)} contains non-JavaScript module reference ${specifier}`);
      }
      if (!(await Bun.file(resolve(dirname(path), specifier)).exists())) {
        fail(`${relative(distDir, path)} references missing module ${specifier}`);
      }
    }
  }
}

async function build(manifest: PackageManifest): Promise<void> {
  await rm(distDir, { recursive: true, force: true });
  await mkdir(distDir, { recursive: true });

  const tsc = join(sdkRoot, "node_modules", ".bin", process.platform === "win32" ? "tsc.cmd" : "tsc");
  if (!(await Bun.file(tsc).exists())) {
    throw new Error("local TypeScript compiler not found; run `bun install --frozen-lockfile` first");
  }
  await run([tsc, "-p", "tsconfig.build.json"]);
  await rewriteRelativeModuleSpecifiers();
  await validatePayload(manifest);
}

async function pack(manifest: PackageManifest): Promise<string> {
  const temporaryDirectory = await mkdtemp(join(tmpdir(), "vmon-sdk-pack-"));
  try {
    await run(["bun", "pm", "pack", "--destination", temporaryDirectory]);
    const archives = (await readdir(temporaryDirectory)).filter((name) => name.endsWith(".tgz"));
    if (archives.length !== 1) {
      throw new Error(`expected Bun to produce one tarball, found ${archives.length}`);
    }

    const packedArchive = join(temporaryDirectory, archives[0]);
    await validateArchive(packedArchive);
    await validateInstalledPackage(packedArchive, manifest);

    const packageDirectory = join(distDir, "package");
    const artifactName = `${manifest.name.replace(/^@/, "").replaceAll("/", "-")}-${manifest.version}.tgz`;
    const artifactPath = join(packageDirectory, artifactName);
    await mkdir(packageDirectory, { recursive: true });
    await copyFile(packedArchive, artifactPath);
    return artifactPath;
  } finally {
    await rm(temporaryDirectory, { recursive: true, force: true });
  }
}

const manifest = (await Bun.file(manifestPath).json()) as PackageManifest;
await build(manifest);

if (!process.argv.includes("--build-only")) {
  const artifactPath = await pack(manifest);
  console.log(relative(sdkRoot, artifactPath).split(sep).join("/"));
}
