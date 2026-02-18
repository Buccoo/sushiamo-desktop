#!/usr/bin/env node
import { context, build } from "esbuild";

const watchMode = process.argv.includes("--watch");

const targets = [
  {
    entryPoints: ["electron/main.ts"],
    outfile: "electron-dist/main.cjs",
  },
  {
    entryPoints: ["electron/printWorker.ts"],
    outfile: "electron-dist/printWorker.cjs",
  },
  {
    entryPoints: ["electron/preload.ts"],
    outfile: "electron-dist/preload.cjs",
  },
];

const sharedOptions = {
  bundle: false,
  platform: "node",
  format: "cjs",
  target: "node20",
  sourcemap: true,
  logLevel: "info",
  tsconfig: "tsconfig.electron.json",
};

async function runBuildOnce() {
  await Promise.all(
    targets.map((targetOptions) =>
      build({
        ...sharedOptions,
        ...targetOptions,
      }),
    ),
  );
}

async function runWatch() {
  const contexts = await Promise.all(
    targets.map((targetOptions) =>
      context({
        ...sharedOptions,
        ...targetOptions,
      }),
    ),
  );

  await Promise.all(contexts.map((ctx) => ctx.watch()));
  // Keep process alive in watch mode.
  await new Promise(() => {});
}

if (watchMode) {
  runWatch().catch((error) => {
    console.error("[build-electron] watch failed:", error);
    process.exit(1);
  });
} else {
  runBuildOnce().catch((error) => {
    console.error("[build-electron] build failed:", error);
    process.exit(1);
  });
}
