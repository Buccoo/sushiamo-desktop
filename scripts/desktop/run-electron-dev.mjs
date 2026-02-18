#!/usr/bin/env node
import { spawn } from "node:child_process";
import electron from "electron";

const env = { ...process.env };
delete env.ELECTRON_RUN_AS_NODE;
env.ELECTRON_DEV_SERVER_URL = env.ELECTRON_DEV_SERVER_URL || "http://localhost:8080";

const child = spawn(electron, ["."], {
  stdio: "inherit",
  windowsHide: false,
  env,
});

child.on("close", (code, signal) => {
  if (code == null) {
    console.error("[desktop-dev] Electron exited with signal", signal);
    process.exit(1);
  }
  process.exit(code);
});
