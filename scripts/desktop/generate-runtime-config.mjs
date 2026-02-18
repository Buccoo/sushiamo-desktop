#!/usr/bin/env node
import fs from "node:fs/promises";
import path from "node:path";

const ROOT = process.cwd();
const OUTPUT_PATH = path.join(ROOT, "electron", "runtime-config.json");

function parseEnvFile(raw) {
  const env = {};
  for (const line of String(raw).split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const idx = trimmed.indexOf("=");
    if (idx <= 0) continue;
    const key = trimmed.slice(0, idx).trim();
    let value = trimmed.slice(idx + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    env[key] = value;
  }
  return env;
}

async function readEnvFiles() {
  const merged = {};
  const files = [".env", ".env.local", ".env.desktop", ".env.desktop.local"];
  for (const file of files) {
    const fullPath = path.join(ROOT, file);
    try {
      const raw = await fs.readFile(fullPath, "utf8");
      Object.assign(merged, parseEnvFile(raw));
    } catch {
      // optional file
    }
  }
  return merged;
}

async function main() {
  const fromFiles = await readEnvFiles();
  const env = { ...fromFiles, ...process.env };

  const config = {
    VITE_SUPABASE_URL: String(env.VITE_SUPABASE_URL || "").trim(),
    VITE_SUPABASE_ANON_KEY: String(env.VITE_SUPABASE_ANON_KEY || "").trim(),
    VITE_ROUTER_MODE: String(env.VITE_ROUTER_MODE || "hash").trim() || "hash",
    SUSHIAMO_DESKTOP_ENABLE_AUTO_UPDATE:
      String(env.SUSHIAMO_DESKTOP_ENABLE_AUTO_UPDATE || "1").trim() || "1",
    SUSHIAMO_DESKTOP_UPDATE_URL:
      String(env.SUSHIAMO_DESKTOP_UPDATE_URL || "https://www.sushiamo.app/downloads").trim(),
    PUBLIC_APP_URL: String(env.PUBLIC_APP_URL || env.SUSHIAMO_PUBLIC_APP_URL || "").trim(),
  };

  if (!config.VITE_SUPABASE_URL || !config.VITE_SUPABASE_ANON_KEY) {
    throw new Error(
      "Missing VITE_SUPABASE_URL / VITE_SUPABASE_ANON_KEY. Set them in .env.desktop (or .env).",
    );
  }

  await fs.mkdir(path.dirname(OUTPUT_PATH), { recursive: true });
  await fs.writeFile(OUTPUT_PATH, `${JSON.stringify(config, null, 2)}\n`, "utf8");
  console.log(`[desktop-config] generated ${path.relative(ROOT, OUTPUT_PATH)}`);
}

main().catch((error) => {
  console.error("[desktop-config] failed:", error);
  process.exit(1);
});
