import { contextBridge, ipcRenderer } from "electron";

type RuntimeConfig = {
  VITE_SUPABASE_URL?: string;
  VITE_SUPABASE_ANON_KEY?: string;
  VITE_ROUTER_MODE?: string;
  PUBLIC_APP_URL?: string;
};

type PrinterWorkerConfigPayload = {
  consumerId?: string;
  deviceName?: string;
  pollMs?: number;
  claimLimit?: number;
  autoStart?: boolean;
};

type RendererLikeGlobal = {
  addEventListener?: (type: string, listener: (event: any) => void) => void;
  location?: { href?: string };
  console?: {
    error?: (...args: unknown[]) => void;
    warn?: (...args: unknown[]) => void;
  };
};

function readInjectedConfig(): RuntimeConfig {
  const arg = process.argv.find((value) => value.startsWith("--sushiamo-desktop-config="));
  if (!arg) return {};
  const encoded = arg.slice("--sushiamo-desktop-config=".length);
  if (!encoded) return {};
  try {
    const decoded = Buffer.from(encoded, "base64").toString("utf8");
    const parsed = JSON.parse(decoded) as RuntimeConfig;
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

const runtimeConfig = readInjectedConfig();
const rendererGlobal = globalThis as unknown as RendererLikeGlobal;

function safeStringify(value: unknown) {
  try {
    if (typeof value === "string") return value;
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function emitRendererLog(level: "INFO" | "WARN" | "ERROR", message: string, details?: unknown) {
  try {
    ipcRenderer.send("desktop:renderer-log", {
      level,
      message,
      details,
      href: rendererGlobal.location?.href || null,
      ts: new Date().toISOString(),
    });
  } catch {
    // ignore logging transport errors
  }
}

if (typeof rendererGlobal.addEventListener === "function") {
  rendererGlobal.addEventListener("error", (event: any) => {
    emitRendererLog("ERROR", "window.error", {
      message: event?.message || null,
      filename: event?.filename || null,
      lineno: event?.lineno || null,
      colno: event?.colno || null,
      stack: event?.error?.stack || null,
    });
  });
  rendererGlobal.addEventListener("unhandledrejection", (event: any) => {
    const reason = event?.reason;
    emitRendererLog("ERROR", "window.unhandledrejection", {
      reason: reason instanceof Error ? reason.stack || reason.message : safeStringify(reason),
    });
  });
  rendererGlobal.addEventListener("DOMContentLoaded", () => {
    emitRendererLog("INFO", "dom-content-loaded");
  });
}

const consoleRef = rendererGlobal.console;
if (consoleRef?.error) {
  const originalConsoleError = consoleRef.error.bind(consoleRef);
  consoleRef.error = (...args: unknown[]) => {
    emitRendererLog("ERROR", "console.error", { args: args.map(safeStringify) });
    originalConsoleError(...args);
  };
}
if (consoleRef?.warn) {
  const originalConsoleWarn = consoleRef.warn.bind(consoleRef);
  consoleRef.warn = (...args: unknown[]) => {
    emitRendererLog("WARN", "console.warn", { args: args.map(safeStringify) });
    originalConsoleWarn(...args);
  };
}

contextBridge.exposeInMainWorld("__SUSHIAMO_DESKTOP_CONFIG__", runtimeConfig);

contextBridge.exposeInMainWorld("desktopShell", {
  isDesktop: true,
  platform: process.platform,
  getRuntimeConfig: () => ({ ...runtimeConfig }),
  getDebugInfo: () => ipcRenderer.invoke("desktop:get-debug-info"),
  openLogFolder: () => ipcRenderer.invoke("desktop:open-log-folder"),
  refocusWindow: () => ipcRenderer.invoke("desktop:refocus-window"),
  reloadApp: () => ipcRenderer.invoke("desktop:reload-app"),
  getSavedCredentials: () => ipcRenderer.invoke("desktop:credentials:get"),
  saveCredentials: (payload: { email?: unknown; password?: unknown }) =>
    ipcRenderer.invoke("desktop:credentials:set", payload || {}),
  clearSavedCredentials: () => ipcRenderer.invoke("desktop:credentials:clear"),
  getUpdateState: () => ipcRenderer.invoke("desktop:get-update-state"),
  checkForUpdates: () => ipcRenderer.invoke("desktop:check-for-updates"),
  downloadUpdate: () => ipcRenderer.invoke("desktop:download-update"),
  installUpdate: () => ipcRenderer.invoke("desktop:install-update"),
  printer: {
    getState: () => ipcRenderer.invoke("desktop:printer:get-state"),
    saveConfig: (payload: PrinterWorkerConfigPayload) => ipcRenderer.invoke("desktop:printer:save-config", payload || {}),
    syncSession: (session: unknown) => ipcRenderer.invoke("desktop:printer:sync-session", session || null),
    clearSession: () => ipcRenderer.invoke("desktop:printer:clear-session"),
    start: () => ipcRenderer.invoke("desktop:printer:start"),
    stop: () => ipcRenderer.invoke("desktop:printer:stop"),
    discover: (timeoutMs?: number) =>
      ipcRenderer.invoke(
        "desktop:printer:discover",
        Number.isFinite(Number(timeoutMs)) ? { timeoutMs: Number(timeoutMs) } : {},
      ),
    discoverRt: (timeoutMs?: number) =>
      ipcRenderer.invoke(
        "desktop:printer:discover-rt",
        Number.isFinite(Number(timeoutMs)) ? { timeoutMs: Number(timeoutMs) } : {},
      ),
    testRtReceipt: (config: { host: string; port: number; brand: string; api_path: string }) =>
      ipcRenderer.invoke("desktop:printer:test-rt-receipt", config || {}),
    onState: (callback: (state: unknown) => void) => {
      if (typeof callback !== "function") return () => {};
      const handler = (_event: unknown, state: unknown) => callback(state);
      ipcRenderer.on("desktop:printer-state", handler);
      return () => ipcRenderer.removeListener("desktop:printer-state", handler);
    },
    onLog: (callback: (row: unknown) => void) => {
      if (typeof callback !== "function") return () => {};
      const handler = (_event: unknown, row: unknown) => callback(row);
      ipcRenderer.on("desktop:printer-log", handler);
      return () => ipcRenderer.removeListener("desktop:printer-log", handler);
    },
  },
  onDeepLink: (callback: (route: string) => void) => {
    if (typeof callback !== "function") {
      return () => {};
    }
    const handler = (_event: unknown, route: string) => callback(route);
    ipcRenderer.on("desktop:deep-link", handler);
    return () => ipcRenderer.removeListener("desktop:deep-link", handler);
  },
  onUpdateEvent: (callback: (payload: unknown) => void) => {
    if (typeof callback !== "function") {
      return () => {};
    }
    const handler = (_event: unknown, payload: unknown) => callback(payload);
    ipcRenderer.on("desktop:update-event", handler);
    return () => ipcRenderer.removeListener("desktop:update-event", handler);
  },
});
