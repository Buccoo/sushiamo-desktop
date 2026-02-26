import { app, BrowserWindow, ipcMain, Menu, shell } from "electron";
import { autoUpdater } from "electron-updater";
import fs from "node:fs";
import path from "node:path";
import type { DesktopPrintWorker as DesktopPrintWorkerType } from "./printWorker";

const { DesktopPrintWorker } = require("./printWorker.cjs") as {
  DesktopPrintWorker: typeof DesktopPrintWorkerType;
};

type DesktopRuntimeConfig = {
  VITE_SUPABASE_URL: string;
  VITE_SUPABASE_ANON_KEY: string;
  VITE_ROUTER_MODE: "browser" | "hash" | string;
  SUSHIAMO_DESKTOP_ENABLE_AUTO_UPDATE: "0" | "1" | string;
  SUSHIAMO_DESKTOP_UPDATE_URL?: string;
  PUBLIC_APP_URL?: string;
};

type DesktopUpdateEventPayload = {
  type:
    | "checking-for-update"
    | "update-available"
    | "update-not-available"
    | "download-progress"
    | "update-downloaded"
    | "error";
  data?: Record<string, unknown>;
};

type WindowState = {
  width: number;
  height: number;
  x?: number;
  y?: number;
  isMaximized?: boolean;
};

type SavedDesktopAccount = {
  email: string;
  password: string;
  displayName?: string;
  updatedAt: string;
};

type SavedDesktopAccountsFile = {
  accounts: SavedDesktopAccount[];
};

const DESKTOP_PROTOCOL = "sushiamo";
const WINDOW_STATE_FILE = "window-state.json";
const CREDENTIALS_FILE = "desktop-credentials.json";
const LOG_DIR = "logs";
const LOG_FILE = "desktop-warn-error.log";
const DEV_SERVER_URL = process.env.ELECTRON_DEV_SERVER_URL;
const WINDOW_ICON_NAME = "favicon.png";
const DEFAULT_UPDATE_CHECK_INTERVAL_MS = 10 * 60 * 1000;
const MIN_UPDATE_CHECK_INTERVAL_MS = 2 * 60 * 1000;

let mainWindow: BrowserWindow | null = null;
let pendingDeepLinkRoute: string | null = null;
let resolvedLogFilePath: string | null = null;
let didResolveLogFilePath = false;
let rendererRetryAttempts = 0;
const MAX_RENDERER_RETRY_ATTEMPTS = 4;

const runtimeConfig = loadDesktopRuntimeConfig();
let printWorker: InstanceType<typeof DesktopPrintWorker> | null = null;
let desktopAutoUpdateEnabled = false;
let desktopUpdateDownloaded = false;
let desktopUpdateAvailable = false;
let desktopUpdateInfo: { version?: string | null; releaseName?: string | null; releaseDate?: string | null } | null = null;
let desktopUpdateCheckInProgress = false;
let desktopUpdateListenersBound = false;
let desktopUpdateFeedCandidates: string[] = [];
let desktopUpdateFeedIndex = 0;
let desktopUpdateIntervalHandle: NodeJS.Timeout | null = null;

function getCredentialsPath() {
  return path.join(app.getPath("userData"), CREDENTIALS_FILE);
}

async function readSavedAccounts(): Promise<SavedDesktopAccount[]> {
  try {
    const raw = await fs.promises.readFile(getCredentialsPath(), "utf8");
    const parsed = JSON.parse(raw) as unknown;

    // Migrate old single-credential format { email, password, updatedAt }
    if (parsed && typeof parsed === "object" && !Array.isArray((parsed as Record<string, unknown>).accounts)) {
      const old = parsed as Partial<SavedDesktopAccount>;
      const email = String(old.email || "").trim();
      const password = String(old.password || "");
      if (email && password) {
        const migrated: SavedDesktopAccountsFile = {
          accounts: [{ email, password, updatedAt: old.updatedAt || new Date().toISOString() }],
        };
        await fs.promises.writeFile(getCredentialsPath(), `${JSON.stringify(migrated, null, 2)}\n`, "utf8");
        return migrated.accounts;
      }
      return [];
    }

    const file = parsed as SavedDesktopAccountsFile;
    if (!Array.isArray(file.accounts)) return [];
    return file.accounts.filter(
      (a) => typeof a.email === "string" && a.email.trim() && typeof a.password === "string" && a.password,
    );
  } catch {
    return [];
  }
}

async function upsertSavedAccount(payload: { email?: unknown; password?: unknown; displayName?: unknown }) {
  const email = String(payload.email || "").trim();
  const password = String(payload.password || "");
  if (!email || !password) throw new Error("INVALID_CREDENTIALS_PAYLOAD");

  const existing = await readSavedAccounts();
  const newAccount: SavedDesktopAccount = {
    email,
    password,
    ...(payload.displayName ? { displayName: String(payload.displayName) } : {}),
    updatedAt: new Date().toISOString(),
  };
  // Most recently used goes first; remove any existing entry for this email
  const updated = [newAccount, ...existing.filter((a) => a.email !== email)];
  const file: SavedDesktopAccountsFile = { accounts: updated };
  await fs.promises.mkdir(path.dirname(getCredentialsPath()), { recursive: true });
  await fs.promises.writeFile(getCredentialsPath(), `${JSON.stringify(file, null, 2)}\n`, "utf8");
  return { ok: true as const };
}

async function removeSavedAccount(email: string) {
  const existing = await readSavedAccounts();
  const updated = existing.filter((a) => a.email !== email);
  const file: SavedDesktopAccountsFile = { accounts: updated };
  await fs.promises.mkdir(path.dirname(getCredentialsPath()), { recursive: true });
  await fs.promises.writeFile(getCredentialsPath(), `${JSON.stringify(file, null, 2)}\n`, "utf8");
  return { ok: true as const };
}

async function clearAllSavedAccounts() {
  try {
    await fs.promises.unlink(getCredentialsPath());
  } catch {
    // ignore
  }
  return { ok: true as const };
}

function ensureWritableLogFile(filePath: string) {
  try {
    const dir = path.dirname(filePath);
    fs.mkdirSync(dir, { recursive: true });
    fs.appendFileSync(filePath, "", "utf8");
    return true;
  } catch {
    return false;
  }
}

function resolveInstallLogFilePath() {
  if (!app.isPackaged) return null;
  try {
    const installDir = path.dirname(process.execPath);
    return path.join(installDir, LOG_DIR, LOG_FILE);
  } catch {
    return null;
  }
}

function resolveUserDataLogFilePath() {
  try {
    const basePath = app.getPath("userData");
    return path.join(basePath, LOG_DIR, LOG_FILE);
  } catch {
    return null;
  }
}

function getLogFilePath() {
  if (didResolveLogFilePath) return resolvedLogFilePath;

  const candidates = [resolveInstallLogFilePath(), resolveUserDataLogFilePath()].filter(
    (value): value is string => Boolean(value),
  );

  for (const candidate of candidates) {
    if (!ensureWritableLogFile(candidate)) continue;
    resolvedLogFilePath = candidate;
    didResolveLogFilePath = true;
    return candidate;
  }

  if (app.isReady()) {
    didResolveLogFilePath = true;
  }
  return null;
}

function log(level: "INFO" | "WARN" | "ERROR", message: string, details?: unknown) {
  const timestamp = new Date().toISOString();
  let extra = "";
  if (details != null) {
    if (typeof details === "string") {
      extra = ` ${details}`;
    } else {
      try {
        extra = ` ${JSON.stringify(details)}`;
      } catch {
        extra = " [unserializable-details]";
      }
    }
  }
  const line = `[${timestamp}] [${level}] ${message}${extra}`;
  if (level === "ERROR") {
    console.error(line);
  } else if (level === "WARN") {
    console.warn(line);
  } else {
    console.log(line);
  }

  if (level === "INFO") return;

  const logFile = getLogFilePath();
  if (!logFile) return;
  try {
    fs.appendFileSync(logFile, `${line}\n`, "utf8");
  } catch {
    // ignore log write failures
  }
}

function resolveWindowIconPath() {
  const packaged = path.join(app.getAppPath(), "dist", WINDOW_ICON_NAME);
  if (fs.existsSync(packaged)) return packaged;

  const dev = path.resolve(process.cwd(), "public", WINDOW_ICON_NAME);
  if (fs.existsSync(dev)) return dev;

  return undefined;
}

function mapConsoleLevel(level: number): "INFO" | "WARN" | "ERROR" {
  if (level >= 2) return "ERROR";
  if (level === 1) return "WARN";
  return "INFO";
}

function emitDesktopUpdateEvent(payload: DesktopUpdateEventPayload) {
  if (!mainWindow || mainWindow.isDestroyed()) return;
  try {
    mainWindow.webContents.send("desktop:update-event", payload);
  } catch {
    // ignore renderer emit failures
  }
}

function escapeHtml(value: string) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

async function showRendererFallback(win: BrowserWindow, reason: string, details?: Record<string, unknown>) {
  if (!win || win.isDestroyed()) return;
  const logPath = getLogFilePath() || "N/A";
  const payload = details ? JSON.stringify(details, null, 2) : "";
  const html = `
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>SushiAMO Desktop - Errore caricamento</title>
  <style>
    body { font-family: Segoe UI, Arial, sans-serif; background: #0b1220; color: #eef2ff; margin: 0; padding: 20px; }
    .card { max-width: 920px; margin: 20px auto; background: #111a2b; border: 1px solid #24314f; border-radius: 12px; padding: 18px; }
    h1 { font-size: 20px; margin: 0 0 8px 0; }
    .muted { color: #9db0d7; font-size: 13px; margin-bottom: 12px; }
    pre { background: #0a1324; border: 1px solid #223357; border-radius: 8px; padding: 10px; white-space: pre-wrap; word-break: break-word; }
    .btn { margin-top: 12px; display: inline-block; border: 1px solid #2d3f65; border-radius: 8px; padding: 8px 12px; color: #eef2ff; text-decoration: none; }
  </style>
</head>
<body>
  <div class="card">
    <h1>SushiAMO Desktop: errore caricamento</h1>
    <div class="muted">La finestra principale non si e caricata correttamente.</div>
    <div><strong>Motivo:</strong> ${escapeHtml(reason)}</div>
    <div style="margin-top:8px;"><strong>Log file:</strong> ${escapeHtml(logPath)}</div>
    ${payload ? `<pre>${escapeHtml(payload)}</pre>` : ""}
    <a class="btn" href="#" onclick="if(window.desktopShell&&window.desktopShell.reloadApp){window.desktopShell.reloadApp();}else{window.location.reload();} return false;">Riprova caricamento</a>
  </div>
</body>
</html>`;
  await win.loadURL(`data:text/html;charset=utf-8,${encodeURIComponent(html)}`);
}

function isDev() {
  return !app.isPackaged;
}

function getWindowStatePath() {
  return path.join(app.getPath("userData"), WINDOW_STATE_FILE);
}

function readWindowState(): WindowState {
  const defaults: WindowState = { width: 1280, height: 860 };
  try {
    const raw = fs.readFileSync(getWindowStatePath(), "utf8");
    const parsed = JSON.parse(raw) as WindowState;
    return {
      width: Number(parsed.width) || defaults.width,
      height: Number(parsed.height) || defaults.height,
      x: Number.isFinite(parsed.x) ? parsed.x : undefined,
      y: Number.isFinite(parsed.y) ? parsed.y : undefined,
      isMaximized: Boolean(parsed.isMaximized),
    };
  } catch {
    return defaults;
  }
}

function writeWindowState(win: BrowserWindow) {
  try {
    const bounds = win.getBounds();
    const payload: WindowState = {
      ...bounds,
      isMaximized: win.isMaximized(),
    };
    fs.writeFileSync(getWindowStatePath(), JSON.stringify(payload, null, 2), "utf8");
  } catch {
    // ignore state persistence errors
  }
}

function loadDesktopRuntimeConfig(): DesktopRuntimeConfig {
  const fallback: DesktopRuntimeConfig = {
    VITE_SUPABASE_URL: String(process.env.VITE_SUPABASE_URL || "").trim(),
    VITE_SUPABASE_ANON_KEY: String(process.env.VITE_SUPABASE_ANON_KEY || "").trim(),
    VITE_ROUTER_MODE: String(process.env.VITE_ROUTER_MODE || "hash").trim() || "hash",
    SUSHIAMO_DESKTOP_ENABLE_AUTO_UPDATE:
      String(process.env.SUSHIAMO_DESKTOP_ENABLE_AUTO_UPDATE || "0").trim() || "0",
    SUSHIAMO_DESKTOP_UPDATE_URL: String(process.env.SUSHIAMO_DESKTOP_UPDATE_URL || "").trim(),
    PUBLIC_APP_URL: String(process.env.PUBLIC_APP_URL || process.env.SUSHIAMO_PUBLIC_APP_URL || "").trim(),
  };

  const candidates = app.isPackaged
    ? [path.join(process.resourcesPath, "desktop-runtime-config.json")]
    : [
        path.resolve(process.cwd(), "electron", "runtime-config.json"),
        path.resolve(__dirname, "..", "electron", "runtime-config.json"),
      ];

  for (const candidate of candidates) {
    try {
      if (!fs.existsSync(candidate)) continue;
      const raw = fs.readFileSync(candidate, "utf8");
      const parsed = JSON.parse(raw) as Partial<DesktopRuntimeConfig>;
      return {
        ...fallback,
        ...parsed,
      };
    } catch {
      // keep fallback
    }
  }

  return fallback;
}

function getInjectedRendererConfig() {
  return {
    VITE_SUPABASE_URL: runtimeConfig.VITE_SUPABASE_URL,
    VITE_SUPABASE_ANON_KEY: runtimeConfig.VITE_SUPABASE_ANON_KEY,
    VITE_ROUTER_MODE: runtimeConfig.VITE_ROUTER_MODE || "hash",
    PUBLIC_APP_URL: runtimeConfig.PUBLIC_APP_URL || "",
  };
}

function parseDeepLinkRoute(rawUrl: string): string | null {
  try {
    const url = new URL(rawUrl);
    if (url.protocol !== `${DESKTOP_PROTOCOL}:`) return null;

    // Expected: sushiamo://join/<token>
    if (url.hostname.toLowerCase() === "join") {
      const token = url.pathname.split("/").filter(Boolean)[0];
      if (token) return `/join/${encodeURIComponent(token)}`;
      return null;
    }

    const normalized = `${url.hostname ? `/${url.hostname}` : ""}${url.pathname}`.replace(/\/+/g, "/");
    if (!normalized || normalized === "/") return null;
    return normalized;
  } catch {
    return null;
  }
}

function extractDeepLinkFromArgv(argv: string[]): string | null {
  const deepLink = argv.find((arg) => String(arg).startsWith(`${DESKTOP_PROTOCOL}://`));
  return deepLink || null;
}

function sendDeepLinkToRenderer(route: string) {
  if (!mainWindow || mainWindow.isDestroyed()) {
    pendingDeepLinkRoute = route;
    return;
  }
  mainWindow.webContents.send("desktop:deep-link", route);
}

function handleDeepLink(rawUrl: string) {
  const route = parseDeepLinkRoute(rawUrl);
  if (!route) return;
  pendingDeepLinkRoute = route;
  sendDeepLinkToRenderer(route);
}

function registerProtocolHandler() {
  if (app.isPackaged) {
    app.setAsDefaultProtocolClient(DESKTOP_PROTOCOL);
    return;
  }

  // Dev mode registration for Windows.
  if (process.platform === "win32" && process.argv.length >= 2) {
    app.setAsDefaultProtocolClient(DESKTOP_PROTOCOL, process.execPath, [path.resolve(process.argv[1])]);
    return;
  }
  app.setAsDefaultProtocolClient(DESKTOP_PROTOCOL);
}

function setupDevShortcuts(win: BrowserWindow) {
  if (!isDev()) return;
  win.webContents.on("before-input-event", (event, input) => {
    const cmdOrCtrl = input.control || input.meta;
    if (!cmdOrCtrl) return;

    const key = String(input.key || "").toLowerCase();

    if (key === "r" && !input.shift && !input.alt) {
      event.preventDefault();
      win.reload();
      return;
    }

    if (key === "i" && input.shift) {
      event.preventDefault();
      win.webContents.toggleDevTools();
    }
  });
}

function setupApplicationMenu() {
  const appVersion = app.getVersion();
  const isMac = process.platform === "darwin";

  const checkForUpdatesItem: Electron.MenuItemConstructorOptions = {
    label: "Controlla aggiornamenti",
    click: async () => {
      if (!desktopAutoUpdateEnabled) {
        await shell.openExternal("https://github.com/Buccoo/sushiamo-desktop/releases/latest");
        return;
      }
      try {
        desktopUpdateCheckInProgress = true;
        await checkForUpdatesWithFeedFallback();
      } catch (error) {
        desktopUpdateCheckInProgress = false;
        log("WARN", "menu: check for updates failed", error instanceof Error ? error.message : String(error));
      }
    },
  };

  const openLogFolderItem: Electron.MenuItemConstructorOptions = {
    label: "Apri cartella log",
    click: async () => {
      const logPath = getLogFilePath();
      if (logPath) {
        await shell.openPath(path.dirname(logPath));
      }
    },
  };

  const sendLogsItem: Electron.MenuItemConstructorOptions = {
    label: "Invia log al supporto...",
    click: async () => {
      const logPath = getLogFilePath() || "non disponibile";
      const subject = encodeURIComponent(`[SushiAMO Desktop v${appVersion}] Segnalazione problema`);
      const body = encodeURIComponent(
        `Salve,\n\nDescrivi qui il problema riscontrato:\n\n\n---\nVersione app: ${appVersion}\nSistema: ${process.platform} ${process.arch}\nFile di log: ${logPath}\n\nAllega il file di log alla email (aprilo con "Apri cartella log" dal menu Aiuto).`,
      );
      await shell.openExternal(`mailto:sviluppo@sushiamo.app?subject=${subject}&body=${body}`);
    },
  };

  const template: Electron.MenuItemConstructorOptions[] = [
    ...(isMac
      ? [
          {
            label: app.getName(),
            submenu: [
              { label: `Versione ${appVersion}`, enabled: false },
              { type: "separator" as const },
              checkForUpdatesItem,
              { type: "separator" as const },
              { role: "hide" as const },
              { role: "hideOthers" as const },
              { role: "unhide" as const },
              { type: "separator" as const },
              { role: "quit" as const },
            ],
          },
        ]
      : []),
    {
      label: "App",
      submenu: [
        { label: `SushiAMO Desktop v${appVersion}`, enabled: false },
        { type: "separator" as const },
        {
          label: "Aggiorna pagina",
          accelerator: "CmdOrCtrl+R",
          click: () => {
            if (!mainWindow || mainWindow.isDestroyed()) return;
            mainWindow.webContents.reloadIgnoringCache();
          },
        },
        { type: "separator" as const },
        checkForUpdatesItem,
        { type: "separator" as const },
        { role: "quit" as const, label: "Esci" },
      ],
    },
    {
      label: "Aiuto",
      submenu: [
        openLogFolderItem,
        sendLogsItem,
        { type: "separator" as const },
        {
          label: "Documentazione online",
          click: async () => {
            await shell.openExternal("https://sushiamo.app/gestionale");
          },
        },
      ],
    },
  ];

  const menu = Menu.buildFromTemplate(template);
  Menu.setApplicationMenu(menu);
}

function getUpdateCheckIntervalMs() {
  const envValue = Number(process.env.SUSHIAMO_DESKTOP_UPDATE_CHECK_INTERVAL_MS || "");
  const resolved = Number.isFinite(envValue) && envValue > 0 ? envValue : DEFAULT_UPDATE_CHECK_INTERVAL_MS;
  return Math.max(MIN_UPDATE_CHECK_INTERVAL_MS, resolved);
}

function startDesktopAutoUpdatePolling() {
  if (!desktopAutoUpdateEnabled) return;

  if (desktopUpdateIntervalHandle) {
    clearInterval(desktopUpdateIntervalHandle);
    desktopUpdateIntervalHandle = null;
  }

  const intervalMs = getUpdateCheckIntervalMs();
  log("INFO", "desktop updater polling started", { intervalMs });

  desktopUpdateIntervalHandle = setInterval(() => {
    if (!desktopAutoUpdateEnabled) return;
    if (desktopUpdateCheckInProgress) return;
    if (desktopUpdateDownloaded) return;

    void (async () => {
      try {
        desktopUpdateCheckInProgress = true;
        await checkForUpdatesWithFeedFallback();
      } catch (error) {
        desktopUpdateCheckInProgress = false;
        log(
          "WARN",
          "desktop updater periodic check failed",
          error instanceof Error ? error.message : String(error),
        );
      }
    })();
  }, intervalMs);
}

function stopDesktopAutoUpdatePolling() {
  if (!desktopUpdateIntervalHandle) return;
  clearInterval(desktopUpdateIntervalHandle);
  desktopUpdateIntervalHandle = null;
}

function setupAutoUpdaterScaffold() {
  desktopAutoUpdateEnabled =
    String(runtimeConfig.SUSHIAMO_DESKTOP_ENABLE_AUTO_UPDATE || "0") === "1" && app.isPackaged;
  if (!desktopAutoUpdateEnabled) return;

  autoUpdater.autoDownload = false;
  autoUpdater.autoInstallOnAppQuit = true;
  autoUpdater.logger = null;

  const explicitUpdateUrl = String(runtimeConfig.SUSHIAMO_DESKTOP_UPDATE_URL || "").trim();
  const publicAppUrl = String(runtimeConfig.PUBLIC_APP_URL || "").trim().replace(/\/+$/, "");
  const derivedFromPublicApp = publicAppUrl ? `${publicAppUrl}/downloads` : "";
  const feedCandidates = [
    explicitUpdateUrl,
    derivedFromPublicApp,
    "https://www.sushiamo.app/downloads",
    "https://sushiamo.app/downloads",
  ]
    .map((value) => String(value || "").trim().replace(/\/+$/, ""))
    .filter(Boolean);
  desktopUpdateFeedCandidates = [...new Set(feedCandidates)];
  desktopUpdateFeedIndex = 0;
  if (desktopUpdateFeedCandidates.length > 0) {
    configureDesktopUpdaterFeed(desktopUpdateFeedCandidates[desktopUpdateFeedIndex]);
  }

  if (desktopUpdateListenersBound) return;
  desktopUpdateListenersBound = true;

  autoUpdater.on("checking-for-update", () => {
    desktopUpdateCheckInProgress = true;
    emitDesktopUpdateEvent({ type: "checking-for-update" });
  });

  autoUpdater.on("update-available", (info) => {
    desktopUpdateAvailable = true;
    desktopUpdateDownloaded = false;
    desktopUpdateInfo = {
      version: info.version || null,
      releaseName: info.releaseName || null,
      releaseDate: info.releaseDate || null,
    };
    emitDesktopUpdateEvent({
      type: "update-available",
      data: {
        version: info.version,
        releaseName: info.releaseName || null,
        releaseDate: info.releaseDate || null,
      },
    });
  });

  autoUpdater.on("update-not-available", (info) => {
    desktopUpdateAvailable = false;
    desktopUpdateDownloaded = false;
    desktopUpdateInfo = {
      version: info.version || null,
      releaseName: null,
      releaseDate: null,
    };
    emitDesktopUpdateEvent({
      type: "update-not-available",
      data: {
        version: info.version,
      },
    });
  });

  autoUpdater.on("download-progress", (progress) => {
    emitDesktopUpdateEvent({
      type: "download-progress",
      data: {
        percent: progress.percent,
        bytesPerSecond: progress.bytesPerSecond,
        transferred: progress.transferred,
        total: progress.total,
      },
    });
  });

  autoUpdater.on("update-downloaded", (info) => {
    desktopUpdateAvailable = true;
    desktopUpdateDownloaded = true;
    desktopUpdateCheckInProgress = false;
    desktopUpdateInfo = {
      version: info.version || null,
      releaseName: info.releaseName || null,
      releaseDate: info.releaseDate || null,
    };
    emitDesktopUpdateEvent({
      type: "update-downloaded",
      data: {
        version: info.version,
      },
    });
  });

  autoUpdater.on("error", (error) => {
    desktopUpdateCheckInProgress = false;
    const message = error instanceof Error ? error.message : String(error);
    log("WARN", "desktop updater error", message);
    emitDesktopUpdateEvent({
      type: "error",
      data: { message },
    });
  });
}

function configureDesktopUpdaterFeed(feedUrl: string) {
  try {
    autoUpdater.setFeedURL({ provider: "generic", url: feedUrl });
    log("INFO", "desktop updater feed configured", { url: feedUrl });
    return true;
  } catch (error) {
    log("WARN", "desktop updater feed configuration failed", {
      url: feedUrl,
      error: error instanceof Error ? error.message : String(error),
    });
    return false;
  }
}

function isUnauthorizedUpdaterError(error: unknown) {
  const message = error instanceof Error ? error.message : String(error || "");
  return /401|unauthorized|status code 401/i.test(message);
}

async function checkForUpdatesWithFeedFallback() {
  const attempts = Math.max(1, desktopUpdateFeedCandidates.length);
  let lastError: unknown = null;

  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      return await autoUpdater.checkForUpdates();
    } catch (error) {
      lastError = error;
      const canSwitch =
        isUnauthorizedUpdaterError(error) &&
        desktopUpdateFeedCandidates.length > 1 &&
        attempt < attempts - 1;

      if (!canSwitch) {
        throw error;
      }

      desktopUpdateFeedIndex = (desktopUpdateFeedIndex + 1) % desktopUpdateFeedCandidates.length;
      const nextFeed = desktopUpdateFeedCandidates[desktopUpdateFeedIndex];
      log("WARN", "desktop updater unauthorized response, switching feed", {
        nextFeed,
      });
      configureDesktopUpdaterFeed(nextFeed);
    }
  }

  if (lastError) throw lastError;
  throw new Error("Updater check failed");
}

async function loadRendererWithRetry(win: BrowserWindow) {
  if (!DEV_SERVER_URL) {
    const publicAppUrl = runtimeConfig.PUBLIC_APP_URL;
    if (publicAppUrl) {
      try {
        await win.loadURL(publicAppUrl);
      } catch {
        // did-fail-load event already handles showing the error fallback page
      }
    } else {
      await win.loadFile(path.join(app.getAppPath(), "dist", "index.html"));
    }
    return;
  }

  const attempts = 40;
  for (let index = 0; index < attempts; index += 1) {
    try {
      await win.loadURL(DEV_SERVER_URL);
      return;
    } catch (error) {
      if (index === attempts - 1) throw error;
      await new Promise((resolve) => setTimeout(resolve, 300));
    }
  }
}

async function createMainWindow() {
  const state = readWindowState();
  const desktopConfigEncoded = Buffer.from(JSON.stringify(getInjectedRendererConfig()), "utf8").toString("base64");
  const windowIcon = resolveWindowIconPath();

  mainWindow = new BrowserWindow({
    width: state.width,
    height: state.height,
    x: state.x,
    y: state.y,
    minWidth: 1080,
    minHeight: 720,
    icon: windowIcon,
    autoHideMenuBar: false,
    webPreferences: {
      preload: path.join(__dirname, "preload.cjs"),
      nodeIntegration: false,
      contextIsolation: true,
      sandbox: true,
      additionalArguments: [`--sushiamo-desktop-config=${desktopConfigEncoded}`],
    },
  });

  if (state.isMaximized) {
    mainWindow.maximize();
  }

  if (printWorker) {
    printWorker.attachWindow(mainWindow);
  }

  mainWindow.on("close", () => {
    if (mainWindow && !mainWindow.isDestroyed()) {
      writeWindowState(mainWindow);
    }
  });
  mainWindow.on("closed", () => {
    if (printWorker) {
      printWorker.attachWindow(null);
    }
    mainWindow = null;
  });
  mainWindow.on("unresponsive", () => {
    log("WARN", "main window unresponsive");
  });
  mainWindow.on("responsive", () => {
    log("INFO", "main window responsive again");
  });

  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    void shell.openExternal(url);
    return { action: "deny" };
  });
  mainWindow.webContents.on("did-fail-load", (_event, errorCode, errorDescription, validatedURL) => {
    const details = { errorCode, errorDescription, validatedURL };
    log("ERROR", "renderer failed to load", details);
    if (errorCode === -3) return; // aborted, ignore

    // Auto-retry on transient network errors (DNS, connection refused, network changed)
    const isRetriableError = errorCode === -105 || errorCode === -106 || errorCode === -21 || errorCode === -100;
    const publicAppUrl = runtimeConfig.PUBLIC_APP_URL;
    if (isRetriableError && publicAppUrl && !DEV_SERVER_URL && rendererRetryAttempts < MAX_RENDERER_RETRY_ATTEMPTS) {
      rendererRetryAttempts++;
      const delay = Math.min(2000 * rendererRetryAttempts, 8000);
      log("WARN", `renderer load failed (${errorCode}), retry ${rendererRetryAttempts}/${MAX_RENDERER_RETRY_ATTEMPTS} in ${delay}ms`, {});
      setTimeout(() => {
        if (!mainWindow || mainWindow.isDestroyed()) return;
        mainWindow.loadURL(publicAppUrl).catch(() => {});
      }, delay);
      return;
    }

    rendererRetryAttempts = 0;
    const win = mainWindow;
    if (win) void showRendererFallback(win, "did-fail-load", details);
  });
  mainWindow.webContents.on("render-process-gone", (_event, details) => {
    log("ERROR", "renderer process gone", details);
    const win = mainWindow;
    if (win) void showRendererFallback(win, "render-process-gone", details as unknown as Record<string, unknown>);
  });
  mainWindow.webContents.on("console-message", (_event, level, message, line, sourceId) => {
    const trimmed = String(message || "").trim();
    if (!trimmed) return;
    if (trimmed.includes("Electron Security Warning")) return;
    log(mapConsoleLevel(level), "renderer console", { level, message: trimmed, line, sourceId });
  });
  mainWindow.webContents.on("dom-ready", () => {
    log("INFO", "renderer dom-ready", { url: mainWindow?.webContents.getURL() || null });
  });

  setupDevShortcuts(mainWindow);

  await loadRendererWithRetry(mainWindow);

  mainWindow.webContents.on("did-finish-load", () => {
    rendererRetryAttempts = 0;
    log("INFO", "renderer did-finish-load", { url: mainWindow?.webContents.getURL() || null });
    if (pendingDeepLinkRoute) {
      sendDeepLinkToRenderer(pendingDeepLinkRoute);
      pendingDeepLinkRoute = null;
    }
  });
}

const gotLock = app.requestSingleInstanceLock();
if (!gotLock) {
  app.quit();
}

app.on("second-instance", (_event, argv) => {
  if (mainWindow) {
    if (mainWindow.isMinimized()) mainWindow.restore();
    mainWindow.focus();
  }
  const deepLink = extractDeepLinkFromArgv(argv);
  if (deepLink) handleDeepLink(deepLink);
});

app.on("open-url", (event, url) => {
  event.preventDefault();
  handleDeepLink(url);
});

app
  .whenReady()
  .then(async () => {
    app.setPath("crashDumps", path.join(app.getPath("userData"), "crash-dumps"));
    log("INFO", "desktop app starting", {
      isPackaged: app.isPackaged,
      devServer: DEV_SERVER_URL || null,
      platform: process.platform,
      arch: process.arch,
    });

    registerProtocolHandler();
    setupAutoUpdaterScaffold();
    setupApplicationMenu();
    printWorker = new DesktopPrintWorker({
      runtimeConfig: {
        VITE_SUPABASE_URL: runtimeConfig.VITE_SUPABASE_URL,
        VITE_SUPABASE_ANON_KEY: runtimeConfig.VITE_SUPABASE_ANON_KEY,
      },
      userDataPath: app.getPath("userData"),
      appVersion: app.getVersion(),
      onMainLog: (level, message) => log(level, message),
    });
    await printWorker.init();
    await createMainWindow();
    startDesktopAutoUpdatePolling();

    const initialDeepLink = extractDeepLinkFromArgv(process.argv);
    if (initialDeepLink) {
      handleDeepLink(initialDeepLink);
    }

    ipcMain.handle("desktop:get-runtime-config", () => getInjectedRendererConfig());
    ipcMain.on("desktop:renderer-log", (_event, payload) => {
      const data = (payload || {}) as Record<string, unknown>;
      const levelRaw = String(data.level || "INFO").toUpperCase();
      const level: "INFO" | "WARN" | "ERROR" =
        levelRaw === "ERROR" ? "ERROR" : levelRaw === "WARN" ? "WARN" : "INFO";
      const message = String(data.message || "renderer event");
      log(level, `renderer: ${message}`, data.details ?? data);
    });
    ipcMain.handle("desktop:get-debug-info", () => ({
      userDataPath: app.getPath("userData"),
      crashDumpsPath: app.getPath("crashDumps"),
      logFilePath: getLogFilePath(),
      isPackaged: app.isPackaged,
      appVersion: app.getVersion(),
    }));
    ipcMain.handle("desktop:open-log-folder", async () => {
      const logPath = getLogFilePath();
      if (!logPath) return { ok: false, reason: "LOG_PATH_UNAVAILABLE" };
      const opened = await shell.openPath(path.dirname(logPath));
      if (opened) return { ok: false, reason: opened };
      return { ok: true };
    });
    ipcMain.handle("desktop:reload-app", async () => {
      if (!mainWindow || mainWindow.isDestroyed()) return { ok: false };
      rendererRetryAttempts = 0;
      const publicAppUrl = runtimeConfig.PUBLIC_APP_URL;
      if (publicAppUrl) {
        mainWindow.loadURL(publicAppUrl).catch(() => {});
      } else {
        mainWindow.reload();
      }
      return { ok: true };
    });

    ipcMain.handle("desktop:refocus-window", async () => {
      if (!mainWindow || mainWindow.isDestroyed()) {
        return { ok: false, reason: "WINDOW_UNAVAILABLE" };
      }
      try {
        if (mainWindow.isMinimized()) {
          mainWindow.restore();
        }
        mainWindow.show();
        mainWindow.focus();
        mainWindow.webContents.focus();
        return { ok: true };
      } catch (error) {
        return {
          ok: false,
          reason: error instanceof Error ? error.message : String(error || "FOCUS_FAILED"),
        };
      }
    });
    ipcMain.handle("desktop:credentials:get", async () => {
      const accounts = await readSavedAccounts();
      return accounts[0] || null;
    });
    ipcMain.handle("desktop:credentials:set", async (_event, payload) => {
      return upsertSavedAccount((payload || {}) as { email?: unknown; password?: unknown; displayName?: unknown });
    });
    ipcMain.handle("desktop:credentials:clear", async () => {
      return clearAllSavedAccounts();
    });
    ipcMain.handle("desktop:accounts:get-all", async () => {
      return readSavedAccounts();
    });
    ipcMain.handle("desktop:accounts:remove", async (_event, payload) => {
      const email = String(((payload || {}) as Record<string, unknown>).email || "").trim();
      if (!email) return { ok: false };
      return removeSavedAccount(email);
    });
    ipcMain.handle("desktop:check-for-updates", async () => {
      if (!desktopAutoUpdateEnabled) {
        return {
          enabled: false,
          currentVersion: app.getVersion(),
          available: false,
          downloaded: false,
          updateInfo: null,
          reason: app.isPackaged ? "DISABLED_BY_CONFIG" : "NOT_PACKAGED",
        };
      }
      desktopUpdateCheckInProgress = true;
      try {
        const result = await checkForUpdatesWithFeedFallback();
        desktopUpdateCheckInProgress = false;
        return {
          enabled: true,
          currentVersion: app.getVersion(),
          checking: false,
          available: desktopUpdateAvailable,
          downloaded: desktopUpdateDownloaded,
          updateInfo: result?.updateInfo || desktopUpdateInfo || null,
        };
      } catch (error) {
        desktopUpdateCheckInProgress = false;
        return {
          enabled: true,
          currentVersion: app.getVersion(),
          checking: false,
          available: desktopUpdateAvailable,
          downloaded: desktopUpdateDownloaded,
          updateInfo: desktopUpdateInfo || null,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    });
    ipcMain.handle("desktop:get-update-state", async () => {
      return {
        enabled: desktopAutoUpdateEnabled,
        checking: desktopUpdateCheckInProgress,
        available: desktopUpdateAvailable,
        downloaded: desktopUpdateDownloaded,
        currentVersion: app.getVersion(),
        updateInfo: desktopUpdateInfo || null,
      };
    });
    ipcMain.handle("desktop:download-update", async () => {
      if (!desktopAutoUpdateEnabled) {
        return {
          ok: false,
          reason: app.isPackaged ? "DISABLED_BY_CONFIG" : "NOT_PACKAGED",
        };
      }
      try {
        await autoUpdater.downloadUpdate();
        return {
          ok: true,
        };
      } catch (error) {
        return {
          ok: false,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    });
    ipcMain.handle("desktop:install-update", async () => {
      if (!desktopAutoUpdateEnabled) {
        return {
          ok: false,
          reason: app.isPackaged ? "DISABLED_BY_CONFIG" : "NOT_PACKAGED",
        };
      }
      if (!desktopUpdateDownloaded) {
        return {
          ok: false,
          reason: "UPDATE_NOT_DOWNLOADED",
        };
      }
      setTimeout(() => {
        autoUpdater.quitAndInstall(false, true);
      }, 200);
      return {
        ok: true,
      };
    });
    ipcMain.handle("desktop:printer:get-state", async () => {
      return printWorker?.getPublicState() || null;
    });
    ipcMain.handle("desktop:printer:save-config", async (_event, payload) => {
      if (!printWorker) throw new Error("PRINT_WORKER_UNAVAILABLE");
      return printWorker.saveConfig((payload || {}) as Record<string, unknown>);
    });
    ipcMain.handle("desktop:printer:sync-session", async (_event, payload) => {
      if (!printWorker) throw new Error("PRINT_WORKER_UNAVAILABLE");
      return printWorker.syncSession(payload || null);
    });
    ipcMain.handle("desktop:printer:clear-session", async () => {
      if (!printWorker) throw new Error("PRINT_WORKER_UNAVAILABLE");
      return printWorker.clearSession();
    });
    ipcMain.handle("desktop:printer:start", async () => {
      if (!printWorker) throw new Error("PRINT_WORKER_UNAVAILABLE");
      return printWorker.startService();
    });
    ipcMain.handle("desktop:printer:stop", async () => {
      if (!printWorker) throw new Error("PRINT_WORKER_UNAVAILABLE");
      return printWorker.stopService();
    });
    ipcMain.handle("desktop:printer:discover", async (_event, payload) => {
      if (!printWorker) throw new Error("PRINT_WORKER_UNAVAILABLE");
      const timeoutMs = Number((payload as Record<string, unknown> | null)?.timeoutMs);
      return printWorker.discoverPrinters(Number.isFinite(timeoutMs) ? timeoutMs : undefined);
    });
    ipcMain.handle("desktop:printer:discover-rt", async (_event, payload) => {
      if (!printWorker) throw new Error("PRINT_WORKER_UNAVAILABLE");
      const timeoutMs = Number((payload as Record<string, unknown> | null)?.timeoutMs);
      return printWorker.discoverRtDevices(Number.isFinite(timeoutMs) ? timeoutMs : undefined);
    });
    ipcMain.handle("desktop:printer:test-rt-receipt", async (_event, payload) => {
      if (!printWorker) throw new Error("PRINT_WORKER_UNAVAILABLE");
      const config = (payload || {}) as { host?: string; port?: number; brand?: string; api_path?: string };
      return printWorker.testRtReceipt({
        host: String(config.host ?? ""),
        port: Number(config.port ?? 80),
        brand: String(config.brand ?? "epson"),
        api_path: String(config.api_path ?? "/cgi-bin/fpmate.cgi"),
      });
    });
  })
  .catch((error) => {
    log("ERROR", "desktop startup failed", error instanceof Error ? error.stack || error.message : String(error));
    app.quit();
  });

app.on("activate", () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    void createMainWindow();
  }
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    app.quit();
  }
});

app.on("before-quit", () => {
  stopDesktopAutoUpdatePolling();
  if (printWorker) {
    void printWorker.shutdown();
  }
});

app.on("render-process-gone", (_event, webContents, details) => {
  log("ERROR", "app render-process-gone", {
    id: webContents.id,
    url: webContents.getURL(),
    details,
  });
});

app.on("child-process-gone", (_event, details) => {
  log("WARN", "child process gone", details);
});

process.on("uncaughtException", (error) => {
  log("ERROR", "uncaught exception", error.stack || error.message);
});

process.on("unhandledRejection", (reason) => {
  log("ERROR", "unhandled rejection", reason);
});
