import fs from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import type { BrowserWindow } from "electron";
import { createClient, type SupabaseClient } from "@supabase/supabase-js";

type RuntimeConfig = {
  VITE_SUPABASE_URL: string;
  VITE_SUPABASE_ANON_KEY: string;
};

type WorkerConfig = {
  consumerId: string;
  deviceName: string;
  pollMs: number;
  claimLimit: number;
  autoStart: boolean;
};

type SessionSnapshot = {
  access_token: string;
  refresh_token: string;
  expires_at: number | null;
};

type WorkerLogRow = {
  at: string;
  level: "INFO" | "WARN" | "ERROR";
  message: string;
};

type RestaurantScope = {
  id: string;
  name: string;
  city: string | null;
  role: "owner" | "admin" | "manager" | "staff";
};

type JobRow = {
  id: string;
  department: string | null;
  payload: Record<string, unknown> | null;
  route: Record<string, unknown> | null;
  created_at: string;
};

type PhysicalReceiptJobRow = {
  id: string;
  payload: Record<string, unknown> | null;
  created_at: string;
};

type NonFiscalReceiptJobRow = {
  id: string;
  payload: Record<string, unknown> | null;
  created_at: string;
};

type LivePrinter = {
  id: string;
  name: string;
  host: string;
  port: number;
  enabled: boolean;
  departments: string[];
};

type LiveRoutes = {
  byId: Map<string, LivePrinter>;
  byDepartment: Map<string, LivePrinter>;
  defaultPrinterId: string | null;
};

type DiscoverPrinter = {
  host: string;
  port: number;
  connection_type: "ethernet" | "wifi" | "unknown";
  interface_name: string | null;
  interface_ip: string | null;
  source: "lan_scan";
  label: string;
};

type DiscoverRtDevice = {
  host: string;
  port: number;
  brand: "epson" | "custom" | "axon" | "rch" | "olivetti" | "other";
  api_path: string;
  connection_type: "ethernet" | "wifi" | "unknown";
  interface_name: string | null;
  interface_ip: string | null;
  source: "lan_scan";
  label: string;
};

type WorkerPublicState = {
  config: WorkerConfig;
  auth: {
    user: { id: string; email: string | null } | null;
    restaurant: RestaurantScope | null;
  };
  service: {
    running: boolean;
    processing: boolean;
    assignedPrinterId: string | null;
    stats: {
      claimed: number;
      printed: number;
      failed: number;
      lastRunAt: string | null;
      lastError: string | null;
    };
  };
  logs: WorkerLogRow[];
};

type PrintWorkerDeps = {
  runtimeConfig: RuntimeConfig;
  userDataPath: string;
  appVersion: string;
  onMainLog?: (level: "INFO" | "WARN" | "ERROR", message: string) => void;
};

const CONFIG_FILENAME = "desktop-print-worker.json";
const DISCOVERY_PORTS = [9100, 515, 631];
const RT_DISCOVERY_PORTS = [8008, 80, 443];
const DISCOVERY_TIMEOUT_MIN = 120;
const DISCOVERY_TIMEOUT_MAX = 2000;
const DISCOVERY_TIMEOUT_DEFAULT = 350;
const DISCOVERY_CONCURRENCY = 96;
const DISCOVERY_MAX_HOSTS = 1024;
const LOG_CAP = 500;

const roleRank: Record<RestaurantScope["role"], number> = {
  owner: 1,
  admin: 2,
  manager: 3,
  staff: 4,
};

function normalizeError(error: unknown) {
  if (!error) return "Errore sconosciuto";
  if (typeof error === "string") return error;
  if (error instanceof Error) return error.message || "Errore sconosciuto";
  try {
    return JSON.stringify(error);
  } catch {
    return String(error);
  }
}

function isMissingRpcError(error: unknown, rpcName: string) {
  const message = normalizeError(error).toLowerCase();
  const needle = String(rpcName || "").trim().toLowerCase();
  if (!needle) return false;
  return (
    message.includes(needle) &&
    (message.includes("schema cache") ||
      message.includes("could not find") ||
      message.includes("does not exist") ||
      message.includes("not found"))
  );
}

function sanitizeConsumerId(value: unknown) {
  const raw = String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._:-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 64);
  if (raw) return raw;
  const host = String(os.hostname() || "")
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 40);
  const prefix = process.platform === "win32" ? "win" : process.platform === "darwin" ? "mac" : "bridge";
  return `${prefix}-bridge-${host || "main"}`.slice(0, 64);
}

function sanitizeDeviceName(value: unknown) {
  const normalized = String(value ?? "").trim().replace(/\s+/g, " ").slice(0, 80);
  return normalized || `Bridge ${os.hostname()}`;
}

function sanitizePollMs(value: unknown) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 2500;
  return Math.max(1000, Math.min(10000, Math.trunc(n)));
}

function sanitizeClaimLimit(value: unknown) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 5;
  return Math.max(1, Math.min(20, Math.trunc(n)));
}

function sanitizePrinterPort(value: unknown) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 9100;
  const port = Math.trunc(n);
  return port >= 1 && port <= 65535 ? port : 9100;
}

function normalizePhysicalBrand(value: unknown) {
  const brand = String(value ?? "").trim().toLowerCase();
  return brand || "epson";
}

function sanitizePhysicalPort(value: unknown, brand: string) {
  const parsed = Number(value);
  if (Number.isInteger(parsed) && parsed >= 1 && parsed <= 65535) return parsed;
  return brand === "epson" ? 8008 : 9100;
}

function defaultPhysicalPathByBrand(brand: string) {
  return brand === "epson" ? "/cgi-bin/fpmate.cgi" : "/";
}

function sanitizePhysicalPath(value: unknown, brand = "epson") {
  const pathValue = String(value ?? "").trim();
  if (!pathValue) return defaultPhysicalPathByBrand(brand);
  return pathValue.startsWith("/") ? pathValue : `/${pathValue}`;
}

function normalizePaymentMethod(value: unknown) {
  const method = String(value ?? "").trim().toLowerCase();
  return method === "card" ? "card" : "cash";
}

function toCents(value: unknown) {
  const amount = Number(value);
  if (!Number.isFinite(amount) || amount <= 0) return 0;
  return Math.max(0, Math.round(amount * 100));
}

function escapeXml(value: unknown) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

function buildEpsonFiscalReceiptXml(payload: Record<string, unknown>) {
  const tableNumber = String(payload?.table_number ?? "").trim() || "-";
  const paymentMethod = normalizePaymentMethod(payload?.payment_method);
  const paymentLabel = paymentMethod === "card" ? "ELETTRONICO" : "CONTANTI";
  const cents = Math.max(1, toCents(payload?.total_amount));
  const description = `Sushiamo Tavolo ${tableNumber}`;

  return (
    `<?xml version="1.0" encoding="UTF-8"?>\n` +
    `<FPMessage>\n` +
    `  <beginFiscalReceipt operator="1"/>\n` +
    `  <printRecItem description="${escapeXml(description)}" price="${cents}" quantity="1" department="1" vatCode="1"/>\n` +
    `  <printRecTotal description="${paymentLabel}" payment="${cents}"/>\n` +
    `  <endFiscalReceipt/>\n` +
    `</FPMessage>`
  );
}

function buildNonFiscalTestXml() {
  const now = new Date();
  const dateStr = now.toLocaleDateString("it-IT", { day: "2-digit", month: "2-digit", year: "numeric" });
  const timeStr = now.toLocaleTimeString("it-IT", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  return (
    `<?xml version="1.0" encoding="UTF-8"?>\n` +
    `<FPMessage>\n` +
    `  <beginNonFiscal operator="1"/>\n` +
    `  <printNormal data="================================"/>\n` +
    `  <printNormal data="   SCONTRINO DI PROVA"/>\n` +
    `  <printNormal data="   TEST RECEIPT"/>\n` +
    `  <printNormal data=""/>\n` +
    `  <printNormal data="   SushiAMO Desktop"/>\n` +
    `  <printNormal data="   Connessione OK"/>\n` +
    `  <printNormal data="   ${dateStr}  ${timeStr}"/>\n` +
    `  <printNormal data="================================"/>\n` +
    `  <endNonFiscal/>\n` +
    `</FPMessage>`
  );
}

function extractReceiptIdFromResponse(responseText: string) {
  const text = String(responseText || "");
  if (!text) return null;
  const patterns = [
    /receipt[_\s-]?id["'\s:=]+([A-Za-z0-9._:/-]+)/i,
    /document[_\s-]?number["'\s:=]+([A-Za-z0-9._:/-]+)/i,
    /progressive[_\s-]?number["'\s:=]+([A-Za-z0-9._:/-]+)/i,
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match?.[1]) return match[1].trim();
  }
  return null;
}

function normalizeDepartment(value: unknown) {
  const dep = String(value ?? "").trim().toLowerCase();
  return dep || "cucina";
}

function toSessionSnapshot(raw: unknown): SessionSnapshot | null {
  if (!raw || typeof raw !== "object") return null;
  const session = raw as Record<string, unknown>;
  const accessToken = String(session.access_token || "").trim();
  const refreshToken = String(session.refresh_token || "").trim();
  if (!accessToken || !refreshToken) return null;
  const expiresAt = Number(session.expires_at);
  return {
    access_token: accessToken,
    refresh_token: refreshToken,
    expires_at: Number.isFinite(expiresAt) ? Math.trunc(expiresAt) : null,
  };
}

function sameSession(a: SessionSnapshot | null, b: SessionSnapshot | null) {
  if (!a && !b) return true;
  if (!a || !b) return false;
  return a.access_token === b.access_token && a.refresh_token === b.refresh_token && Number(a.expires_at || 0) === Number(b.expires_at || 0);
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function shouldRetryPrintLocally(error: unknown) {
  const message = normalizeError(error).toLowerCase();
  return (
    message.includes("timeout") ||
    message.includes("econnreset") ||
    message.includes("ehostunreach") ||
    message.includes("econnrefused") ||
    message.includes("epipe")
  );
}

function openRawSocket(host: string, port: number, payload: Buffer, timeoutMs = 25000) {
  return new Promise<void>((resolve, reject) => {
    const socket = new net.Socket();
    let done = false;
    const complete = (error?: Error | null) => {
      if (done) return;
      done = true;
      socket.destroy();
      if (error) reject(error);
      else resolve();
    };
    socket.setNoDelay(true);
    socket.setTimeout(timeoutMs);
    socket.once("error", (error) => complete(error));
    socket.once("timeout", () => complete(new Error("Timeout stampante")));
    socket.connect(port, host, () => {
      socket.write(payload, (error) => {
        if (error) {
          complete(error);
          return;
        }
        socket.end(() => complete(null));
      });
    });
    socket.once("close", (hadError) => {
      if (hadError) return;
      if (!done) complete(null);
    });
  });
}

function wrapText(input: string, width: number) {
  const text = String(input || "").trim();
  if (!text) return [""];
  if (text.length <= width) return [text];
  const words = text.split(/\s+/);
  const lines: string[] = [];
  let current = "";
  for (const word of words) {
    if (!current) {
      current = word;
      continue;
    }
    if (`${current} ${word}`.length <= width) {
      current += ` ${word}`;
      continue;
    }
    lines.push(current);
    current = word;
  }
  if (current) lines.push(current);
  return lines;
}

function formatTimestamp(value: unknown) {
  if (!value) return "";
  const date = new Date(String(value));
  if (Number.isNaN(date.getTime())) return String(value);
  const year = date.getFullYear();
  const month = date.getMonth() + 1;
  const day = date.getDate();
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  return `${year}/${month}/${day} ${hours}:${minutes}`;
}

function prettifyDishName(value: unknown) {
  const raw = String(value || "").trim().replace(/\s+/g, " ");
  if (!raw) return "";
  if (/[a-zàèéìòù]/.test(raw)) return raw;
  return raw
    .toLowerCase()
    .split(" ")
    .map((part) => (part ? `${part[0]?.toUpperCase() || ""}${part.slice(1)}` : part))
    .join(" ");
}

function renderTicket(job: JobRow) {
  const width = 42;
  const payload = (job.payload || {}) as Record<string, unknown>;
  const items = Array.isArray(payload.items) ? payload.items : [];
  const department = normalizeDepartment(payload.department || job.department);
  const restaurantName = String(payload.restaurant_name || "").trim() || "Ristorante";
  const tableLabel = String(payload.table_number || "").trim() || "-";
  const orderLabel = payload.order_number != null ? `#${String(payload.order_number)}` : "#-";
  const createdAt = formatTimestamp(payload.created_at || job.created_at);
  const lines: string[] = [];
  lines.push(`COMANDA ${department.toUpperCase()} ${orderLabel}`);
  lines.push(`TAVOLO: ${tableLabel.toUpperCase()}`);
  if (createdAt) lines.push(`DATA: ${createdAt}`);
  lines.push("-".repeat(width));
  for (const rawItem of items) {
    const item = (rawItem || {}) as Record<string, unknown>;
    const qty = Math.max(1, Number(item.quantity) || 1);
    const label = `${qty}x ${prettifyDishName(item.name)}`;
    for (const chunk of wrapText(label, width)) lines.push(chunk);
    const notes = String(item.notes || "").trim();
    if (notes) for (const chunk of wrapText(`Nota: ${notes}`, width - 2)) lines.push(` ${chunk}`);
  }
  lines.push(`-- ${restaurantName} --`);
  return lines.join("\n");
}

function formatCurrency(value: unknown) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "€   0,00";
  const str = Math.abs(n).toFixed(2).replace(".", ",");
  return `€ ${str}`;
}

function padRow(label: string, value: string, width: number) {
  const gap = width - label.length - value.length;
  return label + " ".repeat(Math.max(1, gap)) + value;
}

function renderNonFiscalReceiptTicket(job: NonFiscalReceiptJobRow) {
  const width = 42;
  const payload = (job.payload || {}) as Record<string, unknown>;
  const restaurantName = String(payload.restaurant_name || "").trim() || "Ristorante";
  const tableLabel = String(payload.table_number || "").trim() || "-";
  const paidAt = formatTimestamp(payload.paid_at || job.created_at);
  const paymentMethod = normalizePaymentMethod(payload.payment_method);
  const paymentLabel = paymentMethod === "card" ? "Carta" : "Contanti";
  const ayceTotal = Number(payload.ayce_total) || 0;
  const coverTotal = Number(payload.cover_total) || 0;
  const extrasTotal = Number(payload.extras_total) || 0;
  const totalAmount = Number(payload.total_amount) || 0;

  const center = (text: string) => {
    if (text.length >= width) return text;
    const pad = Math.floor((width - text.length) / 2);
    return " ".repeat(pad) + text;
  };

  const lines: string[] = [];
  lines.push("=".repeat(width));
  lines.push(center(restaurantName));
  lines.push("=".repeat(width));
  lines.push(center("SCONTRINO NON FISCALE"));
  lines.push("-".repeat(width));
  lines.push(`Tavolo: ${tableLabel}`);
  if (paidAt) lines.push(`Data:   ${paidAt}`);
  lines.push("-".repeat(width));

  if (ayceTotal > 0) lines.push(padRow("AYCE", formatCurrency(ayceTotal), width));
  if (coverTotal > 0) lines.push(padRow("Coperto", formatCurrency(coverTotal), width));
  if (extrasTotal > 0) lines.push(padRow("Extra", formatCurrency(extrasTotal), width));

  lines.push("-".repeat(width));
  lines.push(padRow("TOTALE", formatCurrency(totalAmount), width));
  lines.push(padRow("Pagamento", paymentLabel, width));
  lines.push("=".repeat(width));
  lines.push(center("Grazie per la visita!"));
  lines.push(center("*** NON FISCALE ***"));
  lines.push("=".repeat(width));
  return lines.join("\n");
}

function isBoldLine(line: string) {
  const trimmed = line.trim();
  if (!trimmed) return false;
  if (/^tavolo:/i.test(trimmed)) return true;
  if (/^\d+x\s+/i.test(trimmed)) return true;
  return false;
}

function getLinePrintSize(line: string) {
  const trimmed = line.trim();
  if (!trimmed) return 0x00;
  if (/^tavolo:/i.test(trimmed)) {
    // Double width + double height for table.
    return 0x11;
  }
  if (/^\d+x\s+/i.test(trimmed)) {
    // Product rows as large as table rows.
    return 0x11;
  }
  // Keep date and other lines in normal size.
  return 0x00;
}

function buildEscPosPayload(ticketText: string) {
  const ESC = 0x1b;
  const GS = 0x1d;
  const EXTRA_FEED_LINES = 7;
  const lines = ticketText.split(/\r?\n/);
  const chunks = [
    Buffer.from([ESC, 0x40]),
    // Alternate ESC/POS font (Font B) for a less rigid look.
    Buffer.from([ESC, 0x4d, 0x01]),
    // Slight character spacing improves readability on thermal heads.
    Buffer.from([ESC, 0x20, 0x02]),
  ];
  let bold = false;
  let size = 0x00;
  for (const line of lines) {
    const shouldBold = isBoldLine(line);
    if (shouldBold !== bold) {
      chunks.push(Buffer.from([ESC, 0x45, shouldBold ? 0x01 : 0x00]));
      bold = shouldBold;
    }
    const nextSize = getLinePrintSize(line);
    if (nextSize !== size) {
      chunks.push(Buffer.from([GS, 0x21, nextSize]));
      size = nextSize;
    }
    chunks.push(Buffer.from(`${line}\n`, "utf8"));
  }
  if (bold) chunks.push(Buffer.from([ESC, 0x45, 0x00]));
  if (size !== 0x00) chunks.push(Buffer.from([GS, 0x21, 0x00]));
  chunks.push(Buffer.from([ESC, 0x64, EXTRA_FEED_LINES]));
  chunks.push(Buffer.from([GS, 0x56, 0x00]));
  return Buffer.concat(chunks);
}

function classifyInterfaceConnectionType(interfaceName: string): "ethernet" | "wifi" | "unknown" {
  const normalized = interfaceName.toLowerCase();
  if (!normalized) return "unknown";
  if (
    normalized.includes("wi-fi") ||
    normalized.includes("wifi") ||
    normalized.includes("wireless") ||
    normalized.includes("wlan")
  ) {
    return "wifi";
  }
  if (normalized.includes("ethernet") || normalized.includes("lan") || normalized.startsWith("eth")) {
    return "ethernet";
  }
  return "unknown";
}

function inferRtBrandFromContent(content: string): DiscoverRtDevice["brand"] | null {
  const normalized = String(content || "").toLowerCase();
  if (!normalized) return null;
  if (normalized.includes("epson") || normalized.includes("fpmate") || normalized.includes("fp90")) return "epson";
  if (normalized.includes("custom")) return "custom";
  if (normalized.includes("olivetti")) return "olivetti";
  if (normalized.includes("axon")) return "axon";
  if (normalized.includes("rch")) return "rch";
  return null;
}

function inferRtBrandByPort(port: number): DiscoverRtDevice["brand"] {
  if (port === 8008) return "epson";
  return "other";
}

function formatRtDeviceLabel(brand: DiscoverRtDevice["brand"], host: string) {
  const brandLabel =
    brand === "epson"
      ? "Epson RT"
      : brand === "custom"
      ? "Custom RT"
      : brand === "olivetti"
      ? "Olivetti RT"
      : brand === "axon"
      ? "Axon RT"
      : brand === "rch"
      ? "RCH RT"
      : "RT device";
  return `${brandLabel} ${host}`;
}

async function fingerprintRtHttp(host: string, port: number, timeoutMs: number) {
  const protocol = port === 443 ? "https" : "http";
  const endpoint = `${protocol}://${host}:${port}/`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(300, timeoutMs));
  try {
    const response = await fetch(endpoint, {
      method: "GET",
      signal: controller.signal,
    });
    const text = await response.text().catch(() => "");
    const headerHints = [
      response.headers.get("server") || "",
      response.headers.get("x-powered-by") || "",
    ]
      .filter(Boolean)
      .join(" ");
    return `${headerHints}\n${text}`.slice(0, 3000);
  } catch {
    return "";
  } finally {
    clearTimeout(timeout);
  }
}

function normalizeIpv4(address: string) {
  const raw = String(address || "").trim();
  const parts = raw.split(".");
  if (parts.length !== 4) return null;
  const octets = parts.map((part) => Number(part));
  if (octets.some((octet) => !Number.isInteger(octet) || octet < 0 || octet > 255)) return null;
  return octets.join(".");
}

function collectLanTargets() {
  const interfaces = os.networkInterfaces();
  const targetsByHost = new Map<
    string,
    { host: string; interfaceName: string; interfaceIp: string; connectionType: "ethernet" | "wifi" | "unknown" }
  >();

  for (const [interfaceName, rows] of Object.entries(interfaces)) {
    for (const row of rows || []) {
      const family = typeof row.family === "string" ? row.family : row.family === 4 ? "IPv4" : "IPv6";
      if (family !== "IPv4") continue;
      if (row.internal) continue;
      const ipv4 = normalizeIpv4(row.address || "");
      if (!ipv4) continue;
      const [first, second, third, current] = ipv4.split(".").map((part) => Number(part));
      if (first === 127) continue;
      if (first === 169 && second === 254) continue;
      const prefix = `${first}.${second}.${third}`;
      const connectionType = classifyInterfaceConnectionType(interfaceName);

      for (let host = 1; host <= 254; host += 1) {
        if (host === current) continue;
        const candidate = `${prefix}.${host}`;
        if (targetsByHost.has(candidate)) continue;
        targetsByHost.set(candidate, {
          host: candidate,
          interfaceName,
          interfaceIp: ipv4,
          connectionType,
        });
      }
    }
  }

  return Array.from(targetsByHost.values()).slice(0, DISCOVERY_MAX_HOSTS);
}

function probeTcpPort(host: string, port: number, timeoutMs: number) {
  return new Promise<boolean>((resolve) => {
    const socket = new net.Socket();
    let done = false;
    const complete = (isOpen: boolean) => {
      if (done) return;
      done = true;
      socket.destroy();
      resolve(isOpen);
    };
    socket.setTimeout(timeoutMs);
    socket.once("connect", () => complete(true));
    socket.once("timeout", () => complete(false));
    socket.once("error", () => complete(false));
    socket.connect(port, host);
  });
}

async function runWithConcurrency<T>(items: T[], concurrency: number, worker: (item: T, idx: number) => Promise<void>) {
  if (!items.length) return;
  const safeConcurrency = Math.max(1, Math.min(concurrency, items.length));
  let index = 0;
  const runners = Array.from({ length: safeConcurrency }, async () => {
    while (index < items.length) {
      const currentIndex = index;
      index += 1;
      await worker(items[currentIndex], currentIndex);
    }
  });
  await Promise.all(runners);
}

export class DesktopPrintWorker {
  private readonly runtimeConfig: RuntimeConfig;
  private readonly appVersion: string;
  private readonly onMainLog?: (level: "INFO" | "WARN" | "ERROR", message: string) => void;
  private readonly configPath: string;
  private readonly logs: WorkerLogRow[] = [];
  private readonly authState: { user: { id: string; email: string | null } | null; restaurant: RestaurantScope | null } = {
    user: null,
    restaurant: null,
  };
  private config: WorkerConfig = {
    consumerId: sanitizeConsumerId(""),
    deviceName: sanitizeDeviceName(`Bridge ${os.hostname()}`),
    pollMs: 2500,
    claimLimit: 5,
    autoStart: true,
  };
  private savedSession: SessionSnapshot | null = null;
  private supabase: SupabaseClient | null = null;
  private physicalReceiptRpcAvailable = true;
  private nonFiscalReceiptRpcAvailable = true;
  private boundWindow: BrowserWindow | null = null;
  private service = {
    running: false,
    processing: false,
    timer: null as NodeJS.Timeout | null,
    assignedPrinterId: null as string | null,
    stats: {
      claimed: 0,
      printed: 0,
      failed: 0,
      lastRunAt: null as string | null,
      lastError: null as string | null,
    },
  };

  constructor(deps: PrintWorkerDeps) {
    this.runtimeConfig = deps.runtimeConfig;
    this.appVersion = deps.appVersion;
    this.onMainLog = deps.onMainLog;
    this.configPath = path.join(deps.userDataPath, CONFIG_FILENAME);
  }

  attachWindow(win: BrowserWindow | null) {
    this.boundWindow = win;
    this.broadcastState();
  }

  getPublicState(): WorkerPublicState {
    return {
      config: { ...this.config },
      auth: {
        user: this.authState.user ? { ...this.authState.user } : null,
        restaurant: this.authState.restaurant ? { ...this.authState.restaurant } : null,
      },
      service: {
        running: this.service.running,
        processing: this.service.processing,
        assignedPrinterId: this.service.assignedPrinterId,
        stats: { ...this.service.stats },
      },
      logs: [...this.logs],
    };
  }

  async init() {
    await this.loadPersistedState();
    if (this.config.autoStart && this.savedSession) {
      try {
        await this.startService();
      } catch (error) {
        this.pushLog("WARN", `Avvio automatico non riuscito: ${normalizeError(error)}`);
      }
    }
  }

  async shutdown() {
    await this.stopService();
  }

  async saveConfig(partial: Partial<WorkerConfig>) {
    this.config = {
      ...this.config,
      ...partial,
      consumerId: sanitizeConsumerId(partial.consumerId ?? this.config.consumerId),
      deviceName: sanitizeDeviceName(partial.deviceName ?? this.config.deviceName),
      pollMs: sanitizePollMs(partial.pollMs ?? this.config.pollMs),
      claimLimit: sanitizeClaimLimit(partial.claimLimit ?? this.config.claimLimit),
      autoStart: partial.autoStart == null ? this.config.autoStart : Boolean(partial.autoStart),
    };
    await this.persistState();
    this.pushLog("INFO", "Configurazione worker stampa aggiornata");
    return this.getPublicState();
  }

  async syncSession(rawSession: unknown) {
    const snapshot = toSessionSnapshot(rawSession);
    if (!snapshot) throw new Error("SESSION_INVALID");
    if (sameSession(this.savedSession, snapshot)) return this.getPublicState();
    this.savedSession = snapshot;
    await this.persistState();
    this.pushLog("INFO", "Sessione desktop sincronizzata");
    if (this.config.autoStart && !this.service.running) {
      try {
        await this.startService();
      } catch (error) {
        this.pushLog("WARN", `Auto-start non riuscito: ${normalizeError(error)}`);
      }
    }
    return this.getPublicState();
  }

  async clearSession() {
    this.savedSession = null;
    this.authState.user = null;
    this.authState.restaurant = null;
    this.supabase = null;
    await this.stopService();
    await this.persistState();
    this.pushLog("INFO", "Sessione desktop rimossa");
    return this.getPublicState();
  }

  async startService() {
    if (this.service.running) return this.getPublicState();
    await this.ensureSignedIn();
    if (!this.authState.restaurant?.id) {
      throw new Error("Nessun ristorante associato all'account.");
    }
    this.service.running = true;
    this.service.processing = false;
    this.service.assignedPrinterId = null;
    this.service.stats = {
      claimed: 0,
      printed: 0,
      failed: 0,
      lastRunAt: null,
      lastError: null,
    };
    this.physicalReceiptRpcAvailable = true;
    this.nonFiscalReceiptRpcAvailable = true;
    this.pushLog("INFO", `Servizio stampa avviato (${this.config.consumerId})`);
    this.broadcastState();
    void this.runTick();
    return this.getPublicState();
  }

  async stopService() {
    this.service.running = false;
    this.service.processing = false;
    if (this.service.timer) {
      clearTimeout(this.service.timer);
      this.service.timer = null;
    }
    try {
      await this.heartbeatAgent(false);
    } catch {
      // ignore
    }
    this.pushLog("INFO", "Servizio stampa fermato");
    this.broadcastState();
    return this.getPublicState();
  }

  async discoverPrinters(timeoutMs?: number) {
    return this.discoverNetworkPrinters(timeoutMs);
  }

  async discoverRtDevices(timeoutMs?: number) {
    return this.discoverNetworkRtDevices(timeoutMs);
  }

  async testRtReceipt(config: { host: string; port: number; brand: string; api_path: string }): Promise<{ ok: boolean; error?: string; elapsed_ms?: number }> {
    const host = String(config?.host ?? "").trim();
    const brand = normalizePhysicalBrand(config?.brand);
    const port = sanitizePhysicalPort(config?.port, brand);
    const apiPath = sanitizePhysicalPath(config?.api_path, brand);
    if (!host) return { ok: false, error: "PHYSICAL_RT_HOST_MISSING" };

    const endpoint = `http://${host}:${port}${apiPath}`;
    const body = buildNonFiscalTestXml();
    const t0 = Date.now();
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);

    try {
      const response = await fetch(endpoint, {
        method: "POST",
        headers: { "content-type": "application/xml; charset=utf-8" },
        body,
        signal: controller.signal,
      });
      const responseText = await response.text().catch(() => "");
      const elapsed = Date.now() - t0;

      if (!response.ok) {
        return { ok: false, error: `HTTP ${response.status}: ${responseText.slice(0, 300) || "errore"}`, elapsed_ms: elapsed };
      }
      if (/\b(error|fault|ko)\b/i.test(responseText)) {
        return { ok: false, error: responseText.slice(0, 300) || "RT_ERROR_RESPONSE", elapsed_ms: elapsed };
      }

      this.pushLog("INFO", `Test RT receipt OK → ${endpoint} (${elapsed}ms)`);
      return { ok: true, elapsed_ms: elapsed };
    } catch (err: unknown) {
      const elapsed = Date.now() - t0;
      const message = err instanceof Error ? err.message : String(err);
      this.pushLog("WARN", `Test RT receipt FAIL → ${endpoint}: ${message}`);
      return { ok: false, error: message, elapsed_ms: elapsed };
    } finally {
      clearTimeout(timeout);
    }
  }

  private pushLog(level: WorkerLogRow["level"], message: string) {
    const entry: WorkerLogRow = { at: new Date().toISOString(), level, message };
    this.logs.push(entry);
    if (this.logs.length > LOG_CAP) this.logs.shift();
    this.onMainLog?.(level, `[print-worker] ${message}`);
    if (this.boundWindow && !this.boundWindow.isDestroyed()) {
      this.boundWindow.webContents.send("desktop:printer-log", entry);
    }
  }

  private broadcastState() {
    if (this.boundWindow && !this.boundWindow.isDestroyed()) {
      this.boundWindow.webContents.send("desktop:printer-state", this.getPublicState());
    }
  }

  private async persistState() {
    await fs.mkdir(path.dirname(this.configPath), { recursive: true });
    await fs.writeFile(
      this.configPath,
      `${JSON.stringify({ config: this.config, session: this.savedSession }, null, 2)}\n`,
      "utf8",
    );
  }

  private async loadPersistedState() {
    try {
      const raw = await fs.readFile(this.configPath, "utf8");
      const parsed = JSON.parse(raw) as Record<string, unknown>;
      const savedConfig = (parsed.config && typeof parsed.config === "object"
        ? parsed.config
        : {}) as Record<string, unknown>;
      this.config = {
        consumerId: sanitizeConsumerId(savedConfig.consumerId ?? this.config.consumerId),
        deviceName: sanitizeDeviceName(savedConfig.deviceName ?? this.config.deviceName),
        pollMs: sanitizePollMs(savedConfig.pollMs ?? this.config.pollMs),
        claimLimit: sanitizeClaimLimit(savedConfig.claimLimit ?? this.config.claimLimit),
        autoStart: savedConfig.autoStart == null ? this.config.autoStart : Boolean(savedConfig.autoStart),
      };
      this.savedSession = toSessionSnapshot(parsed.session || null);
    } catch {
      // first run
    }
  }

  private ensureSupabaseClient() {
    const url = String(this.runtimeConfig.VITE_SUPABASE_URL || "").trim();
    const anonKey = String(this.runtimeConfig.VITE_SUPABASE_ANON_KEY || "").trim();
    if (!url || !anonKey) {
      throw new Error("Config desktop Supabase non valida.");
    }
    if (!this.supabase) {
      this.supabase = createClient(url, anonKey, {
        auth: {
          persistSession: false,
          autoRefreshToken: true,
          detectSessionInUrl: false,
        },
      });
    }
    return this.supabase;
  }

  private async ensureSignedIn() {
    const client = this.ensureSupabaseClient();
    const { data, error } = await client.auth.getUser();
    if (!error && data?.user) {
      this.authState.user = { id: data.user.id, email: data.user.email ?? null };
      if (!this.authState.restaurant) {
        this.authState.restaurant = await this.resolveRestaurantForCurrentUser(data.user.id);
      }
      return;
    }

    if (!this.savedSession) {
      throw new Error("Sessione desktop assente: effettua login nell'app.");
    }

    const { data: restored, error: restoreError } = await client.auth.setSession({
      access_token: this.savedSession.access_token,
      refresh_token: this.savedSession.refresh_token,
    });
    if (restoreError || !restored?.user) {
      throw restoreError || new Error("SESSION_INVALID");
    }

    this.authState.user = { id: restored.user.id, email: restored.user.email ?? null };
    this.authState.restaurant = await this.resolveRestaurantForCurrentUser(restored.user.id);
    const refreshedSnapshot = toSessionSnapshot(restored.session || null);
    if (refreshedSnapshot && !sameSession(this.savedSession, refreshedSnapshot)) {
      this.savedSession = refreshedSnapshot;
      await this.persistState();
    }
  }

  private async resolveRestaurantForCurrentUser(userId: string): Promise<RestaurantScope | null> {
    const client = this.ensureSupabaseClient();

    const { data: owned, error: ownerError } = await client
      .from("restaurants")
      .select("id,name,city")
      .eq("owner_id", userId)
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();
    if (ownerError) throw ownerError;
    if (owned?.id) {
      return {
        id: owned.id,
        name: owned.name,
        city: owned.city ?? null,
        role: "owner",
      };
    }

    const { data: roles, error: rolesError } = await client
      .from("user_roles")
      .select("role, restaurant_id, created_at")
      .eq("user_id", userId)
      .not("restaurant_id", "is", null)
      .in("role", ["admin", "manager", "staff"]);
    if (rolesError) throw rolesError;

    const rankedRows = (roles || [])
      .map((row) => ({
        role: row.role as RestaurantScope["role"],
        restaurant_id: String(row.restaurant_id || ""),
        created_at: String(row.created_at || ""),
      }))
      .filter((row) => Boolean(row.role) && Boolean(row.restaurant_id))
      .sort((a, b) => {
        const roleDelta = (roleRank[a.role] || 99) - (roleRank[b.role] || 99);
        if (roleDelta !== 0) return roleDelta;
        return new Date(a.created_at).getTime() - new Date(b.created_at).getTime();
      });

    const best = rankedRows[0];
    if (!best) return null;

    const { data: roleRestaurant, error: restaurantError } = await client
      .from("restaurants")
      .select("id,name,city")
      .eq("id", best.restaurant_id)
      .maybeSingle();
    if (restaurantError) throw restaurantError;
    if (!roleRestaurant?.id) return null;

    return {
      id: roleRestaurant.id,
      name: roleRestaurant.name,
      city: roleRestaurant.city ?? null,
      role: best.role,
    };
  }

  private async fetchAssignedPrinterId() {
    if (!this.authState.restaurant?.id) return null;
    const client = this.ensureSupabaseClient();
    const { data, error } = await client.rpc("printing_list_agents", {
      p_restaurant_id: this.authState.restaurant.id,
    });
    if (error) throw error;
    const rows = Array.isArray(data) ? data : [];
    const current = rows.find((row) => String((row as Record<string, unknown>).agent_id || "").trim() === this.config.consumerId);
    return String((current as Record<string, unknown> | undefined)?.printer_id || "").trim() || null;
  }

  private async heartbeatAgent(isActive: boolean) {
    if (!this.authState.restaurant?.id) return;
    try {
      const client = this.ensureSupabaseClient();
      let serverAssigned: string | null = null;
      try {
        serverAssigned = await this.fetchAssignedPrinterId();
      } catch (error) {
        this.pushLog("WARN", `Lettura assegnazione agente fallita: ${normalizeError(error)}`);
      }

      const { data, error } = await client.rpc("printing_register_agent", {
        p_restaurant_id: this.authState.restaurant.id,
        p_agent_id: this.config.consumerId,
        p_printer_id: serverAssigned || this.service.assignedPrinterId || null,
        p_device_name: this.config.deviceName,
        p_app_version: this.appVersion,
        p_is_active: isActive,
      });
      if (error) throw error;
      const nextAssigned =
        String((data as Record<string, unknown> | null)?.printer_id || "").trim() || serverAssigned || null;
      if (this.service.assignedPrinterId !== nextAssigned) {
        this.service.assignedPrinterId = nextAssigned;
        this.broadcastState();
      }
    } catch (error) {
      this.pushLog("WARN", `Heartbeat agente fallito: ${normalizeError(error)}`);
    }
  }

  private async fetchLivePrinterRoutes(restaurantId: string): Promise<LiveRoutes> {
    const client = this.ensureSupabaseClient();
    const { data, error } = await client
      .from("restaurants")
      .select("settings")
      .eq("id", restaurantId)
      .maybeSingle();
    if (error) throw error;
    const settings = (data?.settings && typeof data.settings === "object" ? data.settings : {}) as Record<string, unknown>;
    const printing =
      settings.printing && typeof settings.printing === "object" ? (settings.printing as Record<string, unknown>) : {};
    const printersRaw = Array.isArray(printing.printers) ? printing.printers : [];
    const byId = new Map<string, LivePrinter>();
    const byDepartment = new Map<string, LivePrinter>();

    for (const rawPrinter of printersRaw) {
      const printer = (rawPrinter && typeof rawPrinter === "object" ? rawPrinter : {}) as Record<string, unknown>;
      const id = String(printer.id || "").trim();
      if (!id) continue;
      const mapped: LivePrinter = {
        id,
        name: String(printer.name || "").trim() || id,
        host: String(printer.host || "").trim(),
        port: sanitizePrinterPort(printer.port),
        enabled: printer.enabled !== false,
        departments: Array.isArray(printer.departments)
          ? printer.departments.map((entry) => normalizeDepartment(entry)).filter(Boolean)
          : [],
      };
      byId.set(id, mapped);
      if (mapped.enabled && mapped.host) {
        for (const dep of mapped.departments) {
          if (!byDepartment.has(dep)) byDepartment.set(dep, mapped);
        }
      }
    }

    return {
      byId,
      byDepartment,
      defaultPrinterId: String(printing.default_printer_id || "").trim() || null,
    };
  }

  private resolveRouteForJob(job: JobRow, liveRoutes: LiveRoutes | null) {
    const snapshotRoute = (job.route && typeof job.route === "object" ? job.route : {}) as Record<string, unknown>;
    const snapshotId = String(snapshotRoute.id || snapshotRoute.printer_id || "").trim() || null;
    const snapshotHost = String(snapshotRoute.host || "").trim();
    const snapshotPort = sanitizePrinterPort(snapshotRoute.port);
    const dep = normalizeDepartment(job.department);

    if (liveRoutes?.byId && snapshotId) {
      const live = liveRoutes.byId.get(snapshotId);
      if (live && live.enabled && live.host) {
        return { id: live.id, name: live.name, host: live.host, port: live.port };
      }
    }
    if (liveRoutes?.byDepartment) {
      const byDep = liveRoutes.byDepartment.get(dep);
      if (byDep && byDep.enabled && byDep.host) {
        return { id: byDep.id, name: byDep.name, host: byDep.host, port: byDep.port };
      }
    }
    if (liveRoutes?.byId && liveRoutes.defaultPrinterId) {
      const fallback = liveRoutes.byId.get(liveRoutes.defaultPrinterId);
      if (fallback && fallback.enabled && fallback.host) {
        return { id: fallback.id, name: fallback.name, host: fallback.host, port: fallback.port };
      }
    }
    if (snapshotHost) {
      return {
        id: snapshotId,
        name: String(snapshotRoute.name || snapshotId || snapshotHost),
        host: snapshotHost,
        port: snapshotPort,
      };
    }
    return null;
  }

  private async sendToPrinter(job: JobRow, routeOverride: { host: string; port: number } | null) {
    const host = String(routeOverride?.host || "").trim();
    const port = sanitizePrinterPort(routeOverride?.port);
    if (!host) throw new Error("NO_PRINTER_HOST");
    const payload = buildEscPosPayload(renderTicket(job));
    let lastError: unknown = null;
    for (let attempt = 1; attempt <= 2; attempt += 1) {
      try {
        await openRawSocket(host, port, payload);
        return;
      } catch (error) {
        lastError = error;
        if (attempt >= 2 || !shouldRetryPrintLocally(error)) break;
        await sleep(500);
      }
    }
    throw new Error(`${normalizeError(lastError)} (target ${host}:${port})`);
  }

  private async completePrintJob(job: JobRow, success: boolean, errorMessage: string | null) {
    const client = this.ensureSupabaseClient();
    const { error } = await client.rpc("print_complete_job", {
      p_job_id: job.id,
      p_consumer_id: this.config.consumerId,
      p_success: success,
      p_error: success ? null : String(errorMessage || "PRINT_FAILED").slice(0, 500),
      p_meta: {
        source: "desktop_all_in_one",
        device_name: this.config.deviceName,
        app_version: this.appVersion,
      },
    });
    if (error) throw error;
  }

  private async postPhysicalReceipt(route: Record<string, unknown>, payload: Record<string, unknown>) {
    const host = String(route.host ?? "").trim();
    const brand = normalizePhysicalBrand(route.brand);
    const port = sanitizePhysicalPort(route.port, brand);
    const apiPath = sanitizePhysicalPath(route.api_path, brand);
    if (!host) throw new Error("PHYSICAL_RT_HOST_MISSING");

    const endpoint = `http://${host}:${port}${apiPath}`;
    const body = buildEpsonFiscalReceiptXml(payload);
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 20000);

    try {
      const response = await fetch(endpoint, {
        method: "POST",
        headers: {
          "content-type": "application/xml; charset=utf-8",
        },
        body,
        signal: controller.signal,
      });
      const responseText = await response.text().catch(() => "");
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${responseText || "RT response error"}`);
      }
      if (/\b(error|fault|ko)\b/i.test(responseText)) {
        throw new Error(responseText.slice(0, 500) || "RT_ERROR_RESPONSE");
      }
      return extractReceiptIdFromResponse(responseText);
    } finally {
      clearTimeout(timeout);
    }
  }

  private async sendToPhysicalReceiptDevice(job: PhysicalReceiptJobRow) {
    const payload = (job.payload && typeof job.payload === "object" ? job.payload : {}) as Record<string, unknown>;
    const route = (payload.route && typeof payload.route === "object" ? payload.route : {}) as Record<string, unknown>;

    let lastError: unknown = null;
    for (let attempt = 1; attempt <= 2; attempt += 1) {
      try {
        const receiptId = await this.postPhysicalReceipt(route, payload);
        return receiptId || `RT-${String(job.id || "").slice(0, 8)}-${Date.now()}`;
      } catch (error) {
        lastError = error;
        if (attempt >= 2 || !shouldRetryPrintLocally(error)) break;
        await sleep(500);
      }
    }

    const host = String(route.host ?? "").trim() || "n/a";
    const brand = normalizePhysicalBrand(route.brand);
    const port = sanitizePhysicalPort(route.port, brand);
    throw new Error(`${normalizeError(lastError)} (target ${host}:${port})`);
  }

  private async completePhysicalReceiptJob(
    job: PhysicalReceiptJobRow,
    success: boolean,
    options: { receiptId?: string | null; error?: string | null } = {},
  ) {
    const client = this.ensureSupabaseClient();
    const { error } = await client.rpc("physical_receipt_complete_job", {
      p_job_id: job.id,
      p_consumer_id: this.config.consumerId,
      p_success: success,
      p_receipt_id: success ? String(options.receiptId || "").trim() || null : null,
      p_error: success ? null : String(options.error || "PHYSICAL_RECEIPT_FAILED").slice(0, 500),
      p_meta: {
        source: "desktop_all_in_one",
        device_name: this.config.deviceName,
        app_version: this.appVersion,
      },
    });
    if (error) throw error;
  }

  private async sendToNonFiscalReceiptPrinter(job: NonFiscalReceiptJobRow) {
    const payload = (job.payload && typeof job.payload === "object" ? job.payload : {}) as Record<string, unknown>;
    const route = (payload.route && typeof payload.route === "object" ? payload.route : {}) as Record<string, unknown>;
    const host = String(route.host ?? "").trim();
    const port = sanitizePrinterPort(route.port ?? 9100);
    if (!host) throw new Error("NO_PRINTER_HOST");
    const ticketText = renderNonFiscalReceiptTicket(job);
    const escPosPayload = buildEscPosPayload(ticketText);
    let lastError: unknown = null;
    for (let attempt = 1; attempt <= 2; attempt += 1) {
      try {
        await openRawSocket(host, port, escPosPayload);
        return;
      } catch (error) {
        lastError = error;
        if (attempt >= 2 || !shouldRetryPrintLocally(error)) break;
        await sleep(500);
      }
    }
    throw new Error(`${normalizeError(lastError)} (target ${host}:${port})`);
  }

  private async completeNonFiscalReceiptJob(job: NonFiscalReceiptJobRow, success: boolean, errorMessage: string | null) {
    const client = this.ensureSupabaseClient();
    const { error } = await client.rpc("non_fiscal_receipt_complete_job", {
      p_job_id: job.id,
      p_consumer_id: this.config.consumerId,
      p_success: success,
      p_error: success ? null : String(errorMessage || "PRINT_FAILED").slice(0, 500),
    });
    if (error) throw error;
  }

  private async runTick() {
    if (!this.service.running || this.service.processing) return;
    this.service.processing = true;
    this.broadcastState();
    try {
      await this.ensureSignedIn();
      if (!this.authState.restaurant?.id) throw new Error("Ristorante non risolto.");
      await this.heartbeatAgent(true);
      const client = this.ensureSupabaseClient();
      const { data, error } = await client.rpc("print_claim_jobs", {
        p_restaurant_id: this.authState.restaurant.id,
        p_consumer_id: this.config.consumerId,
        p_limit: this.config.claimLimit,
      });
      if (error) throw error;
      const jobs = (Array.isArray(data) ? data : []) as JobRow[];
      this.service.stats.claimed += jobs.length;
      if (jobs.length > 0) this.pushLog("INFO", `Claimati ${jobs.length} job`);

      let liveRoutes: LiveRoutes | null = null;
      try {
        liveRoutes = await this.fetchLivePrinterRoutes(this.authState.restaurant.id);
      } catch (routesError) {
        this.pushLog("WARN", `Risoluzione route live fallita: ${normalizeError(routesError)}`);
      }

      for (const job of jobs) {
        try {
          const route = this.resolveRouteForJob(job, liveRoutes);
          await this.sendToPrinter(job, route);
          await this.completePrintJob(job, true, null);
          this.service.stats.printed += 1;
          this.pushLog("INFO", `Stampato job ${String(job.id).slice(0, 8)} -> ${normalizeDepartment(job.department)}`);
        } catch (jobError) {
          const message = normalizeError(jobError);
          try {
            await this.completePrintJob(job, false, message);
          } catch (ackError) {
            this.pushLog("ERROR", `Ack failed job ${String(job.id).slice(0, 8)}: ${normalizeError(ackError)}`);
          }
          this.service.stats.failed += 1;
          this.pushLog("ERROR", `Errore job ${String(job.id).slice(0, 8)}: ${message}`);
        }
      }

      if (this.physicalReceiptRpcAvailable) {
        try {
          const { data: physicalData, error: physicalError } = await client.rpc("physical_receipt_claim_jobs", {
            p_restaurant_id: this.authState.restaurant.id,
            p_consumer_id: this.config.consumerId,
            p_limit: this.config.claimLimit,
          });

          if (physicalError) {
            if (isMissingRpcError(physicalError, "physical_receipt_claim_jobs")) {
              this.physicalReceiptRpcAvailable = false;
              this.pushLog("WARN", "RPC physical_receipt_claim_jobs non trovata: applica la migrazione RT fisico.");
            } else {
              throw physicalError;
            }
          } else {
            const physicalJobs = (Array.isArray(physicalData) ? physicalData : []) as PhysicalReceiptJobRow[];
            this.service.stats.claimed += physicalJobs.length;
            if (physicalJobs.length > 0) {
              this.pushLog("INFO", `Claimati ${physicalJobs.length} job RT fisico`);
            }

            for (const job of physicalJobs) {
              try {
                const receiptId = await this.sendToPhysicalReceiptDevice(job);
                await this.completePhysicalReceiptJob(job, true, { receiptId });
                this.service.stats.printed += 1;
                this.pushLog("INFO", `Emesso scontrino RT job ${String(job.id).slice(0, 8)}`);
              } catch (jobError) {
                const message = normalizeError(jobError);
                try {
                  await this.completePhysicalReceiptJob(job, false, { error: message });
                } catch (ackError) {
                  if (isMissingRpcError(ackError, "physical_receipt_complete_job")) {
                    this.physicalReceiptRpcAvailable = false;
                    this.pushLog("WARN", "RPC physical_receipt_complete_job non trovata: applica la migrazione RT fisico.");
                  } else {
                    this.pushLog("ERROR", `Ack failed RT job ${String(job.id).slice(0, 8)}: ${normalizeError(ackError)}`);
                  }
                }
                this.service.stats.failed += 1;
                this.pushLog("ERROR", `Errore RT job ${String(job.id).slice(0, 8)}: ${message}`);
              }
            }
          }
        } catch (physicalTickError) {
          this.pushLog("ERROR", `Tick RT fisico: ${normalizeError(physicalTickError)}`);
        }
      }

      if (this.nonFiscalReceiptRpcAvailable) {
        try {
          const { data: nfrData, error: nfrError } = await client.rpc("non_fiscal_receipt_claim_jobs", {
            p_restaurant_id: this.authState.restaurant.id,
            p_consumer_id: this.config.consumerId,
            p_limit: this.config.claimLimit,
          });

          if (nfrError) {
            if (isMissingRpcError(nfrError, "non_fiscal_receipt_claim_jobs")) {
              this.nonFiscalReceiptRpcAvailable = false;
              this.pushLog("WARN", "RPC non_fiscal_receipt_claim_jobs non trovata: applica la migrazione.");
            } else {
              throw nfrError;
            }
          } else {
            const nfrJobs = (Array.isArray(nfrData) ? nfrData : []) as NonFiscalReceiptJobRow[];
            this.service.stats.claimed += nfrJobs.length;
            if (nfrJobs.length > 0) {
              this.pushLog("INFO", `Claimati ${nfrJobs.length} job scontrino non fiscale`);
            }

            for (const job of nfrJobs) {
              try {
                await this.sendToNonFiscalReceiptPrinter(job);
                await this.completeNonFiscalReceiptJob(job, true, null);
                this.service.stats.printed += 1;
                this.pushLog("INFO", `Stampato scontrino non fiscale ${String(job.id).slice(0, 8)}`);
              } catch (jobError) {
                const message = normalizeError(jobError);
                try {
                  await this.completeNonFiscalReceiptJob(job, false, message);
                } catch (ackError) {
                  if (isMissingRpcError(ackError, "non_fiscal_receipt_complete_job")) {
                    this.nonFiscalReceiptRpcAvailable = false;
                  } else {
                    this.pushLog("ERROR", `Ack failed NFR job ${String(job.id).slice(0, 8)}: ${normalizeError(ackError)}`);
                  }
                }
                this.service.stats.failed += 1;
                this.pushLog("ERROR", `Errore NFR job ${String(job.id).slice(0, 8)}: ${message}`);
              }
            }
          }
        } catch (nfrTickError) {
          this.pushLog("ERROR", `Tick scontrino non fiscale: ${normalizeError(nfrTickError)}`);
        }
      }

      this.service.stats.lastRunAt = new Date().toISOString();
      this.service.stats.lastError = null;
    } catch (error) {
      const message = normalizeError(error);
      this.service.stats.lastError = message;
      this.pushLog("ERROR", `Tick error: ${message}`);
    } finally {
      this.service.processing = false;
      this.broadcastState();
      if (this.service.running) {
        this.service.timer = setTimeout(() => {
          void this.runTick();
        }, this.config.pollMs);
      }
    }
  }

  private async discoverNetworkPrinters(timeoutMs?: number) {
    const safeTimeout = Number.isFinite(Number(timeoutMs))
      ? Math.max(DISCOVERY_TIMEOUT_MIN, Math.min(DISCOVERY_TIMEOUT_MAX, Math.trunc(Number(timeoutMs))))
      : DISCOVERY_TIMEOUT_DEFAULT;
    const targets = collectLanTargets();
    const printers: DiscoverPrinter[] = [];

    await runWithConcurrency(targets, DISCOVERY_CONCURRENCY, async (target) => {
      let openPort: number | null = null;
      for (const port of DISCOVERY_PORTS) {
        const isOpen = await probeTcpPort(target.host, port, safeTimeout);
        if (isOpen) {
          openPort = port;
          break;
        }
      }
      if (openPort == null) return;
      printers.push({
        host: target.host,
        port: openPort,
        connection_type: target.connectionType,
        interface_name: target.interfaceName,
        interface_ip: target.interfaceIp,
        source: "lan_scan",
        label: `Network printer ${target.host}`,
      });
    });

    printers.sort((a, b) => a.host.localeCompare(b.host, "en", { numeric: true, sensitivity: "base" }));
    return {
      ok: true as const,
      generated_at: new Date().toISOString(),
      scanned_hosts: targets.length,
      timeout_ms: safeTimeout,
      scanned_ports: [...DISCOVERY_PORTS],
      printers,
    };
  }

  private async discoverNetworkRtDevices(timeoutMs?: number) {
    const safeTimeout = Number.isFinite(Number(timeoutMs))
      ? Math.max(DISCOVERY_TIMEOUT_MIN, Math.min(DISCOVERY_TIMEOUT_MAX, Math.trunc(Number(timeoutMs))))
      : DISCOVERY_TIMEOUT_DEFAULT;
    const targets = collectLanTargets();
    const devices: DiscoverRtDevice[] = [];

    await runWithConcurrency(targets, DISCOVERY_CONCURRENCY, async (target) => {
      const openPorts: number[] = [];
      for (const port of RT_DISCOVERY_PORTS) {
        const isOpen = await probeTcpPort(target.host, port, safeTimeout);
        if (isOpen) openPorts.push(port);
      }
      if (openPorts.length === 0) return;

      const preferredPort = openPorts.includes(8008) ? 8008 : openPorts.includes(80) ? 80 : openPorts[0];
      let brand = inferRtBrandByPort(preferredPort);

      // Lightweight fingerprint to improve brand inference where possible.
      const httpPort = openPorts.includes(80) ? 80 : openPorts.includes(8008) ? 8008 : openPorts.includes(443) ? 443 : null;
      if (httpPort != null) {
        const fingerprint = await fingerprintRtHttp(target.host, httpPort, safeTimeout);
        const inferred = inferRtBrandFromContent(fingerprint);
        if (inferred) {
          brand = inferred;
        }
      }

      devices.push({
        host: target.host,
        port: preferredPort,
        brand,
        api_path: defaultPhysicalPathByBrand(brand),
        connection_type: target.connectionType,
        interface_name: target.interfaceName,
        interface_ip: target.interfaceIp,
        source: "lan_scan",
        label: formatRtDeviceLabel(brand, target.host),
      });
    });

    devices.sort((a, b) => a.host.localeCompare(b.host, "en", { numeric: true, sensitivity: "base" }));
    return {
      ok: true as const,
      generated_at: new Date().toISOString(),
      scanned_hosts: targets.length,
      timeout_ms: safeTimeout,
      scanned_ports: [...RT_DISCOVERY_PORTS],
      devices,
    };
  }
}
