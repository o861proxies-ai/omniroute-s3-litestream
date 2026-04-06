#!/usr/bin/env node
"use strict";

const https = require("https");
const http = require("http");
const { spawnSync } = require("child_process");
const crypto = require("crypto");
const os = require("os");

// ──────────────────────────────────────────────────────────────────────
// 1. Config
// ──────────────────────────────────────────────────────────────────────
const RTDB_URL_RAW = process.env.RTDB_URL;
if (!RTDB_URL_RAW) {
  console.error("[elector] RTDB_URL is required");
  process.exit(1);
}

const urlObj = new URL(RTDB_URL_RAW);
const RTDB_BASE = `${urlObj.protocol}//${urlObj.host}`;
const RTDB_QUERY = urlObj.search;

const _rawProject = (process.env.COMPOSE_PROJECT_NAME || "").trim();
const COMPOSE_PROJECT =
  _rawProject && _rawProject !== "COMPOSE_PROJECT_NAME"
    ? _rawProject
        .toLowerCase()
        .replace(/[^a-z0-9-]/g, "-")
        .replace(/^-+|-+$/g, "")
    : (() => {
        const hn = require("os").hostname();
        const parts = hn.split("-");
        if (parts.length > 2) return parts.slice(0, -2).join("-");
        return "omniroute-s3-litestream";
      })();

const _rawInstance = (process.env.INSTANCE_ID || "").trim();
const INSTANCE_ID = _rawInstance && _rawInstance !== "INSTANCE_ID" ? _rawInstance : crypto.randomBytes(8).toString("hex");

const LOCK_NODE = `leader-lock-${COMPOSE_PROJECT}/instances`;
const LEADER_CORE_SVCS = ["litestream", "omniroute", "cloudflared"];
const FOLLOWER_STOP_FALLBACK = ["cloudflared", "omniroute", "litestream", "dozzle", "filebrowser"];
const KEEP_SERVICES = (process.env.ELECTOR_KEEP_SERVICES || "elector")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const STOP_SELF_ON_FOLLOWER = (process.env.ELECTOR_STOP_SELF_ON_FOLLOWER || "true").toLowerCase() === "true";

// Compose file & env — mounted vào elector container từ host workspace
const COMPOSE_FILE = "/workspace/docker-compose.yml";
const ENV_FILE = "/workspace/.env";

// ──────────────────────────────────────────────────────────────────────
// 2. Logging
// ──────────────────────────────────────────────────────────────────────
const ts = () => new Date().toTimeString().slice(0, 8);
const log = (...a) => console.log(`[elector ${ts()}]`, ...a);
const warn = (...a) => console.error(`[elector ${ts()}] ⚠`, ...a);

// ──────────────────────────────────────────────────────────────────────
// 3. RTDB REST helpers
// ──────────────────────────────────────────────────────────────────────
function buildUrl(path) {
  return `${RTDB_BASE}/${path}.json${RTDB_QUERY}`;
}

function rtdbRequest(method, path, body = null, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    const url = new URL(buildUrl(path));
    const lib = url.protocol === "https:" ? https : http;
    const opts = {
      hostname: url.hostname,
      port: url.port || (url.protocol === "https:" ? 443 : 80),
      path: url.pathname + url.search,
      method,
      headers: { "Content-Type": "application/json" },
    };
    const req = lib.request(opts, (res) => {
      let data = "";
      res.on("data", (c) => (data += c));
      res.on("end", () => {
        try {
          resolve({ status: res.statusCode, body: JSON.parse(data) });
        } catch {
          resolve({ status: res.statusCode, body: data });
        }
      });
    });
    req.setTimeout(timeoutMs, () => {
      req.destroy();
      reject(new Error("RTDB timeout"));
    });
    req.on("error", reject);
    if (body !== null) req.write(JSON.stringify(body));
    req.end();
  });
}

const rtdbGet = (path) => rtdbRequest("GET", path);
const rtdbPut = (path, body) => rtdbRequest("PUT", path, body);
const rtdbDelete = (path) => rtdbRequest("DELETE", path);

// ──────────────────────────────────────────────────────────────────────
// 4. Instance registry
// ──────────────────────────────────────────────────────────────────────
const REGISTERED_AT = Date.now();
const makeSelfPayload = () => ({ registered_at: REGISTERED_AT });

async function registerSelf() {
  await rtdbPut(`${LOCK_NODE}/${INSTANCE_ID}`, makeSelfPayload());
  log(`📝 Registered: ${INSTANCE_ID} @ ${REGISTERED_AT}`);
}

function electLeader(instances) {
  let leader = null;
  let maxTs = -1;
  for (const [id, data] of Object.entries(instances || {})) {
    const t = Number(data?.registered_at || 0);
    if (t > maxTs) {
      maxTs = t;
      leader = id;
    }
  }
  return leader;
}

async function pruneAllExceptSelf(instances) {
  const ids = Object.keys(instances || {}).filter((id) => id !== INSTANCE_ID);
  if (!ids.length) return;
  log(`🧹 Leader cleanup: xóa ${ids.length} instance cũ khỏi RTDB`);
  for (const id of ids) {
    await rtdbDelete(`${LOCK_NODE}/${id}`).catch((e) => warn(`delete ${id}:`, e.message));
  }
}

// ──────────────────────────────────────────────────────────────────────
// 5. Docker helpers
// ──────────────────────────────────────────────────────────────────────
function dockerExec(args, { silent = false, timeout = 90000 } = {}) {
  const r = spawnSync("docker", args, { encoding: "utf8", timeout });
  if (!silent && r.stderr && r.status !== 0) process.stderr.write(r.stderr);
  return { ok: r.status === 0, stdout: (r.stdout || "").trim() };
}

function detectHostWorkspaceDir() {
  if ((process.env.CUR_WORK_DIR || "").trim()) {
    return process.env.CUR_WORK_DIR.trim();
  }

  // Trong container elector, hostname thường là container id.
  // Inspect mount /workspace để lấy source path thực trên host.
  const selfId = os.hostname();
  const inspect = dockerExec(
    [
      "inspect",
      "-f",
      "{{range .Mounts}}{{if eq .Destination \"/workspace\"}}{{.Source}}{{end}}{{end}}",
      selfId,
    ],
    { silent: true, timeout: 15000 },
  );

  if (inspect.ok && inspect.stdout) {
    return inspect.stdout;
  }

  return null;
}

const HOST_WORKSPACE_DIR = detectHostWorkspaceDir();

// ── Docker Compose helpers ────────────────────────────────────────────
function composeExec(args, opts = {}) {
  const env = { ...process.env };
  if (HOST_WORKSPACE_DIR) env.CUR_WORK_DIR = HOST_WORKSPACE_DIR;
  const r = spawnSync("docker", ["compose", "-f", COMPOSE_FILE, "--env-file", ENV_FILE, "-p", COMPOSE_PROJECT, ...args], {
    encoding: "utf8",
    timeout: opts.timeout || 90000,
    env,
  });
  if (!opts.silent && r.stderr && r.status !== 0) process.stderr.write(r.stderr);
  return { ok: r.status === 0, stdout: (r.stdout || "").trim() };
}

function composeStopRemove(service, graceSec = 10) {
  log(`  ■ compose stop ${service} (grace=${graceSec}s)`);
  composeExec(["stop", "-t", String(graceSec), service], { silent: true, timeout: (graceSec + 30) * 1000 });
  composeExec(["rm", "-f", service], { silent: true, timeout: 30000 });
}

function composeDown(services) {
  log(`🔽 compose down [${services.join(", ")}]`);
  for (const svc of services) {
    const grace = svc === "omniroute" ? 35 : 10;
    composeStopRemove(svc, grace);
  }
  log("  ✅ compose down done");
}

function composeUp(service) {
  log(`🔼 compose up -d ${service}`);
  const r = composeExec(["up", "-d", service]);
  r.ok ? log(`  ✅ ${service} up`) : warn(`  ✖ compose up failed: ${service} — stdout: ${r.stdout}`);
  return r.ok;
}

function listComposeServices() {
  const r = composeExec(["config", "--services"], { silent: true, timeout: 30000 });
  if (!r.ok || !r.stdout) return [];
  return r.stdout
    .split("\n")
    .map((s) => s.trim())
    .filter(Boolean);
}

function followerStopServices() {
  const fromCompose = listComposeServices().filter((svc) => !KEEP_SERVICES.includes(svc));
  if (fromCompose.length) return fromCompose;
  return FOLLOWER_STOP_FALLBACK.filter((svc) => !KEEP_SERVICES.includes(svc));
}

function stopSelfElector(reason = "") {
  if (!STOP_SELF_ON_FOLLOWER) return;
  const selfId = os.hostname();
  log(`🛑 stopping self elector container${reason ? ` (${reason})` : ""}: ${selfId}`);
  dockerExec(["stop", "-t", "5", selfId], { silent: true, timeout: 15000 });
}

// ── State inspection helpers ──────────────────────────────────────────
function getContainerName(service) {
  const r = dockerExec(
    [
      "ps",
      "-a",
      "--filter",
      `label=com.docker.compose.service=${service}`,
      "--filter",
      `label=com.docker.compose.project=${COMPOSE_PROJECT}`,
      "--format",
      "{{.Names}}",
    ],
    { silent: true },
  );
  return r.stdout.split("\n").filter(Boolean)[0] || null;
}

function isRunning(service) {
  const cname = getContainerName(service);
  if (!cname) return false;
  const r = dockerExec(["inspect", "-f", "{{.State.Running}}", cname], { silent: true });
  return r.stdout === "true";
}

function getHealth(service) {
  const cname = getContainerName(service);
  if (!cname) return "missing";
  const r = dockerExec(["inspect", "-f", "{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}", cname], { silent: true });
  return r.stdout || "unknown";
}

// ──────────────────────────────────────────────────────────────────────
// FIX 2: waitHealthy — retry khi "unhealthy", không thoát sớm
//
// Trước: unhealthy → resolve(false) ngay lập tức
//   → waitLitestreamReady fail ngay khi litestream vừa start (đang trong
//     start_period, health = unhealthy là bình thường)
//   → omniroute được start trước khi restore xong
//
// Sau: unhealthy → retry cho đến timeout
//   → chỉ thoát khi hết timeoutSec hoặc trạng thái là "missing"
//     sau khi đã chờ đủ lâu
// ──────────────────────────────────────────────────────────────────────
function waitHealthy(service, timeoutSec = 180) {
  return new Promise((resolve) => {
    const POLL = 5000;
    let waited = 0;
    const check = () => {
      const h = getHealth(service);
      if (h === "healthy" || h === "no-healthcheck") {
        log(`  ${service}: ${h} ✅`);
        return resolve(true);
      }
      // FIX 2: bỏ early-exit khi unhealthy/missing — luôn retry đến timeout
      // Lý do: trong start_period (120s), litestream đang restore từ S3
      //        nên health = "unhealthy" là bình thường, không phải lỗi cuối
      waited += POLL / 1000;
      if (waited >= timeoutSec) {
        warn(`  ${service}: timeout sau ${timeoutSec}s (trạng thái cuối: ${h})`);
        return resolve(false);
      }
      log(`  ${service}: ${h} — ${waited}/${timeoutSec}s`);
      setTimeout(check, POLL);
    };
    check();
  });
}

/**
 * Chờ litestream healthy VÀ xác nhận DB file tồn tại và có dữ liệu.
 * Đảm bảo S3 restore hoàn thành TRƯỚC khi omniroute được start.
 */
async function waitLitestreamReady(timeoutSec = 180) {
  log(`⏳ Waiting litestream + S3 sync (max ${timeoutSec}s)...`);

  // Bước 1: chờ healthcheck pass (= restore xong, replicate đang chạy)
  const healthy = await waitHealthy("litestream", timeoutSec);
  if (!healthy) {
    warn("litestream không healthy sau timeout");
    return false;
  }

  // Bước 2: verify DB file tồn tại và có dữ liệu (>0 bytes)
  const cname = getContainerName("litestream");
  if (!cname) {
    warn("litestream container không tìm thấy sau healthy");
    return false;
  }

  const r = dockerExec(
    ["exec", cname, "sh", "-c", "test -f /app/data/storage.sqlite && test -s /app/data/storage.sqlite && echo DB_OK || echo DB_EMPTY"],
    { silent: true },
  );

  if (r.stdout.includes("DB_OK")) {
    log("✅ litestream: healthy + DB file verified (có dữ liệu)");
    return true;
  }

  if (r.stdout.includes("DB_EMPTY")) {
    warn("litestream: healthy nhưng DB file rỗng (có thể là fresh install — tiếp tục)");
    return true;
  }

  warn("litestream: không thể kiểm tra DB file — tiếp tục cautiously");
  return true;
}

// ──────────────────────────────────────────────────────────────────────
// 6. Role transitions
// ──────────────────────────────────────────────────────────────────────
let IS_LEADER = false;
let IS_RETIRED = false;
let _transitioning = false;

async function onBecomeLeader(instances) {
  if (IS_RETIRED || IS_LEADER || _transitioning) return;
  _transitioning = true;
  try {
    IS_LEADER = true;
    log("══════════════════════════════════════════════");
    log(`🎉 LEADER — ${INSTANCE_ID}`);
    log(`   Project: ${COMPOSE_PROJECT}`);
    log("══════════════════════════════════════════════");

    await pruneAllExceptSelf(instances);

    log("🧹 compose down managed services (clean start)...");
    composeDown(LEADER_CORE_SVCS);

    composeUp("litestream");

    const dbReady = await waitLitestreamReady(180);
    if (!dbReady) {
      warn("⚠ litestream không ready — vẫn tiếp tục start app (có thể DB rỗng)");
    }

    composeUp("omniroute");
    composeUp("cloudflared");
    log("══════════════════════════════════════════════");
    log("✅ LEADER mode fully active");
    log("══════════════════════════════════════════════");
  } finally {
    _transitioning = false;
  }
}

async function onFollowerRetire(reason = "") {
  if (IS_RETIRED || _transitioning) return;
  _transitioning = true;
  try {
    IS_LEADER = false;
    IS_RETIRED = true;

    log("══════════════════════════════════════════════");
    log(`📡 FOLLOWER RETIRE — ${INSTANCE_ID}${reason ? ` (${reason})` : ""}`);
    log("══════════════════════════════════════════════");

    composeDown(followerStopServices());

    await rtdbDelete(`${LOCK_NODE}/${INSTANCE_ID}`).catch((e) => warn("delete self:", e.message));
    log("🧼 Retired: containers removed + RTDB entry deleted");
    stopSelfElector("follower retire");
  } finally {
    _transitioning = false;
  }
}

// ──────────────────────────────────────────────────────────────────────
// FIX 3: leaderHealthCheck — guard bằng _transitioning
//
// Trước: leaderHealthCheck() gọi ngay sau onBecomeLeader() kết thúc
//   → _transitioning vừa set false, nhưng container vừa compose up
//     chưa kịp chuyển sang Running
//   → isRunning() = false → compose up lại thừa (double start)
//
// Sau: thêm guard _transitioning
//   → nếu đang trong bất kỳ transition nào, skip health-check
//   → tránh race condition giữa onBecomeLeader và leaderHealthCheck
// ──────────────────────────────────────────────────────────────────────
function leaderHealthCheck() {
  // FIX 3: thêm || _transitioning để tránh race ngay sau onBecomeLeader
  if (!IS_LEADER || IS_RETIRED || _transitioning) return;
  for (const svc of LEADER_CORE_SVCS) {
    if (!isRunning(svc)) {
      warn(`${svc} không chạy — compose up lại`);
      composeUp(svc);
    }
  }
}

// ──────────────────────────────────────────────────────────────────────
// 7. Evaluate role từ RTDB snapshot
// ──────────────────────────────────────────────────────────────────────
let _evaluating = false;

async function evaluateRole(instances) {
  if (_evaluating || IS_RETIRED || !instances || typeof instances !== "object") return;
  _evaluating = true;
  try {
    const leader = electLeader(instances);
    log(`📊 Instances: ${Object.keys(instances).length} | Leader: ${leader || "none"}`);

    if (!leader) {
      warn("Không có leader — register lại self");
      await registerSelf();
      return;
    }

    if (leader === INSTANCE_ID) {
      await onBecomeLeader(instances);
      leaderHealthCheck();
      return;
    }

    await onFollowerRetire(`leader mới: ${leader}`);
  } finally {
    _evaluating = false;
  }
}

// ──────────────────────────────────────────────────────────────────────
// 8. SSE listener
// ──────────────────────────────────────────────────────────────────────
let _sseReq = null;
let _sseReconnectTimer = null;

function startSSE() {
  if (IS_RETIRED) return;

  if (_sseReconnectTimer) {
    clearTimeout(_sseReconnectTimer);
    _sseReconnectTimer = null;
  }

  const sseUrl = new URL(buildUrl(LOCK_NODE));
  const lib = sseUrl.protocol === "https:" ? https : http;
  const opts = {
    hostname: sseUrl.hostname,
    port: sseUrl.port || (sseUrl.protocol === "https:" ? 443 : 80),
    path: sseUrl.pathname + sseUrl.search,
    method: "GET",
    headers: { Accept: "text/event-stream", "Cache-Control": "no-cache" },
  };

  log(`🔌 SSE connecting: ${RTDB_BASE}/${LOCK_NODE}`);

  _sseReq = lib.request(opts, (res) => {
    if (res.statusCode !== 200) {
      warn(`SSE HTTP ${res.statusCode} — reconnect 5s`);
      scheduleSSEReconnect(5000);
      return;
    }
    log("✅ SSE connected");

    let buf = "";
    let eventName = "";

    res.on("data", (chunk) => {
      buf += chunk.toString();
      const lines = buf.split("\n");
      buf = lines.pop();

      for (const line of lines) {
        const t = line.trim();
        if (!t) {
          eventName = "";
          continue;
        }
        if (t.startsWith("event:")) {
          eventName = t.slice(6).trim();
        } else if (t.startsWith("data:")) {
          handleSSEEvent(eventName || "put", t.slice(5).trim()).catch((e) => warn("SSE handler:", e.message));
        }
      }
    });

    res.on("end", () => {
      if (!IS_RETIRED) {
        warn("SSE end — 3s");
        scheduleSSEReconnect(3000);
      }
    });
    res.on("error", (e) => {
      if (!IS_RETIRED) {
        warn("SSE err:", e.message, "— 3s");
        scheduleSSEReconnect(3000);
      }
    });
  });

  _sseReq.on("error", (e) => {
    if (!IS_RETIRED) {
      warn("SSE req:", e.message);
      scheduleSSEReconnect(5000);
    }
  });
  _sseReq.end();
}

function scheduleSSEReconnect(ms) {
  if (IS_RETIRED) return;
  if (_sseReq) {
    try {
      _sseReq.destroy();
    } catch {}
    _sseReq = null;
  }
  _sseReconnectTimer = setTimeout(startSSE, ms);
}

function shouldEvaluateFromEvent(parsed) {
  const path = parsed?.path;
  const data = parsed?.data;

  if (path === "/") return true;

  if (/^\/[^/]+$/.test(path)) {
    const isJoin = data && typeof data === "object" && data.registered_at;
    const isLeave = data === null;
    return isJoin || isLeave;
  }

  return false;
}

async function handleSSEEvent(event, raw) {
  if (IS_RETIRED) return;

  if (event === "cancel") {
    warn("SSE cancel");
    scheduleSSEReconnect(5000);
    return;
  }
  if (event === "auth_revoked") {
    warn("SSE auth_revoked");
    scheduleSSEReconnect(10000);
    return;
  }

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return;
  }

  if (!shouldEvaluateFromEvent(parsed)) return;

  const instances = parsed?.path === "/" ? parsed?.data : null;
  if (instances && typeof instances === "object") {
    if (!instances[INSTANCE_ID] && !IS_LEADER) {
      log("SSE: self bị xóa và không phải leader — retire");
      await onFollowerRetire("self missing");
      return;
    }
    await evaluateRole(instances);
    return;
  }

  const snap = await rtdbGet(LOCK_NODE);
  const live = snap.status === 200 && snap.body && typeof snap.body === "object" ? snap.body : null;

  if (!live) {
    warn("Node trống — register lại self");
    await registerSelf();
    return;
  }
  if (!live[INSTANCE_ID] && !IS_LEADER) {
    await onFollowerRetire("self missing after refresh");
    return;
  }
  await evaluateRole(live);
}

// ──────────────────────────────────────────────────────────────────────
// 9. Graceful shutdown
// ──────────────────────────────────────────────────────────────────────
let _shuttingDown = false;

async function shutdown(signal) {
  if (_shuttingDown) return;
  _shuttingDown = true;
  log(`🛑 Shutdown (${signal})`);

  if (_sseReq) {
    try {
      _sseReq.destroy();
    } catch {}
  }
  if (_sseReconnectTimer) {
    clearTimeout(_sseReconnectTimer);
  }

  composeDown(followerStopServices());

  await rtdbDelete(`${LOCK_NODE}/${INSTANCE_ID}`).catch(() => {});
  log(`Goodbye — ${INSTANCE_ID}`);
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("uncaughtException", (e) => warn("uncaughtException:", e.message));
process.on("unhandledRejection", (r) => warn("unhandledRejection:", r));

// ──────────────────────────────────────────────────────────────────────
// 10. Main
// ──────────────────────────────────────────────────────────────────────
async function main() {
  log("╔══════════════════════════════════════════════════╗");
  log("║ Leader Elector v5 (fix waitHealthy + healthCheck) ║");
  log("╠══════════════════════════════════════════════════╣");
  log(`║ Instance      : ${INSTANCE_ID}`);
  log(`║ Project       : ${COMPOSE_PROJECT}`);
  log(`║ RTDB node     : ${LOCK_NODE}`);
  log(`║ registered_at : ${REGISTERED_AT}`);
  log(`║ Compose file  : ${COMPOSE_FILE}`);
  log(`║ Host workdir  : ${HOST_WORKSPACE_DIR || "(không detect được)"}`);
  log("╚══════════════════════════════════════════════════╝");

  if (COMPOSE_PROJECT === "omniroute-s3-litestream") {
    warn("COMPOSE_PROJECT_NAME không được inject — dùng fallback hostname");
  }

  log("Init: compose down managed services (clean start)...");
  composeDown(followerStopServices());

  await registerSelf();

  try {
    const snap = await rtdbGet(LOCK_NODE);
    const instances = snap.status === 200 && snap.body && typeof snap.body === "object" ? snap.body : { [INSTANCE_ID]: makeSelfPayload() };
    await evaluateRole(instances);
  } catch (e) {
    warn("Init evaluate:", e.message, "— assume leader");
    await onBecomeLeader({ [INSTANCE_ID]: makeSelfPayload() });
  }

  startSSE();
  log("🚀 Elector running (SSE-driven | compose down/up)");
}

main().catch((e) => {
  console.error("[elector] Fatal:", e);
  process.exit(1);
});
