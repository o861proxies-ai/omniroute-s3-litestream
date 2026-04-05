#!/usr/bin/env node
"use strict";

const https = require("https");
const http = require("http");
const { spawnSync } = require("child_process");
const crypto = require("crypto");

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
const LEADER_SVCS = ["litestream", "omniroute", "cloudflared"];
const FOLLOWER_STOP = ["cloudflared", "omniroute", "litestream"];

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
  log(`📝 Registered once: ${INSTANCE_ID} @ ${REGISTERED_AT}`);
}

function electLeader(instances) {
  let leader = null;
  let maxRegisteredAt = -1;
  for (const [id, data] of Object.entries(instances || {})) {
    const t = Number(data?.registered_at || 0);
    if (t > maxRegisteredAt) {
      maxRegisteredAt = t;
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
function dockerExec(args, { silent = false } = {}) {
  const r = spawnSync("docker", args, { encoding: "utf8", timeout: 30000 });
  if (!silent && r.stderr && r.status !== 0) process.stderr.write(r.stderr);
  return { ok: r.status === 0, stdout: (r.stdout || "").trim() };
}

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

function svcStart(service) {
  const cname = getContainerName(service);
  if (!cname) {
    warn(`Container không tìm thấy: ${service} (project=${COMPOSE_PROJECT})`);
    return false;
  }
  if (isRunning(service)) {
    log(`${service} đã chạy`);
    return true;
  }
  log(`▶ Starting ${service} (${cname})...`);
  const r = dockerExec(["start", cname]);
  r.ok ? log(`  ✅ ${service} started`) : warn(`  ✖ Failed: ${service}`);
  return r.ok;
}

function svcStop(service, graceSeconds = 10) {
  const cname = getContainerName(service);
  if (!cname) return;
  if (!isRunning(service)) return;
  log(`■ Stopping ${service} (grace=${graceSeconds}s)...`);
  const r = dockerExec(["stop", "-t", String(graceSeconds), cname]);
  r.ok ? log(`  ✅ ${service} stopped`) : warn(`  ✖ Failed stop: ${service}`);
}

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
      if (h === "unhealthy" || h === "missing") {
        warn(`  ${service}: ${h}`);
        return resolve(false);
      }
      waited += POLL / 1000;
      if (waited >= timeoutSec) {
        warn(`  ${service}: timeout`);
        return resolve(false);
      }
      log(`  ${service}: ${h} — ${waited}/${timeoutSec}s`);
      setTimeout(check, POLL);
    };
    check();
  });
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
    log("══════════════════════════════════════");
    log(`🎉 LEADER — ${INSTANCE_ID}`);
    log(`   Project: ${COMPOSE_PROJECT}`);
    log("══════════════════════════════════════");

    await pruneAllExceptSelf(instances);

    svcStart("litestream");
    const ok = await waitHealthy("litestream", 180);
    if (!ok) warn("Litestream chưa healthy — tiếp tục start app...");

    svcStart("omniroute");
    svcStart("cloudflared");
    log("✅ LEADER mode active");
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

    log("══════════════════════════════════════");
    log(`📡 FOLLOWER RETIRE — ${INSTANCE_ID}${reason ? ` (${reason})` : ""}`);
    log("══════════════════════════════════════");

    for (const svc of FOLLOWER_STOP) {
      svcStop(svc, svc === "omniroute" ? 35 : 10);
    }

    await rtdbDelete(`${LOCK_NODE}/${INSTANCE_ID}`).catch((e) => warn("delete self:", e.message));
    log("🧼 Đã stop services + xóa self entry khỏi RTDB");
  } finally {
    _transitioning = false;
  }
}

function leaderHealthCheck() {
  if (!IS_LEADER || IS_RETIRED) return;
  for (const svc of LEADER_SVCS) {
    if (!isRunning(svc)) {
      warn(`${svc} crashed — restart`);
      svcStart(svc);
    }
  }
}

// ──────────────────────────────────────────────────────────────────────
// 7. Evaluate role từ snapshot
// ──────────────────────────────────────────────────────────────────────
let _evaluating = false;

async function evaluateRole(instances) {
  if (_evaluating || IS_RETIRED || !instances || typeof instances !== "object") return;
  _evaluating = true;
  try {
    const leader = electLeader(instances);
    log(`📊 Instances: ${Object.keys(instances).length} | Leader: ${leader || "none"}`);

    if (!leader) {
      warn("Không có leader trong snapshot — register lại self");
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
// 8. SSE listener (chỉ xử lý join/leave thực sự)
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
      if (IS_RETIRED) return;
      warn("SSE end — reconnect 3s");
      scheduleSSEReconnect(3000);
    });
    res.on("error", (e) => {
      if (IS_RETIRED) return;
      warn("SSE error:", e.message, "— 3s");
      scheduleSSEReconnect(3000);
    });
  });

  _sseReq.on("error", (e) => {
    if (IS_RETIRED) return;
    warn("SSE req:", e.message);
    scheduleSSEReconnect(5000);
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

  if (path === "/") return true; // initial snapshot hoặc reset node

  // join/leave = chỉ quan tâm child-level thay đổi
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

  if (!shouldEvaluateFromEvent(parsed)) {
    return;
  }

  const instances = parsed?.path === "/" ? parsed?.data : null;
  if (instances && typeof instances === "object") {
    if (!instances[INSTANCE_ID] && !IS_LEADER) {
      log("SSE: self đã bị xóa và không phải leader — retire");
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
  if (_sseReconnectTimer) clearTimeout(_sseReconnectTimer);

  for (const svc of FOLLOWER_STOP) {
    svcStop(svc, svc === "omniroute" ? 10 : 5);
  }

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
  log("╔══════════════════════════════════════════════╗");
  log("║ Leader Elector v3 (SSE join/leave only)      ║");
  log("╠══════════════════════════════════════════════╣");
  log(`║ Instance      : ${INSTANCE_ID}`);
  log(`║ Project       : ${COMPOSE_PROJECT}`);
  log(`║ RTDB node     : ${LOCK_NODE}`);
  log(`║ registered_at : ${REGISTERED_AT}`);
  log("╚══════════════════════════════════════════════╝");

  if (COMPOSE_PROJECT === "omniroute-s3-litestream") {
    warn("COMPOSE_PROJECT_NAME không được inject đúng — dùng fallback hostname detection");
  }

  log("Init: stop toàn bộ managed services để start sạch...");
  for (const svc of FOLLOWER_STOP) {
    svcStop(svc, 5);
  }

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
  log("🚀 Elector running (không heartbeat PUT định kỳ)");
}

main().catch((e) => {
  console.error("[elector] Fatal:", e);
  process.exit(1);
});
