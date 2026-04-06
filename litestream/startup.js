#!/usr/bin/env node
"use strict";

/**
 * litestream/startup.js
 *
 * Logic đơn giản, rõ ràng:
 *   A) Local DB đã tồn tại → skip restore, replicate luôn
 *   B) Không có local DB + S3 có snapshot → restore (fail hard nếu lỗi)
 *   C) Không có local DB + S3 không có snapshot → start fresh, replicate luôn
 *
 * Nếu S3 check bị lỗi (network/credentials) → hard exit, không start với DB rỗng.
 */

const { spawnSync, spawn } = require("child_process");
const fs = require("fs");
const path = require("path");
const os = require("os");

// ──────────────────────────────────────────────────────────────────────
// Config
// ──────────────────────────────────────────────────────────────────────
const DB_PATH = process.env.DB_PATH || "/app/data/storage.sqlite";
const CONFIG_PATH = process.env.LITESTREAM_CONFIG || "/etc/litestream.yml";

// ──────────────────────────────────────────────────────────────────────
// Logging
// ──────────────────────────────────────────────────────────────────────
const log = (...a) => console.log("[startup]", ...a);
const warn = (...a) => console.error("[startup] ⚠", ...a);
const fatal = (msg) => {
  console.error("[startup] ✖ FATAL:", msg);
  process.exit(1);
};

// ──────────────────────────────────────────────────────────────────────
// Shell helper — trả về { ok, stdout, stderr, exitCode }
// ──────────────────────────────────────────────────────────────────────
function run(cmd, args, { timeout = 300_000 } = {}) {
  const r = spawnSync(cmd, args, { encoding: "utf8", timeout, maxBuffer: 10 * 1024 * 1024 });
  return {
    ok: r.status === 0,
    stdout: (r.stdout || "").trim(),
    stderr: (r.stderr || "").trim(),
    exitCode: r.status ?? -1,
  };
}

// ──────────────────────────────────────────────────────────────────────
// Validate config
// ──────────────────────────────────────────────────────────────────────
function validateConfig() {
  if (!fs.existsSync(CONFIG_PATH)) {
    fatal(`Config không tìm thấy: ${CONFIG_PATH}`);
  }
  // Đảm bảo thư mục chứa DB tồn tại
  fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
}

// ──────────────────────────────────────────────────────────────────────
// Case A: local DB đã tồn tại
// ──────────────────────────────────────────────────────────────────────
function localDbExists() {
  try {
    const stat = fs.statSync(DB_PATH);
    return stat.size > 0;
  } catch {
    return false;
  }
}

// ──────────────────────────────────────────────────────────────────────
// Case B/C: kiểm tra S3 có snapshot không
// Trả về: "has_snapshot" | "no_snapshot" | "error"
// ──────────────────────────────────────────────────────────────────────
function checkS3Snapshots() {
  log("Không có local DB — kiểm tra S3 snapshot...");

  const r = run("litestream", ["snapshots", "-config", CONFIG_PATH, DB_PATH]);

  if (r.stderr) {
    // In ra stderr để debug, nhưng không fail ngay
    r.stderr.split("\n").forEach((line) => warn(" ", line));
  }

  if (!r.ok) {
    // Lỗi thật (network, credentials, endpoint sai…)
    log("════════════════════════════════════════");
    fatal(
      `litestream snapshots thất bại (exit ${r.exitCode})\n` +
        `Không thể xác định trạng thái S3 — từ chối start với DB rỗng.\n` +
        `Nguyên nhân thường gặp:\n` +
        `  1. SUPABASE_PROJECT_REF sai hoặc chưa set\n` +
        `  2. LITESTREAM_ACCESS_KEY_ID / SECRET sai\n` +
        `  3. Bucket '${process.env.LITESTREAM_BUCKET || "?"}' chưa tạo trong Supabase Storage\n` +
        `  4. Endpoint S3 không đúng (kiểm tra litestream.yml)\n` +
        `  5. Network không reach được Supabase`,
    );
  }

  const hasData = r.stdout.split("\n").some((l) => l.trim().length > 0);
  return hasData ? "has_snapshot" : "no_snapshot";
}

// ──────────────────────────────────────────────────────────────────────
// Restore từ S3
// ──────────────────────────────────────────────────────────────────────
function restoreFromS3() {
  log("✅ Tìm thấy snapshot trên S3 — bắt đầu restore...");

  const tmpPath = path.join(os.tmpdir(), `storage.restore.${process.pid}.sqlite`);

  // Dọn dẹp file tạm cũ nếu có
  try {
    fs.unlinkSync(tmpPath);
  } catch {}

  const r = run("litestream", ["restore", "-config", CONFIG_PATH, "-o", tmpPath, DB_PATH], { timeout: 600_000 }); // 10 phút — DB lớn cần thêm thời gian

  if (!r.ok) {
    try {
      fs.unlinkSync(tmpPath);
    } catch {}
    fatal(
      `Restore thất bại (exit ${r.exitCode})\n${r.stderr}\n` +
        `Kiểm tra:\n` +
        `  1. Credentials S3 có đúng không?\n` +
        `  2. Network có reach được Supabase không?\n` +
        `  3. Bucket '${process.env.LITESTREAM_BUCKET || "?"}' có tồn tại không?`,
    );
  }

  // Verify file tạm tồn tại và có dữ liệu
  let stat;
  try {
    stat = fs.statSync(tmpPath);
  } catch {
    fatal(`Restore báo thành công nhưng không tìm thấy file tạm: ${tmpPath}`);
  }
  if (stat.size === 0) {
    try {
      fs.unlinkSync(tmpPath);
    } catch {}
    fatal("Restore tạo ra file rỗng — có thể snapshot bị lỗi.");
  }

  // Atomic move về DB_PATH
  try {
    fs.unlinkSync(DB_PATH);
  } catch {}
  fs.renameSync(tmpPath, DB_PATH);

  const finalStat = fs.statSync(DB_PATH);
  log(`✅ Restore thành công (${(finalStat.size / 1024).toFixed(1)} KB)`);
}

// ──────────────────────────────────────────────────────────────────────
// Exec litestream replicate (replace current process)
// ──────────────────────────────────────────────────────────────────────
function startReplicate() {
  log("Khởi động Litestream replication...");

  // Dùng spawn thay vì spawnSync để không block + forward signals đúng
  const child = spawn("litestream", ["replicate", "-config", CONFIG_PATH], {
    stdio: "inherit",
    detached: false,
  });

  child.on("error", (e) => fatal(`Không thể start litestream replicate: ${e.message}`));
  child.on("exit", (code, signal) => {
    if (signal) {
      log(`litestream replicate kết thúc do signal: ${signal}`);
      process.exit(0);
    }
    log(`litestream replicate thoát với code: ${code}`);
    process.exit(code ?? 1);
  });

  // Forward signals xuống child
  for (const sig of ["SIGTERM", "SIGINT", "SIGHUP"]) {
    process.on(sig, () => {
      log(`Nhận ${sig} — forward xuống litestream`);
      child.kill(sig);
    });
  }
}

// ──────────────────────────────────────────────────────────────────────
// Main
// ──────────────────────────────────────────────────────────────────────
function main() {
  log("════════════════════════════════════════");
  log(" Litestream Startup (Node.js)");
  log(` DB        : ${DB_PATH}`);
  log(` Config    : ${CONFIG_PATH}`);
  log(` Bucket    : ${process.env.LITESTREAM_BUCKET || "<not set>"}`);
  log(` Supabase  : ${process.env.SUPABASE_PROJECT_REF || "<not set>"}`);
  log("════════════════════════════════════════");

  validateConfig();

  // Case A: local DB đã có → replicate luôn
  if (localDbExists()) {
    const size = (fs.statSync(DB_PATH).size / 1024).toFixed(1);
    log(`✅ Local DB đã tồn tại (${size} KB) — bỏ qua restore`);
    startReplicate();
    return;
  }

  // Case B/C: không có local DB
  const s3State = checkS3Snapshots();

  if (s3State === "has_snapshot") {
    // Case B: có snapshot → restore bắt buộc thành công
    restoreFromS3();
  } else {
    // Case C: S3 kết nối OK nhưng chưa có data → fresh install
    log("ℹ Không tìm thấy snapshot trên S3 (fresh install) — bắt đầu với DB mới");
  }

  startReplicate();
}

main();
