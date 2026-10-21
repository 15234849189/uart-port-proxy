/* uart-proxy 前端逻辑：多标签页串口终端 + 指令库 + 序列 + 脚本 */
"use strict";

/* ---------------- 工具 ---------------- */
function $(sel, root) { return (root || document).querySelector(sel); }
function $$(sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); }
function el(tag, cls, text) {
  var n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text !== undefined && text !== null) n.textContent = text;
  return n;
}
function esc(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
function b64ToBytes(b64) {
  var bin = atob(b64), out = new Uint8Array(bin.length);
  for (var i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}
function fmtTs(ms) {
  var d = new Date(ms);
  function p(n, w) { n = String(n); while (n.length < (w || 2)) n = "0" + n; return n; }
  return p(d.getHours()) + ":" + p(d.getMinutes()) + ":" + p(d.getSeconds()) + "." + p(d.getMilliseconds(), 3);
}
function fmtBytes(n) {
  if (n < 1024) return n + " B";
  if (n < 1048576) return (n / 1024).toFixed(1) + " KB";
  return (n / 1048576).toFixed(2) + " MB";
}
function fmtDur(ms) {
  if (!ms || ms < 0) ms = 0;
  var s = Math.floor(ms / 1000);
  var h = Math.floor(s / 3600); s %= 3600;
  var m = Math.floor(s / 60); s %= 60;
  function p(n) { return (n < 10 ? "0" : "") + n; }
  return p(h) + ":" + p(m) + ":" + p(s);
}

/* ---------------- 全局状态 ---------------- */
var state = {
  version: "",
  token: localStorage.getItem("uartProxyToken") || "",
  ws: null,
  wsOk: false,
  sessions: new Map(),   // port_id -> {status, lines, paused, pending}
  activeId: null,
  commands: [],
  sequences: [],
  prefs: { display_mode: "text", show_ts: true, encoding: "utf-8" },
  history: JSON.parse(localStorage.getItem("uartProxyHistory") || "[]"),
  histIdx: -1,
  periodic: new Map(),   // cmdId -> {timer, ms, portId}
  editingCmd: null,
  editingSeq: null,
  currentScript: null,
};

var MAX_LINES = 5000;

/* ---------------- REST ---------------- */
function api(path, opts) {
  opts = opts || {};
  var url = path + (path.indexOf("?") >= 0 ? "&" : "?") + "token=" + encodeURIComponent(state.token);
  opts.headers = Object.assign({ "Content-Type": "application/json" }, opts.headers || {});
  return fetch(url, opts).then(function (r) {
    if (r.status === 401) { showTokenDialog(); throw new Error("需要访问令牌"); }
    return r.json().then(function (j) {
      if (!r.ok) throw new Error(j.error || ("HTTP " + r.status));
      return j;
    });
  });
}
function apiPost(path, body) { return api(path, { method: "POST", body: JSON.stringify(body || {}) }); }
function apiPut(path, body) { return api(path, { method: "PUT", body: JSON.stringify(body || {}) }); }
function apiDel(path) { return api(path, { method: "DELETE" }); }

/* ---------------- WebSocket ---------------- */
var wsRetry = 0;
function wsConnect() {
  var proto = location.protocol === "https:" ? "wss" : "ws";
  var ws = new WebSocket(proto + "://" + location.host + "/ws?token=" + encodeURIComponent(state.token));
  state.ws = ws;
  ws.onopen = function () {
    wsRetry = 0;
    setSrv(true);
    var ping = setInterval(function () {
      if (state.ws === ws && ws.readyState === 1) ws.send(JSON.stringify({ t: "ping" }));
      else clearInterval(ping);
    }, 20000);
  };
  ws.onmessage = function (ev) {
    var msg;
    try { msg = JSON.parse(ev.data); } catch (e) { return; }
    handleWs(msg);
  };
  ws.onclose = function () {
    setSrv(false);
    var delay = Math.min(5000, 500 * (++wsRetry));
    setTimeout(function () { if (state.ws === ws) wsConnect(); }, delay);
  };
}
function wsSend(obj) {
  if (state.ws && state.ws.readyState === 1) state.ws.send(JSON.stringify(obj));
}
function setSrv(ok) {
  var n = $("#srvStatus");
  n.className = "srv " + (ok ? "on" : "off");
  $("em", n).textContent = ok ? "已连接" : "未连接";
}

function handleWs(msg) {
  if (msg.t === "hello") {
    state.version = msg.version;
    state.commands = msg.commands || [];
    state.sequences = msg.sequences || [];
    state.prefs = Object.assign(state.prefs, msg.prefs || {});
    (msg.sessions || []).forEach(function (s) { ensureSession(s); });
    renderPrefs(); renderTabs(); renderCmdList(); renderSeqList();
    return;
  }
  if (msg.t === "pong") return;
  if (msg.t === "status") {
    if (msg.state === "closed") {
      var gone = state.sessions.get(msg.port_id);
      if (gone) { sysLine(gone, "会话已关闭"); removeSession(msg.port_id); }
      return;
    }
    ensureSession(msg);
    var tab = state.sessions.get(msg.port_id);
    tab.status = msg;
    renderTabs(); updateToolbar();
    if (msg.error) sysLine(tab, "状态 " + msg.state + ": " + msg.error, true);
    else sysLine(tab, "状态: " + msg.state);
    return;
  }
  if (msg.t === "data") {
    var tab = state.sessions.get(msg.port_id);
    if (!tab) return;
    addFrame(tab, msg.dir, msg.b64, msg.ts);
    return;
  }
  if (msg.t === "stats") {
    var tab2 = state.sessions.get(msg.port_id);
    if (!tab2) return;
    tab2.status = Object.assign(tab2.status || {}, { rx_bytes: msg.rx_bytes, tx_bytes: msg.tx_bytes, connected_since: msg.connected_since });
    updateStats();
    return;
  }
  if (msg.t === "automation") {
    var tab3 = state.sessions.get(msg.port_id);
    if (!tab3) return;
    var txt = "自动化[" + (msg.kind === "seq" ? "序列" : "脚本") + " " + msg.name + "] " +
      ({ running: "运行中", done: "完成", stopped: "已停止", error: "出错" }[msg.state] || msg.state);
    sysLine(tab3, txt, msg.state === "error");
    return;
  }
  if (msg.t === "script_log") {
    var tab4 = state.sessions.get(msg.port_id);
    if (!tab4) return;
    sysLine(tab4, "[脚本] " + msg.line);
    return;
  }
  if (msg.t === "error") {
    var tab5 = msg.port_id ? state.sessions.get(msg.port_id) : state.sessions.get(state.activeId);
    if (tab5) sysLine(tab5, "错误: " + msg.message, true);
    else alert(msg.message);
  }
}

/* ---------------- 会话/标签页 ---------------- */
function ensureSession(status) {
  var tab = state.sessions.get(status.port_id);
  if (!tab) {
    tab = { status: status, lines: [], paused: false, pending: [] };
    state.sessions.set(status.port_id, tab);
  } else {
    tab.status = Object.assign(tab.status || {}, status);
  }
  if (!state.activeId) state.activeId = status.port_id;
  $("#app").classList.remove("no-session");
  return tab;
}

function removeSession(portId) {
  stopPeriodicByPort(portId);
  state.sessions.delete(portId);
  if (state.activeId === portId) {
    state.activeId = null;
    var first = state.sessions.keys().next();
    if (!first.done) state.activeId = first.value;
  }
  renderTabs();
  if (!state.sessions.size) $("#app").classList.add("no-session");
}

function renderTabs() {
  var bar = $("#tabbar");
  bar.innerHTML = "";
  state.sessions.forEach(function (tab, pid) {
    var st = tab.status || {};
    var t = el("div", "tab" + (pid === state.activeId ? " on" : ""));
    t.appendChild(el("i", "dot " + (st.state || "")));
    t.appendChild(el("span", "", (st.cfg && st.cfg.port || "串口") + " @" + (st.cfg && st.cfg.baudrate || "")));
    var x = el("button", "x", "×");
    x.title = "断开";
    x.onclick = function (e) { e.stopPropagation(); apiPost("/api/ports/" + pid + "/close").catch(function () {}); };
    t.appendChild(x);
    t.onclick = function () { state.activeId = pid; renderTabs(); rerenderAll(); };
    bar.appendChild(t);
  });
  var add = el("button", "add", "+");
  add.title = "打开串口";
  add.onclick = showOpenDialog;
  bar.appendChild(add);
  updateToolbar();
  updateStats();
}

function activeTab() {
  return state.sessions.get(state.activeId) || null;
}

/* ---------------- 终端 ---------------- */
function addFrame(tab, dir, b64, ts) {
  if (tab.paused) {
    tab.pending.push({ dir: dir, b64: b64, ts: ts });
    if (tab.pending.length > 2000) tab.pending.shift();
    return;
  }
  appendFrame(tab, dir, b64, ts);
}

function appendFrame(tab, dir, b64, ts) {
  tab.lines.push({ dir: dir, b64: b64, ts: ts });
  if (tab.lines.length > MAX_LINES) tab.lines.shift();
  var log = $("#termLog");
  var stick = log.scrollTop + log.clientHeight >= log.scrollHeight - 30;
  log.appendChild(renderLine(dir, b64, ts));
  while (log.children.length > MAX_LINES) log.removeChild(log.firstChild);
  if (stick && tab === activeTab()) log.scrollTop = log.scrollHeight;
}

function payloadText(b64) {
  var bytes = b64ToBytes(b64);
  if (state.prefs.display_mode === "hex") {
    var arr = [];
    for (var i = 0; i < bytes.length; i++) arr.push(("0" + bytes[i].toString(16).toUpperCase()).slice(-2));
    return arr.join(" ");
  }
  try {
    var dec = new TextDecoder($("#selEnc").value || "utf-8");
    return dec.decode(bytes);
  } catch (e) {
    try { return decodeURIComponent(escape(String.fromCharCode.apply(null, bytes))); }
    catch (e2) { return "(二进制 " + bytes.length + " 字节)"; }
  }
}

function renderLine(dir, b64, ts) {
  var line = el("div", "line" + (dir === "sys" ? " sys" : ""));
  if (state.prefs.show_ts && ts) line.appendChild(el("span", "ts", "[" + fmtTs(ts) + "]"));
  if (dir === "rx" || dir === "tx") {
    line.appendChild(el("span", "dir " + dir, dir === "rx" ? "RX" : "TX"));
    line.appendChild(el("span", "payload " + dir, payloadText(b64)));
  } else {
    line.appendChild(el("span", "payload", String(b64)));
  }
  return line;
}

function sysLine(tab, text, isErr) {
  if (!tab) return;
  tab.lines.push({ dir: "sys", b64: text, ts: Date.now() });
  if (tab === activeTab()) {
    var log = $("#termLog");
    var stick = log.scrollTop + log.clientHeight >= log.scrollHeight - 30;
    var n = el("div", "line sys" + (isErr ? " err" : ""));
    n.appendChild(el("span", "ts", "[" + fmtTs(Date.now()) + "]"));
    n.appendChild(el("span", "payload", text));
    log.appendChild(n);
    if (stick) log.scrollTop = log.scrollHeight;
  }
}

function rerenderAll() {
  var log = $("#termLog");
  log.innerHTML = "";
  var tab = activeTab();
  if (!tab) return;
  tab.lines.forEach(function (f) {
    if (f.dir === "sys") {
      var n = el("div", "line sys");
      if (state.prefs.show_ts) n.appendChild(el("span", "ts", "[" + fmtTs(f.ts) + "]"));
      n.appendChild(el("span", "payload", f.b64));
      log.appendChild(n);
    } else {
      log.appendChild(renderLine(f.dir, f.b64, f.ts));
    }
  });
  log.scrollTop = log.scrollHeight;
  updateToolbar();
  updateStats();
}

function updateToolbar() {
  var tab = activeTab();
  var st = tab ? (tab.status || {}) : {};
  $("#btnPause").textContent = tab && tab.paused ? "继续" : "暂停";
  $("#btnRecord").textContent = st.recording ? "停止录制" : "录制";
  $("#btnRecord").classList.toggle("danger", !!st.recording);
  $("#btnDtr").classList.toggle("on", !!st.cfg && !!st.cfg.dtr);
  $("#btnRts").classList.toggle("on", !!st.cfg && !!st.cfg.rts);
  if (st.cfg) {
    if ($("#selEnc").value !== st.cfg.encoding) $("#selEnc").value = st.cfg.encoding || "utf-8";
  }
  $$("#segMode button").forEach(function (b) {
    b.classList.toggle("on", b.dataset.v === state.prefs.display_mode);
  });
  $("#ckTs").checked = !!state.prefs.show_ts;
}

function updateStats() {
  var tab = activeTab();
  if (!tab) { $("#statLine").textContent = ""; return; }
  var st = tab.status || {};
  var dur = st.connected_since ? Date.now() - st.connected_since : 0;
  $("#statLine").textContent = "RX " + fmtBytes(st.rx_bytes || 0) + " · TX " + fmtBytes(st.tx_bytes || 0) + " · " + fmtDur(dur) + (st.state && st.state !== "open" ? " · " + st.state : "");
}
setInterval(updateStats, 1000);

/* ---------------- 发送 ---------------- */
function doSend(data, mode, newline) {
  var tab = activeTab();
  if (!tab) { alert("请先打开串口"); return; }
  wsSend({ t: "send", port_id: state.activeId, data: data, mode: mode, newline: newline });
  if (data) {
    if (state.history[state.history.length - 1] !== data) {
      state.history.push(data);
      if (state.history.length > 50) state.history.shift();
      localStorage.setItem("uartProxyHistory", JSON.stringify(state.history));
    }
    state.histIdx = state.history.length;
  }
}

/* ---------------- 指令库 ---------------- */
function renderCmdList() {
  var box = $("#cmdList");
  box.innerHTML = "";
  if (!state.commands.length) {
    box.appendChild(el("div", "empty-tip", "暂无指令，点击「+ 新增」保存常用指令"));
    return;
  }
  state.commands.forEach(function (c) {
    var item = el("div", "item");
    var head = el("div", "it-head");
    var name = el("span", "it-name", c.name);
    name.title = "点击发送";
    name.onclick = function () {
      var nl = c.newline;
      var mode = c.mode;
      doSend(c.data, mode, nl);
    };
    head.appendChild(name);
    head.appendChild(el("span", "badge", c.mode === "hex" ? "HEX" : "TXT"));
    var loop = state.periodic.get(c.id);
    if (loop) head.appendChild(el("span", "badge loop", "循环 " + loop.ms + "ms"));
    var ops = el("div", "ops");
    var bPeriod = el("button", "", loop ? "■" : "⏱");
    bPeriod.title = loop ? "停止循环发送" : "设置循环发送";
    bPeriod.onclick = function (ev) { togglePeriodic(c, loop, ev.currentTarget); };
    var bEdit = el("button", "", "改");
    bEdit.onclick = function () { showCmdDialog(c); };
    var bDel = el("button", "del", "删");
    bDel.onclick = function () {
      if (!confirm("删除指令「" + c.name + "」？")) return;
      apiDel("/api/commands/" + c.id).then(loadCommands);
    };
    ops.appendChild(bPeriod); ops.appendChild(bEdit); ops.appendChild(bDel);
    head.appendChild(ops);
    item.appendChild(head);
    item.appendChild(el("div", "it-data", (c.mode === "hex" ? "" : "") + c.data.replace(/\n/g, "\\n")));
    var pr = el("div", "period-row" + (loop ? " show" : ""));
    pr.innerHTML = '<span class="muted">间隔</span><input type="number" min="50" step="50" value="' + (loop ? loop.ms : 1000) + '"><span class="muted">ms</span>';
    var prInput = $("input", pr);
    var bGo = el("button", "small primary", loop ? "应用" : "启动");
    bGo.onclick = function () {
      var ms = parseInt(prInput.value, 10);
      if (!ms || ms < 50) { alert("间隔至少 50ms"); return; }
      if (loop) { clearInterval(loop.timer); loop.timer = null; }
      startPeriodic(c, ms);
      renderCmdList();
    };
    pr.appendChild(bGo);
    item.appendChild(pr);
    box.appendChild(item);
  });
}

function startPeriodic(cmd, ms) {
  var tab = activeTab();
  if (!tab) { alert("请先打开串口"); return; }
  var pid = state.activeId;
  var timer = setInterval(function () {
    if (!state.sessions.has(pid)) { stopPeriodic(cmd.id); renderCmdList(); return; }
    wsSend({ t: "send", port_id: pid, data: cmd.data, mode: cmd.mode, newline: cmd.newline });
  }, ms);
  state.periodic.set(cmd.id, { timer: timer, ms: ms, portId: pid });
}

function togglePeriodic(cmd, loop, btn) {
  if (loop) {
    clearInterval(loop.timer);
    state.periodic.delete(cmd.id);
    renderCmdList();
    return;
  }
  var item = btn ? btn.closest(".item") : null;
  if (item) {
    var row = $(".period-row", item);
    if (row) row.classList.add("show");
  }
}

function stopPeriodic(cmdId) {
  var loop = state.periodic.get(cmdId);
  if (loop) clearInterval(loop.timer);
  state.periodic.delete(cmdId);
}

function stopPeriodicByPort(pid) {
  state.periodic.forEach(function (loop, id) {
    if (loop.portId === pid) stopPeriodic(id);
  });
}

function loadCommands() {
  return api("/api/commands").then(function (j) {
    state.commands = j.commands;
    renderCmdList();
  });
}

/* ---------------- 序列 ---------------- */
function renderSeqList() {
  var box = $("#seqList");
  box.innerHTML = "";
  if (!state.sequences.length) {
    box.appendChild(el("div", "empty-tip", "暂无序列，点击「+ 新增」组合多条指令"));
    return;
  }
  state.sequences.forEach(function (s) {
    var item = el("div", "item");
    var head = el("div", "it-head");
    var name = el("span", "it-name", s.name);
    name.title = "点击运行";
    name.onclick = function () { runSeq(s); };
    head.appendChild(name);
    head.appendChild(el("span", "badge", s.steps.length + " 步"));
    head.appendChild(el("span", "badge", s.loops > 0 ? "x" + s.loops : "∞"));
    var ops = el("div", "ops");
    var bRun = el("button", "", "▶");
    bRun.onclick = function () { runSeq(s); };
    var bStop = el("button", "", "■");
    bStop.onclick = function () {
      if (!state.activeId) return;
      apiPost("/api/automation/stop", { port_id: state.activeId });
    };
    var bEdit = el("button", "", "改");
    bEdit.onclick = function () { showSeqDialog(s); };
    var bDel = el("button", "del", "删");
    bDel.onclick = function () {
      if (!confirm("删除序列「" + s.name + "」？")) return;
      apiDel("/api/sequences/" + s.id).then(loadSeqs);
    };
    ops.appendChild(bRun); ops.appendChild(bStop); ops.appendChild(bEdit); ops.appendChild(bDel);
    head.appendChild(ops);
    item.appendChild(head);
    item.appendChild(el("div", "it-data", s.steps.map(function (st) { return st.data; }).join(" → ").replace(/\n/g, "\\n")));
    box.appendChild(item);
  });
}

function runSeq(s) {
  if (!state.activeId) { alert("请先打开串口"); return; }
  apiPost("/api/sequences/" + s.id + "/start", { port_id: state.activeId })
    .catch(function (e) { alert(e.message); });
}

function loadSeqs() {
  return api("/api/sequences").then(function (j) {
    state.sequences = j.sequences;
    renderSeqList();
  });
}

/* ---------------- 脚本 ---------------- */
function loadScripts() {
  return api("/api/scripts").then(function (j) {
    renderScriptList(j.scripts);
  });
}

function renderScriptList(scripts) {
  var box = $("#scriptList");
  box.innerHTML = "";
  if (!scripts.length) {
    box.appendChild(el("div", "empty-tip", "暂无脚本，点击「+ 新建」"));
    return;
  }
  scripts.forEach(function (s) {
    var item = el("div", "item");
    var head = el("div", "it-head");
    var name = el("span", "it-name", s.name);
    name.onclick = function () { openScript(s.name); };
    head.appendChild(name);
    var ops = el("div", "ops");
    var bRun = el("button", "", "▶");
    bRun.title = "运行";
    bRun.onclick = function () { runScript(s.name); };
    var bStop = el("button", "", "■");
    bStop.title = "停止";
    bStop.onclick = function () {
      if (!state.activeId) return;
      apiPost("/api/automation/stop", { port_id: state.activeId });
    };
    var bDel = el("button", "del", "删");
    bDel.onclick = function () {
      if (!confirm("删除脚本 " + s.name + "？")) return;
      apiDel("/api/scripts/" + s.name).then(function () {
        if (state.currentScript === s.name) { state.currentScript = null; $("#scriptCode").value = ""; $("#edName").textContent = "未选择"; }
        loadScripts();
      });
    };
    ops.appendChild(bRun); ops.appendChild(bStop); ops.appendChild(bDel);
    head.appendChild(ops);
    item.appendChild(head);
    box.appendChild(item);
  });
}

function openScript(name) {
  api("/api/scripts/" + name).then(function (j) {
    state.currentScript = name;
    $("#edName").textContent = name;
    $("#scriptCode").value = j.content;
  }).catch(function (e) { alert(e.message); });
}

function runScript(name) {
  if (!state.activeId) { alert("请先打开串口"); return; }
  apiPost("/api/scripts/" + name + "/run", { port_id: state.activeId })
    .catch(function (e) { alert(e.message); });
}

/* ---------------- 弹窗 ---------------- */
function openDlg(id) { $(id).classList.add("show"); }
function closeDlg(id) { $(id).classList.remove("show"); }
$$(".modal [data-close]").forEach(function (b) {
  b.onclick = function () { b.closest(".modal").classList.remove("show"); };
});

/* 打开串口 */
function showOpenDialog() {
  api("/api/ports").then(function (j) {
    var sel = $("#opPort");
    sel.innerHTML = "";
    if (!j.ports.length) {
      var o = el("option", "", "（未发现串口设备）");
      o.value = "";
      sel.appendChild(o);
    }
    j.ports.forEach(function (p) {
      var o = el("option", "", p.port + (p.desc ? "  —  " + p.desc : "") + (p.open ? "  [已打开]" : ""));
      o.value = p.port;
      o.disabled = !!p.open;
      sel.appendChild(o);
    });
    // 恢复上次参数（取第一个可用端口的记忆）
    var first = j.ports.filter(function (p) { return !p.open; })[0];
    if (first && first.saved_cfg) applySavedCfg(first.saved_cfg);
    openDlg("#dlgOpen");
  }).catch(function (e) { alert(e.message); });
}

function applySavedCfg(c) {
  if (c.baudrate) $("#opBaud").value = c.baudrate;
  if (c.bytesize) $("#opBytesize").value = String(c.bytesize);
  if (c.parity) $("#opParity").value = c.parity;
  if (c.stopbits) $("#opStopbits").value = String(c.stopbits);
  if (c.flow) $("#opFlow").value = c.flow;
  if (c.encoding) $("#opEnc").value = c.encoding;
  if (c.dtr !== undefined) $("#opDtr").checked = !!c.dtr;
  if (c.rts !== undefined) $("#opRts").checked = !!c.rts;
  if (c.auto_reconnect !== undefined) $("#opReconnect").checked = !!c.auto_reconnect;
}

$("#opRefresh").onclick = showOpenDialog;
$("#opPort").onchange = function () {
  api("/api/ports").then(function (j) {
    var p = j.ports.filter(function (x) { return x.port === $("#opPort").value; })[0];
    if (p && p.saved_cfg) applySavedCfg(p.saved_cfg);
  });
};
$("#opConfirm").onclick = function () {
  var port = $("#opPort").value;
  if (!port) return;
  apiPost("/api/ports/open", {
    port: port,
    baudrate: parseInt($("#opBaud").value, 10) || 115200,
    bytesize: parseInt($("#opBytesize").value, 10),
    parity: $("#opParity").value,
    stopbits: parseFloat($("#opStopbits").value),
    flow: $("#opFlow").value,
    encoding: $("#opEnc").value,
    dtr: $("#opDtr").checked,
    rts: $("#opRts").checked,
    auto_reconnect: $("#opReconnect").checked,
  }).then(function (j) {
    closeDlg("#dlgOpen");
    state.activeId = j.port_id;
    renderTabs();
  }).catch(function (e) { alert(e.message); });
};
$("#btnOpenPort").onclick = showOpenDialog;

/* 指令编辑 */
function showCmdDialog(cmd) {
  state.editingCmd = cmd || null;
  $("#dlgCmdTitle").textContent = cmd ? "编辑指令" : "新增指令";
  $("#cmdName").value = cmd ? cmd.name : "";
  $("#cmdData").value = cmd ? cmd.data : "";
  $("#cmdMode").value = cmd ? cmd.mode : "text";
  $("#cmdNewline").value = cmd ? cmd.newline : "none";
  openDlg("#dlgCmd");
}
$("#btnAddCmd").onclick = function () { showCmdDialog(null); };
$("#cmdSave").onclick = function () {
  var body = {
    name: $("#cmdName").value.trim(),
    data: $("#cmdData").value,
    mode: $("#cmdMode").value,
    newline: $("#cmdNewline").value,
  };
  if (!body.name || body.data === "") { alert("名称与内容不能为空"); return; }
  var p = state.editingCmd ? apiPut("/api/commands/" + state.editingCmd.id, body) : apiPost("/api/commands", body);
  p.then(function () { closeDlg("#dlgCmd"); loadCommands(); })
   .catch(function (e) { alert(e.message); });
};

/* 序列编辑 */
function stepRow(step) {
  step = step || {};
  var row = el("div", "step");
  row.innerHTML =
    '<span class="no"></span>' +
    '<input class="s-data" placeholder="发送内容">' +
    '<select class="s-mode"><option value="text">文本</option><option value="hex">HEX</option></select>' +
    '<input class="s-delay" type="number" min="0" step="50" title="发送后延时(ms)">' +
    '<button class="rm" title="删除步骤">✕</button>';
  $(".s-data", row).value = step.data || "";
  $(".s-mode", row).value = step.mode || "text";
  $(".s-delay", row).value = step.delay_ms !== undefined ? step.delay_ms : 100;
  $(".rm", row).onclick = function () { row.remove(); renumber(); };
  return row;
}
function renumber() {
  $$("#seqSteps .step .no").forEach(function (n, i) { n.textContent = i + 1; });
}
function showSeqDialog(seq) {
  state.editingSeq = seq || null;
  $("#dlgSeqTitle").textContent = seq ? "编辑序列" : "新增序列";
  $("#seqName").value = seq ? seq.name : "";
  $("#seqLoops").value = seq ? seq.loops : 1;
  var box = $("#seqSteps");
  box.innerHTML = "";
  (seq ? seq.steps : [{}]).forEach(function (s) { box.appendChild(stepRow(s)); });
  renumber();
  openDlg("#dlgSeq");
}
$("#btnAddSeq").onclick = function () { showSeqDialog(null); };
$("#seqAddStep").onclick = function () { $("#seqSteps").appendChild(stepRow({})); renumber(); };
$("#seqSave").onclick = function () {
  var steps = $$("#seqSteps .step").map(function (row) {
    return {
      data: $(".s-data", row).value,
      mode: $(".s-mode", row).value,
      newline: "none",
      delay_ms: parseInt($(".s-delay", row).value, 10) || 0,
    };
  }).filter(function (s) { return s.data !== ""; });
  var body = { name: $("#seqName").value.trim(), steps: steps, loops: parseInt($("#seqLoops").value, 10) || 0 };
  if (!body.name || !steps.length) { alert("名称与步骤不能为空"); return; }
  var p = state.editingSeq ? apiPut("/api/sequences/" + state.editingSeq.id, body) : apiPost("/api/sequences", body);
  p.then(function () { closeDlg("#dlgSeq"); loadSeqs(); })
   .catch(function (e) { alert(e.message); });
};

/* 脚本 */
$("#btnAddScript").onclick = function () {
  var name = prompt("脚本名称（不含 .py）：", "script_" + Date.now() % 10000);
  if (!name) return;
  var tpl = "# uart-proxy 脚本示例\n" +
    "send('AT', newline='crlf')\n" +
    "r = recv(timeout=2)\n" +
    "if r:\n" +
    "    log('收到: %r' % r['text'])\n" +
    "else:\n" +
    "    log('超时未收到数据')\n";
  apiPost("/api/scripts", { name: name, content: tpl }).then(function () {
    loadScripts();
    openScript(name.endsWith(".py") ? name : name + ".py");
  }).catch(function (e) { alert(e.message); });
};
$("#btnScriptSave").onclick = function () {
  if (!state.currentScript) { alert("请先选择脚本"); return; }
  apiPut("/api/scripts/" + state.currentScript, { content: $("#scriptCode").value })
    .then(function () { $("#edName").textContent = state.currentScript + " ✓"; })
    .catch(function (e) { alert(e.message); });
};

/* 记录 */
$("#btnRecords").onclick = function () {
  api("/api/records").then(function (j) {
    $("#recordsDir").textContent = "目录: " + j.dir;
    var box = $("#recordList");
    box.innerHTML = "";
    if (!j.records.length) box.appendChild(el("div", "empty-tip", "暂无记录文件"));
    j.records.forEach(function (r) {
      var item = el("div", "item rec");
      var a = el("a", "", r.name);
      a.href = "/api/records/" + encodeURIComponent(r.name) + "?token=" + encodeURIComponent(state.token);
      a.setAttribute("download", r.name);
      item.appendChild(a);
      item.appendChild(el("span", "size", fmtBytes(r.size)));
      box.appendChild(item);
    });
    openDlg("#dlgRecords");
  }).catch(function (e) { alert(e.message); });
};

/* 令牌 */
function showTokenDialog() {
  openDlg("#dlgToken");
}
$("#tokenSave").onclick = function () {
  state.token = $("#tokenInput").value.trim();
  localStorage.setItem("uartProxyToken", state.token);
  closeDlg("#dlgToken");
  location.reload();
};

/* ---------------- 工具栏/发送栏事件 ---------------- */
$$("#segMode button").forEach(function (b) {
  b.onclick = function () {
    state.prefs.display_mode = b.dataset.v;
    apiPut("/api/prefs", { display_mode: state.prefs.display_mode }).catch(function () {});
    renderTabs(); rerenderAll();
  };
});
$("#ckTs").onchange = function () {
  state.prefs.show_ts = $("#ckTs").checked;
  apiPut("/api/prefs", { show_ts: state.prefs.show_ts }).catch(function () {});
  rerenderAll();
};
$("#selEnc").onchange = function () {
  if (!state.activeId) return;
  apiPost("/api/ports/" + state.activeId + "/encoding", { encoding: $("#selEnc").value })
    .catch(function (e) { alert(e.message); });
};
$("#btnPause").onclick = function () {
  var tab = activeTab();
  if (!tab) return;
  if (!tab.paused) {
    tab.paused = true;
  } else {
    tab.paused = false;
    var pend = tab.pending.splice(0);
    pend.forEach(function (f) { appendFrame(tab, f.dir, f.b64, f.ts); });
    var log = $("#termLog");
    log.scrollTop = log.scrollHeight;
  }
  updateToolbar();
};
$("#btnClear").onclick = function () {
  var tab = activeTab();
  if (!tab) return;
  tab.lines = [];
  tab.pending = [];
  $("#termLog").innerHTML = "";
};
$("#btnRecord").onclick = function () {
  if (!state.activeId) return;
  var on = !(activeTab().status && activeTab().status.recording);
  apiPost("/api/ports/" + state.activeId + "/record", { enable: on }).catch(function (e) { alert(e.message); });
};
$("#btnDtr").onclick = function () {
  if (!state.activeId) return;
  apiPost("/api/ports/" + state.activeId + "/dtr", { value: !(activeTab().status && activeTab().status.cfg && activeTab().status.cfg.dtr) })
    .catch(function (e) { alert(e.message); });
};
$("#btnRts").onclick = function () {
  if (!state.activeId) return;
  apiPost("/api/ports/" + state.activeId + "/rts", { value: !(activeTab().status && activeTab().status.cfg && activeTab().status.cfg.rts) })
    .catch(function (e) { alert(e.message); });
};

$("#btnSend").onclick = function () {
  var v = $("#sendInput").value;
  if (v === "") return;
  doSend(v, $("#sendMode").value, $("#sendNewline").value);
};
$("#sendInput").addEventListener("keydown", function (e) {
  if (e.key === "Enter") {
    var v = $("#sendInput").value;
    if (v === "") return;
    doSend(v, $("#sendMode").value, $("#sendNewline").value);
    state.histIdx = state.history.length;
    $("#sendInput").value = "";
  } else if (e.key === "ArrowUp") {
    if (!state.history.length) return;
    if (state.histIdx > 0) state.histIdx--;
    $("#sendInput").value = state.history[state.histIdx] || "";
    e.preventDefault();
  } else if (e.key === "ArrowDown") {
    if (state.histIdx < state.history.length) {
      state.histIdx++;
      $("#sendInput").value = state.history[state.histIdx] || "";
    }
    e.preventDefault();
  }
});

/* 侧栏页签 */
$$(".side-tabs button").forEach(function (b) {
  b.onclick = function () {
    $$(".side-tabs button").forEach(function (x) { x.classList.remove("on"); });
    b.classList.add("on");
    $$(".pane").forEach(function (p) { p.classList.remove("on"); });
    $("#pane" + { commands: "Commands", seqs: "Seqs", scripts: "Scripts" }[b.dataset.pane]).classList.add("on");
  };
});

/* ---------------- 启动 ---------------- */
function renderPrefs() {
  $$("#segMode button").forEach(function (b) {
    b.classList.toggle("on", b.dataset.v === state.prefs.display_mode);
  });
  $("#ckTs").checked = !!state.prefs.show_ts;
}

function init() {
  renderPrefs();
  if (state.sessions.size) $("#app").classList.remove("no-session");
  else $("#app").classList.add("no-session");
  wsConnect();
  // REST 兜底拉取（hello 已带数据，此处保证一致性）
  api("/api/state").then(function (j) {
    state.version = j.version;
    state.commands = j.commands;
    state.sequences = j.sequences;
    state.prefs = Object.assign(state.prefs, j.prefs);
    (j.sessions || []).forEach(ensureSession);
    renderPrefs(); renderTabs(); renderCmdList(); renderSeqList(); loadScripts();
  }).catch(function (e) {
    if (e.message !== "需要访问令牌") console.warn("state 加载失败:", e);
  });
}

init();
