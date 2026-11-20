#!/usr/bin/env node
'use strict';
/*
 * Ambassador Bridge ship watcher.
 *
 * Reads live AIS ship positions (aisstream.io) for the Detroit River and Lake
 * St. Clair and sends an ntfy notification about an hour before a large ship
 * passes under the Ambassador Bridge, saying which direction it is coming from.
 *
 * Needs Node 22 or newer (built-in WebSocket and fetch). No packages.
 *
 *   node bridge-watch.js --once        listen for a few minutes, schedule any
 *                                      alerts that are due, then exit (this is
 *                                      what the cloud schedule runs)
 *   node bridge-watch.js               stay connected and alert in real time
 *                                      (for an always-on machine)
 *   node bridge-watch.js --init        create config.json with a new ntfy topic
 *   node bridge-watch.js --test-notify send a test notification
 *   node bridge-watch.js --selftest    check the timing logic with simulated ships
 *
 * Settings come from config.json, or from environment variables in the cloud:
 * AISSTREAM_API_KEY, NTFY_TOPIC, NTFY_SERVER, MIN_LENGTH_M, LEAD_MINUTES,
 * LISTEN_SECONDS, LOOKAHEAD_MINUTES.
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const DIR = __dirname;
const CONFIG_PATH = path.join(DIR, 'config.json');
const CACHE_PATH = path.join(DIR, 'ships-cache.json');
const LOG_PATH = path.join(DIR, 'bridge-watch.log');

const DEFAULTS = {
  aisstreamApiKey: '',
  ntfyServer: 'https://ntfy.sh',
  ntfyTopic: '',
  // Alert for ships at least this long, in metres. Seaway-size ships are
  // 225.5 m and report as 225 or 226, so 227 means "bigger than Seaway-size".
  minLengthM: 227,
  // How long before the bridge passage the alert should arrive.
  leadMinutes: 60,
  // Extra ships (MMSI numbers) to alert on whatever length they report, for
  // example tugs that push a long barge but report only the tug's length.
  alwaysAlertMmsi: [],
  timeZone: 'America/Detroit',
};

// Large lakers whose length is known in advance, so they are recognised even
// before they broadcast it (a ship sends its length only every 6 minutes, and
// the cloud check listens for less than that). Other ships are learned from
// their own broadcasts and remembered in ships-cache.json.
// The tugs listed push a long barge; the length is tug and barge together,
// because some of them report only the tug.
const KNOWN_SHIPS = {
  366971360: ['JOHN G MUNSON', 234],
  366904910: ['KAYE E BARKER', 228],
  366904890: ['HON JAMES L OBERSTAR', 246],
  366904950: ['LEE A TREGURTHA', 252],
  366983440: ['JOYCE L VANENKEVORT', 257],
  338888000: ['CLYDE S VANENKEVORT', 257],
  338866000: ['DIRK S VANENKEVORT', 262],
  // The thirteen 1,000-footers.
  366904940: ['PAUL R TREGURTHA', 309],
  367121010: ['AMERICAN CENTURY', 305],
  367120990: ['AMERICAN INTEGRITY', 305],
  366930730: ['AMERICAN SPIRIT', 306],
  367050160: ['BURNS HARBOR', 305],
  366938730: ['INDIANA HARBOR', 305],
  366905890: ['JAMES R BARKER', 306],
  366904880: ['MESABI MINER', 306],
  367348220: ['PRESQUE ISLE', 305],
  367050550: ['STEWART J CORT', 305],
  366906610: ['WALTER J MCCARTHY JR', 305],
  366971330: ['EDGAR B SPEER', 306],
  366971370: ['EDWIN H GOTT', 306],
};

// ---------------------------------------------------------------- route ----

// Shipping channel centreline in downbound order (Lake St. Clair to Lake
// Erie), as [latitude, longitude]. Approximate: good to a few hundred metres,
// which is a minute or two of travel.
const ROUTE = [
  [42.5350, -82.6930], // lower end of the St. Clair Cutoff channel
  [42.4650, -82.7550], // Lake St. Clair Light
  [42.3535, -82.9280], // head of the Detroit River (Windmill Point / Peche Island)
  [42.3420, -82.9550], // south of Belle Isle, east end
  [42.3310, -82.9900], // south of Belle Isle, west end
  [42.3275, -83.0200],
  [42.3235, -83.0400], // downtown Detroit / Windsor
  [42.3195, -83.0550],
  [42.3118, -83.0740], // AMBASSADOR BRIDGE
  [42.2960, -83.0900],
  [42.2790, -83.1040], // Zug Island / Rouge River mouth
  [42.2500, -83.1230], // Ecorse
  [42.2150, -83.1330], // Wyandotte
  [42.1800, -83.1280], // head of Grosse Ile
  [42.1500, -83.1200],
  [42.1100, -83.1200], // Livingstone / Amherstburg channels
  [42.0500, -83.1250],
  [42.0000, -83.1410], // Detroit River Light
];
const RIVER_HEAD_INDEX = 2;
const BRIDGE_INDEX = 8;

// The river has a 12 mph (10.4 knot) speed limit, so a ship doing 13 knots on
// Lake St. Clair is assumed to slow to this once it is in the river.
const RIVER_MAX_KN = 10.4;
// A ship further than this from the channel line is not treated as in transit.
const MAX_OFF_ROUTE_NM = 2.0;
const MIN_SPEED_KN = 2.0;
const MIN_EVAL_GAP_MS = 20 * 1000;
// After an alert, stay quiet about the same ship for this long.
const REARM_MS = 2 * 60 * 60 * 1000;

// Area to subscribe to: the Detroit River, Lake St. Clair and a margin.
const BOUNDING_BOX = [[42.80, -83.40], [41.85, -82.35]];
const STREAM_URL = 'wss://stream.aisstream.io/v0/stream';

const NM_TO_MI = 1.15078;
const rad = (d) => (d * Math.PI) / 180;

function localXY(lat, lon, lat0, lon0) {
  return {
    x: (lon - lon0) * 60 * Math.cos(rad((lat + lat0) / 2)),
    y: (lat - lat0) * 60,
  };
}

function buildRoute(points) {
  const segs = [];
  let s = 0;
  const cum = [0];
  for (let i = 0; i < points.length - 1; i++) {
    const [lat0, lon0] = points[i];
    const [lat1, lon1] = points[i + 1];
    const d = localXY(lat1, lon1, lat0, lon0);
    const len = Math.hypot(d.x, d.y);
    const bearing = (Math.atan2(d.x, d.y) * 180 / Math.PI + 360) % 360;
    segs.push({ lat0, lon0, lat1, lon1, dx: d.x, dy: d.y, len, bearing, s0: s });
    s += len;
    cum.push(s);
  }
  return { segs, cum, total: s };
}

const R = buildRoute(ROUTE);
const BRIDGE_S = R.cum[BRIDGE_INDEX];
const RIVER_HEAD_S = R.cum[RIVER_HEAD_INDEX];

// Closest point on the channel line: distance along it (s, nautical miles from
// the start), distance off it, and the downbound bearing at that point.
function project(lat, lon) {
  let best = null;
  for (const g of R.segs) {
    const p = localXY(lat, lon, g.lat0, g.lon0);
    let t = (p.x * g.dx + p.y * g.dy) / (g.len * g.len);
    t = Math.max(0, Math.min(1, t));
    const off = Math.hypot(p.x - t * g.dx, p.y - t * g.dy);
    if (!best || off < best.off) best = { s: g.s0 + t * g.len, off, bearing: g.bearing };
  }
  return best;
}

function pointAt(s) {
  const c = Math.max(0, Math.min(R.total, s));
  for (const g of R.segs) {
    if (c <= g.s0 + g.len + 1e-9) {
      const t = (c - g.s0) / g.len;
      return { lat: g.lat0 + t * (g.lat1 - g.lat0), lon: g.lon0 + t * (g.lon1 - g.lon0), bearing: g.bearing };
    }
  }
  const g = R.segs[R.segs.length - 1];
  return { lat: g.lat1, lon: g.lon1, bearing: g.bearing };
}

// Minutes to the bridge from point s at the given speed, applying the river
// speed limit to the part of the trip that is in the river.
function minutesToBridge(s, speedKn) {
  const riverKn = Math.min(speedKn, RIVER_MAX_KN);
  if (s < BRIDGE_S) {
    const lake = Math.max(0, RIVER_HEAD_S - s);
    const river = BRIDGE_S - Math.max(s, RIVER_HEAD_S);
    return (lake / speedKn + river / riverKn) * 60;
  }
  return ((s - BRIDGE_S) / riverKn) * 60;
}

// ---------------------------------------------------------------- logic ----

const DIRECTIONS = {
  downbound: {
    label: 'Downbound',
    text: 'coming from Lake St. Clair (the Belle Isle and downtown side), heading for Lake Erie',
    tag: 'arrow_down',
  },
  upbound: {
    label: 'Upbound',
    text: 'coming from Lake Erie (the Zug Island side), heading for Lake St. Clair',
    tag: 'arrow_up',
  },
};

function shipLength(mmsi, reported) {
  const known = KNOWN_SHIPS[mmsi] ? KNOWN_SHIPS[mmsi][1] : 0;
  const len = Math.max(known, Number(reported) || 0);
  return len > 0 ? len : undefined;
}

function shipName(mmsi, reported) {
  return reported || (KNOWN_SHIPS[mmsi] ? KNOWN_SHIPS[mmsi][0] : `MMSI ${mmsi}`);
}

function isBigEnough(mmsi, reportedLength, cfg) {
  if ((cfg.alwaysAlertMmsi || []).includes(mmsi)) return true;
  const len = shipLength(mmsi, reportedLength);
  return len !== undefined && len >= cfg.minLengthM;
}

/*
 * Is this ship heading for the bridge, and if so from which side and how
 * long until it gets there? Returns null when it is stopped, off the channel,
 * or moving away from the bridge.
 */
function assess(rep, speedKn) {
  const sog = rep.sog;
  if (!(sog >= MIN_SPEED_KN && sog < 60)) return null; // 102.3 means "not available"
  if (!(speedKn >= MIN_SPEED_KN)) return null;
  let course = rep.cog;
  if (!(course >= 0 && course < 360)) course = rep.heading;
  if (!(course >= 0 && course < 360)) return null;

  const pos = project(rep.lat, rep.lon);
  if (pos.off > MAX_OFF_ROUTE_NM) return null;

  const along = Math.cos(rad(course - pos.bearing)); // +1 = moving downbound
  let direction = null;
  if (pos.s < BRIDGE_S && along > 0.5) direction = 'downbound';
  if (pos.s > BRIDGE_S && along < -0.5) direction = 'upbound';
  if (!direction) return null;

  return {
    direction,
    distNm: Math.abs(BRIDGE_S - pos.s),
    etaMin: minutesToBridge(pos.s, speedKn),
    sog: speedKn,
  };
}

/*
 * Real-time mode: feed one position report for a ship. Returns an alert when
 * it is time to notify, otherwise null. `ship` remembers state between calls.
 */
function evaluate(ship, rep, now, cfg) {
  ship.lastSeen = now;
  if (ship.lastEval && now - ship.lastEval < MIN_EVAL_GAP_MS) return null;
  ship.lastEval = now;

  if (rep.sog >= 0 && rep.sog < 60) ship.sogAvg = ship.sogAvg == null ? rep.sog : ship.sogAvg * 0.7 + rep.sog * 0.3;
  const a = assess(rep, ship.sogAvg);
  if (!a) { ship.pending = 0; ship.direction = null; return null; }
  ship.etaMin = a.etaMin;
  ship.direction = a.direction;

  const due = a.etaMin <= cfg.leadMinutes && a.distNm >= 0.15;
  if (!due || !isBigEnough(ship.mmsi, ship.length, cfg)) { ship.pending = 0; return null; }
  if (ship.alertedAt && now - ship.alertedAt < REARM_MS) return null;

  // Wait for two readings in a row so one odd report cannot trigger an alert.
  ship.pending = (ship.pending || 0) + 1;
  if (ship.pending < 2) return null;

  ship.pending = 0;
  ship.alertedAt = now;
  return { ...a, mmsi: ship.mmsi, name: shipName(ship.mmsi, ship.name), length: shipLength(ship.mmsi, ship.length), seenAt: now };
}

// `a.seenAt` is when the ship was last seen; the notification may be held
// back and delivered later, at `deliverAt`.
function formatAlert(a, cfg, deliverAt) {
  const d = DIRECTIONS[a.direction];
  const clock = (ms) => new Date(ms).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: cfg.timeZone });
  const arrival = a.seenAt + a.etaMin * 60000;
  const mins = Math.max(1, Math.round((arrival - (deliverAt || a.seenAt)) / 60000));
  const size = a.length ? `${a.length} m` : 'Length not reported';
  return {
    title: `${a.name}: bridge in about ${mins} min`,
    message:
      `${d.label}: ${d.text}.\n` +
      `Expected under the Ambassador Bridge around ${clock(arrival)}.\n` +
      `${size}. Last seen ${(a.distNm * NM_TO_MI).toFixed(1)} mi from the bridge doing ${a.sog.toFixed(1)} knots at ${clock(a.seenAt)}.`,
    tags: ['ship', d.tag],
  };
}

// ------------------------------------------------------------- plumbing ----

function log(msg) {
  const line = `${new Date().toISOString()} ${msg}`;
  console.log(line);
  if (process.env.GITHUB_ACTIONS) return;
  try { fs.appendFileSync(LOG_PATH, line + '\n'); } catch { /* logging must not stop the watcher */ }
}

function loadConfig() {
  let file = {};
  if (fs.existsSync(CONFIG_PATH)) file = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
  const cfg = { ...DEFAULTS, ...file };
  const env = process.env;
  if (env.AISSTREAM_API_KEY) cfg.aisstreamApiKey = env.AISSTREAM_API_KEY;
  if (env.NTFY_TOPIC) cfg.ntfyTopic = env.NTFY_TOPIC;
  if (env.NTFY_SERVER) cfg.ntfyServer = env.NTFY_SERVER;
  if (env.MIN_LENGTH_M) cfg.minLengthM = Number(env.MIN_LENGTH_M);
  if (env.LEAD_MINUTES) cfg.leadMinutes = Number(env.LEAD_MINUTES);
  return cfg;
}

function requireSettings(cfg) {
  const missing = [];
  if (!cfg.aisstreamApiKey) missing.push('the aisstream.io API key (AISSTREAM_API_KEY, or "aisstreamApiKey" in config.json)');
  if (!cfg.ntfyTopic) missing.push('the ntfy topic (NTFY_TOPIC, or run: node bridge-watch.js --init)');
  if (typeof WebSocket === 'undefined') missing.push('Node 22 or newer');
  if (missing.length) {
    console.error('Cannot start. Missing: ' + missing.join('; '));
    process.exit(1);
  }
}

function initConfig() {
  if (fs.existsSync(CONFIG_PATH)) {
    console.log('config.json already exists; leaving it alone.');
    console.log(`ntfy topic: ${loadConfig().ntfyTopic}`);
    return;
  }
  const cfg = { ...DEFAULTS, ntfyTopic: 'bridge-watch-' + crypto.randomBytes(9).toString('base64url') };
  fs.writeFileSync(CONFIG_PATH, JSON.stringify(cfg, null, 2) + '\n');
  console.log('Created config.json');
  console.log(`ntfy topic: ${cfg.ntfyTopic}`);
}

// Send a notification. With `deliverAt` (ms), ntfy holds it until that time.
async function notify(cfg, n, deliverAt) {
  if (!cfg.ntfyTopic) throw new Error('no ntfy topic set');
  const body = { topic: cfg.ntfyTopic, title: n.title, message: n.message, tags: n.tags, priority: n.priority || 4 };
  if (deliverAt && deliverAt - Date.now() > 30 * 1000) body.delay = String(Math.round(deliverAt / 1000));
  const res = await fetch(cfg.ntfyServer.replace(/\/+$/, '') + '/', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`ntfy answered ${res.status}: ${(await res.text()).slice(0, 200)}`);
  return res.json();
}

function loadCache() {
  try { return JSON.parse(fs.readFileSync(CACHE_PATH, 'utf8')); } catch { return {}; }
}

function subscription(cfg) {
  return JSON.stringify({
    APIKey: cfg.aisstreamApiKey,
    BoundingBoxes: [BOUNDING_BOX],
    FilterMessageTypes: ['PositionReport', 'ShipStaticData'],
  });
}

// Pull the useful fields out of one aisstream message.
function parseMessage(msg) {
  const meta = msg.MetaData || {};
  const mmsi = Number(meta.MMSI);
  if (!mmsi) return null;
  const out = { mmsi, type: msg.MessageType, name: (meta.ShipName || '').trim() };
  if (msg.MessageType === 'ShipStaticData') {
    const sd = (msg.Message && msg.Message.ShipStaticData) || {};
    const dim = sd.Dimension || {};
    out.length = (Number(dim.A) || 0) + (Number(dim.B) || 0);
    out.name = (sd.Name || '').trim() || out.name;
  } else if (msg.MessageType === 'PositionReport') {
    const pr = (msg.Message && msg.Message.PositionReport) || {};
    out.lat = pr.Latitude ?? meta.Latitude ?? meta.latitude;
    out.lon = pr.Longitude ?? meta.Longitude ?? meta.longitude;
    out.sog = pr.Sog;
    out.cog = pr.Cog;
    out.heading = pr.TrueHeading;
    if (typeof out.lat !== 'number' || typeof out.lon !== 'number') return null;
  } else {
    return null;
  }
  return out;
}

const decoder = new TextDecoder();
const decodeFrame = (data) => JSON.parse(typeof data === 'string' ? data : decoder.decode(data));

// ----------------------------------------------------------- once mode ----

// Listen to the stream for a fixed time, then resolve with what happened.
function listenFor(cfg, ms, onShipMessage) {
  return new Promise((resolve) => {
    const stats = { messages: 0, confirmed: false, error: null };
    let finished = false;
    const ws = new WebSocket(STREAM_URL);
    ws.binaryType = 'arraybuffer';
    const finish = () => {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      try { ws.close(); } catch { /* already closed */ }
      resolve(stats);
    };
    const timer = setTimeout(finish, ms);
    // aisstream.io drops the connection without a message when the key is wrong.
    const dropped = 'aisstream.io dropped the connection right after subscribing, which usually means the API key is wrong';
    let opened = false;
    ws.addEventListener('open', () => { opened = true; ws.send(subscription(cfg)); });
    ws.addEventListener('message', (ev) => {
      let msg;
      try { msg = decodeFrame(ev.data); } catch { return; }
      if (msg.error) { stats.error = String(msg.error); finish(); return; }
      if (msg.MessageType === 'SubscriptionConfirmation') { stats.confirmed = true; return; }
      stats.messages++;
      const m = parseMessage(msg);
      if (m) onShipMessage(m);
    });
    ws.addEventListener('error', () => {
      if (stats.error || stats.confirmed) return;
      stats.error = opened ? dropped : 'could not connect to aisstream.io';
    });
    ws.addEventListener('close', () => {
      if (!finished && !stats.error && !stats.confirmed) stats.error = opened ? dropped : 'could not connect to aisstream.io';
      finish();
    });
  });
}

async function runOnce() {
  const cfg = loadConfig();
  requireSettings(cfg);
  const listenMs = (Number(process.env.LISTEN_SECONDS) || 180) * 1000;
  // Alerts are scheduled this far ahead of the lead time, so a ship is not
  // missed if the next scheduled run starts late (GitHub often runs
  // 10-minute schedules 15 to 30 minutes apart).
  const lookaheadMin = Number(process.env.LOOKAHEAD_MINUTES) || 30;

  const cache = loadCache(); // mmsi -> { name, length, alertedAt }
  const seen = new Map();    // mmsi -> latest position and recent speeds

  log(`listening for ${Math.round(listenMs / 1000)} s`);
  const stats = await listenFor(cfg, listenMs, (m) => {
    const c = cache[m.mmsi] || (cache[m.mmsi] = {});
    if (m.name) c.name = m.name;
    if (m.type === 'ShipStaticData') {
      if (m.length > 0) c.length = m.length;
      return;
    }
    const s = seen.get(m.mmsi) || { sogs: [] };
    Object.assign(s, { lat: m.lat, lon: m.lon, sog: m.sog, cog: m.cog, heading: m.heading, at: Date.now() });
    if (m.sog >= 0 && m.sog < 60) { s.sogs.push(m.sog); if (s.sogs.length > 6) s.sogs.shift(); }
    seen.set(m.mmsi, s);
  });

  if (stats.error && !stats.messages) {
    log(`FAILED: ${stats.error}`);
    process.exit(1);
  }

  const now = Date.now();
  let sent = 0;
  const large = [];
  for (const [mmsi, s] of seen) {
    const c = cache[mmsi] || {};
    if (!isBigEnough(mmsi, c.length, cfg)) continue;
    const avg = s.sogs.length ? s.sogs.reduce((x, y) => x + y, 0) / s.sogs.length : s.sog;
    const a = assess(s, avg);
    const name = shipName(mmsi, c.name);
    large.push(a ? `${name} (${a.direction}, about ${Math.round(a.etaMin)} min)` : `${name} (not heading for the bridge)`);
    if (!a || a.distNm < 0.15 || a.etaMin > cfg.leadMinutes + lookaheadMin) continue;
    if (c.alertedAt && now - c.alertedAt < REARM_MS) continue;

    const alert = { ...a, mmsi, name, length: shipLength(mmsi, c.length), seenAt: s.at };
    const arrival = s.at + a.etaMin * 60000;
    const deliverAt = Math.max(now, arrival - cfg.leadMinutes * 60000);
    const n = formatAlert(alert, cfg, deliverAt);
    try {
      await notify(cfg, n, deliverAt);
      c.alertedAt = deliverAt;
      cache[mmsi] = c;
      sent++;
      const wait = Math.round((deliverAt - now) / 60000);
      log(`ALERT ${wait > 0 ? `scheduled in ${wait} min` : 'sent now'}: ${n.title} | ${n.message.replace(/\n/g, ' ')}`);
    } catch (e) {
      log(`ntfy failed for ${name}: ${e.message}`);
    }
  }

  // Keep the cache small: remember ships with a length or a recent alert.
  for (const [mmsi, c] of Object.entries(cache)) {
    if (c.alertedAt && now - c.alertedAt > REARM_MS) delete c.alertedAt;
    if (!c.length && !c.alertedAt) delete cache[mmsi];
  }
  fs.writeFileSync(CACHE_PATH, JSON.stringify(cache));

  log(`done: ${stats.messages} messages, ${seen.size} ships seen, ${large.length} large${large.length ? ': ' + large.join(', ') : ''}; ${sent} alert(s)`);
  process.exit(0);
}

// ------------------------------------------------------ real-time mode ----

function runForever() {
  const cfg = loadConfig();
  requireSettings(cfg);

  const ships = new Map(); // mmsi -> ship; names, lengths and alerts survive a restart
  for (const [mmsi, s] of Object.entries(loadCache())) {
    ships.set(Number(mmsi), { mmsi: Number(mmsi), name: s.name, length: s.length, alertedAt: s.alertedAt });
  }
  let cacheDirty = false;
  setInterval(() => {
    if (!cacheDirty) return;
    cacheDirty = false;
    const out = {};
    for (const s of ships.values()) if (s.length || s.alertedAt) out[s.mmsi] = { name: s.name, length: s.length, alertedAt: s.alertedAt };
    try { fs.writeFileSync(CACHE_PATH, JSON.stringify(out)); } catch (e) { log(`could not save cache: ${e.message}`); }
  }, 60 * 1000);

  let ws = null;
  let lastMessageAt = Date.now();
  let backoffMs = 5000;
  let messageCount = 0;

  function handle(msg) {
    if (msg.error) { log(`aisstream error: ${msg.error}`); return; }
    if (msg.MessageType === 'SubscriptionConfirmation') { log('subscribed'); return; }
    const m = parseMessage(msg);
    if (!m) return;
    let ship = ships.get(m.mmsi);
    if (!ship) { ship = { mmsi: m.mmsi }; ships.set(m.mmsi, ship); }
    if (m.name && m.name !== ship.name) { ship.name = m.name; cacheDirty = true; }
    if (m.type === 'ShipStaticData') {
      if (m.length > 0 && m.length !== ship.length) { ship.length = m.length; cacheDirty = true; }
      return;
    }
    const alert = evaluate(ship, m, Date.now(), cfg);
    if (!alert) return;
    cacheDirty = true;
    const n = formatAlert(alert, cfg);
    log(`ALERT ${n.title} | ${n.message.replace(/\n/g, ' ')}`);
    notify(cfg, n).catch((e) => {
      log(`ntfy failed (${e.message}); will retry on the next report`);
      ship.alertedAt = null;
    });
  }

  function connect() {
    log('connecting to aisstream.io');
    const mine = new WebSocket(STREAM_URL);
    ws = mine;
    mine.binaryType = 'arraybuffer';
    mine.addEventListener('open', () => mine.send(subscription(cfg)));
    mine.addEventListener('message', (ev) => {
      lastMessageAt = Date.now();
      backoffMs = 5000;
      messageCount++;
      try { handle(decodeFrame(ev.data)); } catch (e) { log(`bad message: ${e.message}`); }
    });
    mine.addEventListener('error', () => log('connection error'));
    mine.addEventListener('close', (ev) => {
      if (ws !== mine) return;
      ws = null;
      log(`connection closed (code ${ev.code}); reconnecting in ${Math.round(backoffMs / 1000)} s`);
      setTimeout(connect, backoffMs);
      backoffMs = Math.min(backoffMs * 2, 5 * 60 * 1000);
    });
  }

  // The stream can go quiet without closing; reconnect if nothing arrives.
  setInterval(() => {
    if (ws && Date.now() - lastMessageAt > 5 * 60 * 1000) {
      log('no data for 5 minutes; reconnecting');
      lastMessageAt = Date.now();
      try { ws.close(); } catch { /* already closed */ }
    }
  }, 60 * 1000);

  // Half-hourly summary so the log shows the watcher is alive.
  setInterval(() => {
    const now = Date.now();
    const recent = [...ships.values()].filter((s) => s.lastSeen && now - s.lastSeen < 15 * 60 * 1000);
    const big = recent.filter((s) => isBigEnough(s.mmsi, s.length, cfg));
    const list = big.map((s) => `${shipName(s.mmsi, s.name)}${s.direction ? ` (${s.direction}, about ${Math.round(s.etaMin)} min)` : ''}`).join(', ');
    log(`alive: ${messageCount} messages, ${recent.length} ships in the area, ${big.length} large${list ? ': ' + list : ''}`);
    for (const [mmsi, s] of ships) if (!s.length && !s.alertedAt && (!s.lastSeen || now - s.lastSeen > 6 * 60 * 60 * 1000)) ships.delete(mmsi);
  }, 30 * 60 * 1000);

  log(`watching for ships of ${cfg.minLengthM} m or more, alerting ${cfg.leadMinutes} min before the bridge`);
  connect();
}

// ------------------------------------------------------------- selftest ----

function selftest() {
  const cfg = { ...DEFAULTS };
  let failures = 0;
  const check = (name, ok, detail) => {
    console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? '  (' + detail + ')' : ''}`);
    if (!ok) failures++;
  };

  // Drive a ship along the channel in 30 s steps. Reports when the real-time
  // logic alerts, when the ship actually reaches the bridge, and what a
  // one-off check made `snapshotAtMin` minutes into the trip would predict.
  function sail({ mmsi, length, startS, dir, lakeKn, snapshotAtMin }) {
    const ship = { mmsi, name: 'TEST SHIP', length };
    let s = startS;
    let t = 0;
    let alertAt = null;
    let alert = null;
    let bridgeAt = null;
    let snapshot = null;
    const step = 30;
    for (let i = 0; i < 100000; i++) {
      const p = pointAt(s);
      const kn = s >= RIVER_HEAD_S ? Math.min(lakeKn, RIVER_MAX_KN) : lakeKn;
      const cog = dir === 'downbound' ? p.bearing : (p.bearing + 180) % 360;
      const rep = { lat: p.lat, lon: p.lon, sog: kn, cog, heading: cog };
      const a = evaluate(ship, rep, t * 1000, cfg);
      if (a && alertAt == null) { alertAt = t; alert = a; }
      if (snapshotAtMin != null && snapshot == null && t >= snapshotAtMin * 60) snapshot = { t, a: assess(rep, kn) };
      const next = s + (dir === 'downbound' ? 1 : -1) * kn * (step / 3600);
      if (bridgeAt == null && (s - BRIDGE_S) * (next - BRIDGE_S) <= 0) bridgeAt = t + step * Math.abs(BRIDGE_S - s) / Math.abs(next - s);
      s = next;
      t += step;
      if (s <= 0 || s >= R.total) break;
    }
    return { alertAt, alert, bridgeAt, snapshot, leadMin: alertAt != null && bridgeAt != null ? (bridgeAt - alertAt) / 60 : null };
  }
  const fmt = (x) => (x == null ? 'none' : x.toFixed(1));

  console.log(`Route: ${(R.total * NM_TO_MI).toFixed(1)} mi; the bridge is ${(BRIDGE_S * NM_TO_MI).toFixed(1)} mi from the top ` +
    `and ${((R.total - BRIDGE_S) * NM_TO_MI).toFixed(1)} mi from the bottom\n`);

  let r = sail({ mmsi: 1, length: 309, startS: 0, dir: 'downbound', lakeKn: 12.5, snapshotAtMin: 20 });
  check('downbound at 12.5 kn: real-time alert about 60 min ahead', r.leadMin > 57 && r.leadMin < 62, `lead ${fmt(r.leadMin)} min`);
  check('...reported as downbound', r.alert && r.alert.direction === 'downbound');
  const predicted = r.snapshot.t / 60 + r.snapshot.a.etaMin;
  check('...a one-off check 20 min in predicts the bridge time within 2 min', Math.abs(predicted - r.bridgeAt / 60) < 2,
    `predicted ${fmt(predicted)} min, actual ${fmt(r.bridgeAt / 60)} min`);
  console.log('\n' + JSON.stringify(formatAlert({ ...r.alert, seenAt: Date.now() }, cfg), null, 2) + '\n');

  r = sail({ mmsi: 2, length: 309, startS: R.total, dir: 'upbound', lakeKn: 10 });
  check('upbound at 10 kn: real-time alert about 60 min ahead', r.leadMin > 57 && r.leadMin < 62, `lead ${fmt(r.leadMin)} min`);
  check('...reported as upbound', r.alert && r.alert.direction === 'upbound');
  console.log('\n' + JSON.stringify(formatAlert({ ...r.alert, seenAt: Date.now() }, cfg), null, 2) + '\n');

  r = sail({ mmsi: 3, length: 309, startS: 0, dir: 'downbound', lakeKn: 8 });
  check('slow ship (8 kn) still alerts about 60 min ahead', r.leadMin > 57 && r.leadMin < 62, `lead ${fmt(r.leadMin)} min`);

  r = sail({ mmsi: 4, length: 225, startS: 0, dir: 'downbound', lakeKn: 12 });
  check('Seaway-size ship (225 m) does not alert', r.alertAt == null);

  r = sail({ mmsi: 5, length: 226, startS: 0, dir: 'downbound', lakeKn: 12 });
  check('Seaway-size ship reporting 226 m does not alert', r.alertAt == null);

  r = sail({ mmsi: 6, length: 250, startS: 0, dir: 'downbound', lakeKn: 12 });
  check('250 m ship alerts', r.alertAt != null);

  r = sail({ mmsi: 7, length: undefined, startS: 0, dir: 'downbound', lakeKn: 12 });
  check('unknown ship with no length reported does not alert', r.alertAt == null);

  r = sail({ mmsi: 366904940, length: undefined, startS: 0, dir: 'downbound', lakeKn: 12 });
  check('known 1,000-footer alerts even before it reports a length', r.alertAt != null && r.alert.length === 309 && r.alert.name === 'TEST SHIP');

  r = sail({ mmsi: 367348220, length: 43, startS: 0, dir: 'downbound', lakeKn: 11 });
  check('tug Presque Isle alerts despite reporting a short length', r.alertAt != null && r.alert.length === 305);

  r = sail({ mmsi: 8, length: 309, startS: BRIDGE_S + 2, dir: 'upbound', lakeKn: 9 });
  check('ship starting 2 nm below the bridge alerts straight away with a short ETA', r.alert && r.alert.etaMin < 20 && r.alertAt <= 60, `eta ${fmt(r.alert && r.alert.etaMin)} min`);

  r = sail({ mmsi: 9, length: 309, startS: BRIDGE_S + 1, dir: 'downbound', lakeKn: 10 });
  check('ship that has already passed the bridge does not alert', r.alertAt == null);

  const moored = { mmsi: 10, name: 'MOORED', length: 309 };
  const mp = pointAt(BRIDGE_S - 3);
  let any = false;
  for (let i = 0; i < 20; i++) if (evaluate(moored, { lat: mp.lat, lon: mp.lon, sog: 0, cog: 360, heading: 60 }, i * 30000, cfg)) any = true;
  check('moored ship does not alert', !any);

  const far = pointAt(BRIDGE_S - 8);
  // The channel runs north-east here, so this offset is about 4 nm to one side of it.
  check('ship well off the channel is ignored', assess({ lat: far.lat + 0.05, lon: far.lon - 0.06, sog: 10, cog: far.bearing }, 10) === null);
  check('ship on the channel at the same spot is picked up', assess({ lat: far.lat, lon: far.lon, sog: 10, cog: far.bearing }, 10) !== null);

  const once = { mmsi: 11, name: 'ONCE', length: 309 };
  let count = 0;
  for (let i = 0, s = BRIDGE_S - 9; s < BRIDGE_S; i++, s += 10 * (30 / 3600)) {
    const p = pointAt(s);
    if (evaluate(once, { lat: p.lat, lon: p.lon, sog: 10, cog: p.bearing, heading: p.bearing }, i * 30000, cfg)) count++;
  }
  check('one alert per passage', count === 1, `${count} alerts`);

  const delayed = formatAlert({ direction: 'upbound', distNm: 14, etaMin: 78, sog: 10.4, name: 'TEST', length: 309, seenAt: 0 }, cfg, 18 * 60000);
  check('a held-back alert is worded for its delivery time', delayed.title === 'TEST: bridge in about 60 min', delayed.title);

  console.log(failures ? `\n${failures} check(s) failed` : '\nAll checks passed');
  process.exit(failures ? 1 : 0);
}

// ----------------------------------------------------------------- main ----

const arg = process.argv[2];
if (arg === '--init') {
  initConfig();
} else if (arg === '--selftest') {
  selftest();
} else if (arg === '--test-notify') {
  notify(loadConfig(), {
    title: 'Bridge watcher test',
    message: 'If you can read this, ship alerts will reach this phone.',
    tags: ['ship', 'white_check_mark'],
    priority: 3,
  }).then(() => console.log('Test notification sent.'), (e) => { console.error(`Failed: ${e.message}`); process.exit(1); });
} else if (arg === '--once') {
  runOnce();
} else {
  runForever();
}
