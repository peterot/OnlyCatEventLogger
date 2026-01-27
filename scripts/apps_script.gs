/**
 * OnlyCat Stats Builder + Prompt Endpoint (Google Apps Script)
 *
 * Sheets built/refreshed:
 * - Sessions
 * - Stats_Last30
 * - Stats_Prev30
 * - Contraband
 * - Cats
 * - Status   <-- NEW (inside/outside + last change)
 *
 * Web App endpoint:
 * - GET /exec?token=YOURTOKEN
 *   - optionally refreshes derived tabs first (throttled)
 *   - returns compact JSON + a ready-to-use ChatGPT prompt
 *
 * Assumptions:
 * - You have a data tab with headers including:
 *   event_time_utc, event_trigger_source, event_classification, rfid_code, cat_label
 * - event_time_utc values are ISO strings like 2026-01-21T12:53:27Z
 * - event_trigger_source uses display names: "Exit Allowed" / "Entry Allowed"
 * - event_classification uses display names: e.g. "Contraband"
 */

const ONLYCAT_API = {
  TOKEN_PROPERTY_KEY: "ONLYCAT_API_TOKEN",

  RUN_REFRESH_FIRST: true,
  MIN_REFRESH_INTERVAL_SECONDS: 60,

  SHEET_SESSIONS: "Sessions",
  SHEET_STATS_LAST30: "Stats_Last30",
  SHEET_STATS_PREV30: "Stats_Prev30",
  SHEET_CONTRABAND: "Contraband",
  SHEET_CATS: "Cats",
  SHEET_STATUS: "Status",
};

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("OnlyCat")
    .addItem("Refresh stats", "refreshOnlyCatStats")
    .addToUi();
}

function refreshOnlyCatStats() {
  const ss = SpreadsheetApp.getActive();
  const tz = ss.getSpreadsheetTimeZone();

  const dataSheet = findDataSheet_(ss);
  if (!dataSheet) throw new Error("Couldn't find a sheet with the expected headers.");

  const data = readEvents_(dataSheet, tz);
  if (data.events.length === 0) throw new Error("No event rows found beneath the header.");

  const sessions = buildSessions_(data.events, tz);

  const today0 = startOfDay_(new Date(), tz);
  const last30Start = addDays_(today0, -30);
  const prev30Start = addDays_(today0, -60);

  const stats = buildStats_(sessions, last30Start, prev30Start, today0, tz);

  const contraband = buildContraband_(data.events, last30Start);

  const catsRows = buildCats_(data.events, sessions, tz);

  const status = buildStatus_(data.events, tz);

  writeSheet_(ss, ONLYCAT_API.SHEET_SESSIONS,
    ["Cat ID", "Cat", "RFID", "Exit (UTC)", "Entry (UTC)", "Minutes Outside", "Exit Date (Local)"],
    sessions.rows
  );

  writeSheet_(ss, ONLYCAT_API.SHEET_STATS_LAST30,
    [
      "Cat ID", "Cat",
      "Days in Window",
      "Trips (count)", "Trips / day (avg)",
      "Minutes outside (total)", "Minutes / trip (avg)", "Minutes / day (avg)"
    ],
    stats.last30Rows
  );

  writeSheet_(ss, ONLYCAT_API.SHEET_STATS_PREV30,
    [
      "Cat ID", "Cat",
      "Days in Window",
      "Trips (count)", "Trips / day (avg)",
      "Minutes outside (total)", "Minutes / trip (avg)", "Minutes / day (avg)"
    ],
    stats.prev30Rows
  );

  // Contraband: summary + incidents
  const contrabandSheet = getOrCreateSheet_(ss, ONLYCAT_API.SHEET_CONTRABAND);
  contrabandSheet.clear();

  const summaryHeader = ["Cat ID", "Cat", "Contraband (Last 30 days)", "Contraband (All time)"];
  const incidentsHeader = ["Event time (UTC)", "Cat ID", "Cat", "RFID", "Trigger", "Classification", "Event ID"];

  contrabandSheet.getRange(1, 1, 1, summaryHeader.length).setValues([summaryHeader]);
  if (contraband.summaryRows.length) {
    contrabandSheet.getRange(2, 1, contraband.summaryRows.length, summaryHeader.length).setValues(contraband.summaryRows);
  }

  const startRow = 3 + Math.max(contraband.summaryRows.length, 1);
  contrabandSheet.getRange(startRow, 1, 1, incidentsHeader.length).setValues([incidentsHeader]);
  if (contraband.incidentRows.length) {
    contrabandSheet.getRange(startRow + 1, 1, contraband.incidentRows.length, incidentsHeader.length).setValues(contraband.incidentRows);
  }

  contrabandSheet.setFrozenRows(1);
  contrabandSheet.autoResizeColumns(1, 10);

  writeSheet_(ss, ONLYCAT_API.SHEET_CATS,
    ["Cat ID", "Cat", "RFID", "First Seen (Local)", "Last Seen (Local)", "Total Events", "Total Outside Trips"],
    catsRows
  );

  writeSheet_(ss, ONLYCAT_API.SHEET_STATUS,
    ["Cat ID", "Cat", "RFID", "Status", "Last Change (UTC)", "Last Change (Local)", "Minutes Since Change", "Last Trigger"],
    status.rows
  );

  Logger.log("OnlyCat stats refreshed.");
}

/* -----------------------------
 * Web App endpoint
 * ----------------------------- */

function doGet(e) {
  try {
    const params = (e && e.parameter) ? e.parameter : {};
    assertToken_(params);

    const ss = SpreadsheetApp.getActive();
    const tz = ss.getSpreadsheetTimeZone();

    const refreshStatus = ONLYCAT_API.RUN_REFRESH_FIRST ? maybeRefreshDerivedTabs_() : { ran: false };

    const payload = buildBriefingPayload_(ss, tz);
    payload.refresh = refreshStatus;

    return ContentService
      .createTextOutput(JSON.stringify(payload, null, 2))
      .setMimeType(ContentService.MimeType.JSON);

  } catch (err) {
    const out = {
      ok: false,
      error: String(err && err.message ? err.message : err),
    };
    return ContentService
      .createTextOutput(JSON.stringify(out, null, 2))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

// If you want POST too, uncomment:
// function doPost(e) { return doGet(e); }

/* -----------------------------
 * Refresh throttling
 * ----------------------------- */

function maybeRefreshDerivedTabs_() {
  const props = PropertiesService.getScriptProperties();
  const key = "ONLYCAT_LAST_REFRESH_MS";
  const now = Date.now();
  const last = Number(props.getProperty(key) || "0");
  const minIntervalMs = (ONLYCAT_API.MIN_REFRESH_INTERVAL_SECONDS || 0) * 1000;

  if (minIntervalMs > 0 && (now - last) < minIntervalMs) {
    return {
      ran: false,
      reason: "throttled",
      seconds_since_last: Math.round((now - last) / 1000),
    };
  }

  try {
    refreshOnlyCatStats();
    props.setProperty(key, String(now));
    return { ran: true };
  } catch (err) {
    return { ran: true, error: String(err && err.message ? err.message : err) };
  }
}

/* -----------------------------
 * Prompt payload builder
 * ----------------------------- */

function buildBriefingPayload_(ss, tz) {
  const sessions = readSheetAsObjects_(ss.getSheetByName(ONLYCAT_API.SHEET_SESSIONS));
  const statsLast30 = readSheetAsObjects_(ss.getSheetByName(ONLYCAT_API.SHEET_STATS_LAST30));
  const statsPrev30 = readSheetAsObjects_(ss.getSheetByName(ONLYCAT_API.SHEET_STATS_PREV30));
  const contraband = readContraband_(ss.getSheetByName(ONLYCAT_API.SHEET_CONTRABAND));
  const cats = readSheetAsObjects_(ss.getSheetByName(ONLYCAT_API.SHEET_CATS));
  const status = readSheetAsObjects_(ss.getSheetByName(ONLYCAT_API.SHEET_STATUS));

  const prompt = buildChatGptPrompt_({
    timezone: tz,
    status: status.rows,
    sessions: sessions.rows.slice(0, 500),
    statsLast30: statsLast30.rows,
    statsPrev30: statsPrev30.rows,
    contrabandSummary: contraband.summaryRows,
    contrabandIncidents: contraband.incidents.slice(0, 200),
    cats: cats.rows,
  });

  return {
    ok: true,
    generated_at_local: Utilities.formatDate(new Date(), tz, "yyyy-MM-dd HH:mm:ss"),
    timezone: tz,

    status: status.rows,
    sessions: sessions.rows,
    stats_last30: statsLast30.rows,
    stats_prev30: statsPrev30.rows,
    contraband: contraband,
    cats: cats.rows,

    chatgpt_prompt: prompt,
  };
}

function buildChatGptPrompt_(d) {
  return [
    "You are helping me analyse my cats' catflap data.",
    "Answer questions about: trips outside, unknown visitors, and contraband incidents.",
    "Also answer: whether each cat is currently inside or outside, and when they last changed state.",
    "Respond to this message with a friendly summary. Keep it simple and fun, no geeky details of the JSON and it's specific fields, just say where the cats are just now i.e. Cleo has been out on adventures since 13:08; Hamish was out late but came back early this morning (8:09) and he's been in since then (probably snoozing).",
    "",
    `Timezone: ${d.timezone}`,
    "",
    "CURRENT STATUS (derived from latest entry/exit event per cat):",
    JSON.stringify(d.status, null, 2),
    "",
    "STATS (Last 30-day window; averages normalised by days of available data, capped at 30):",
    JSON.stringify(d.statsLast30, null, 2),
    "",
    "STATS (Previous 30-day window; averages normalised by days of available data, capped at 30):",
    JSON.stringify(d.statsPrev30, null, 2),
    "",
    "CONTRABAND SUMMARY:",
    JSON.stringify(d.contrabandSummary, null, 2),
    "",
    "CONTRABAND INCIDENTS (most recent first; may be truncated):",
    JSON.stringify(d.contrabandIncidents, null, 2),
    "",
    "CATS (reference list):",
    JSON.stringify(d.cats, null, 2),
    "",
    "SESSIONS (Exit->Entry pairs; may be truncated):",
    JSON.stringify(d.sessions, null, 2),
    "",
    "Now give the summary and answer any follow up questions"
  ].join("\n");
}

/* -----------------------------
 * Read helper sheets
 * ----------------------------- */

function readSheetAsObjects_(sh) {
  if (!sh) return { header: [], rows: [] };
  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  if (lastRow < 2 || lastCol < 1) return { header: [], rows: [] };

  const values = sh.getRange(1, 1, lastRow, lastCol).getValues();
  const header = values[0].map(h => String(h).trim());

  const rows = [];
  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    if (row.every(v => v === "" || v === null)) continue;
    const obj = {};
    for (let c = 0; c < header.length; c++) obj[header[c]] = row[c];
    rows.push(obj);
  }
  return { header, rows };
}

function readContraband_(sh) {
  if (!sh) return { summaryRows: [], incidents: [] };

  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  if (lastRow < 2) return { summaryRows: [], incidents: [] };

  const values = sh.getRange(1, 1, lastRow, lastCol).getValues();

  const summaryHeader = values[0].map(h => String(h).trim());
  const summaryRows = [];
  let r = 1;

  for (; r < values.length; r++) {
    const row = values[r];
    const firstCell = String(row[0] || "").trim();
    if (!firstCell) break;
    if (firstCell.toLowerCase().includes("event time")) break;

    const obj = {};
    for (let c = 0; c < summaryHeader.length; c++) obj[summaryHeader[c]] = row[c];
    summaryRows.push(obj);
  }

  let incidentsHeaderRow = -1;
  for (let i = 0; i < values.length; i++) {
    const a = String(values[i][0] || "").toLowerCase();
    if (a.includes("event time")) { incidentsHeaderRow = i; break; }
  }

  const incidents = [];
  if (incidentsHeaderRow >= 0 && incidentsHeaderRow + 1 < values.length) {
    const header = values[incidentsHeaderRow].map(h => String(h).trim());
    for (let i = incidentsHeaderRow + 1; i < values.length; i++) {
      const row = values[i];
      if (row.every(v => v === "" || v === null)) continue;
      const obj = {};
      for (let c = 0; c < header.length; c++) obj[header[c]] = row[c];
      incidents.push(obj);
    }
  }

  return { summaryRows, incidents };
}

/* -----------------------------
 * Auth (token)
 * ----------------------------- */

function assertToken_(params) {
  const expected = PropertiesService.getScriptProperties().getProperty(ONLYCAT_API.TOKEN_PROPERTY_KEY);
  if (!expected) throw new Error(`Missing Script Property ${ONLYCAT_API.TOKEN_PROPERTY_KEY}. Set it in Project Settings -> Script properties.`);
  const provided = String(params.token || "");
  if (!provided || provided !== expected) throw new Error("Unauthorized: bad or missing token.");
}

/* -----------------------------
 * Data discovery & parsing
 * ----------------------------- */

function findDataSheet_(ss) {
  const needed = new Set([
    "event_time_utc",
    "event_trigger_source",
    "event_classification",
    "rfid_code",
    "cat_label"
  ]);

  for (const sh of ss.getSheets()) {
    const lastCol = sh.getLastColumn();
    if (lastCol < 5) continue;
    const header = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(h => String(h).trim());
    const headerSet = new Set(header);
    let ok = true;
    needed.forEach(k => { if (!headerSet.has(k)) ok = false; });
    if (ok) return sh;
  }
  return null;
}

function readEvents_(sheet, tz) {
  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  const values = sheet.getRange(1, 1, lastRow, lastCol).getValues();

  const header = values[0].map(h => String(h).trim());
  const idx = indexMap_(header);

  const events = [];
  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const eventTimeStr = row[idx.event_time_utc];
    if (!eventTimeStr) continue;

    const eventTime = parseIsoUtc_(eventTimeStr);
    if (!eventTime) continue;

    const trigger = String(row[idx.event_trigger_source] || "").trim();
    const klass = String(row[idx.event_classification] || "").trim();
    const rfid = row[idx.rfid_code];
    const label = String(row[idx.cat_label] || "").trim();

    const catId = makeCatId_(label, rfid);
    const display = makeDisplayName_(label, rfid);

    events.push({
      event_time_utc_str: String(eventTimeStr),
      event_time: eventTime,
      event_time_local_str: formatLocal_(eventTime, tz),
      event_trigger_source: trigger,
      event_classification: klass,
      event_id: (idx.event_id >= 0) ? (row[idx.event_id] ?? "") : "",
      rfid_code: rfid ?? "",
      cat_label: label,
      cat_id: catId,
      display_name: display
    });
  }

  events.sort((a, b) => a.event_time.getTime() - b.event_time.getTime());
  return { header, events };
}

function indexMap_(header) {
  const map = {};
  header.forEach((h, i) => { map[h] = i; });

  const required = ["event_time_utc", "event_trigger_source", "event_classification", "rfid_code", "cat_label"];
  required.forEach(k => {
    if (map[k] === undefined) throw new Error("Missing required column: " + k);
  });

  map.event_id = (map.event_id !== undefined) ? map.event_id : -1;
  return map;
}

function parseIsoUtc_(s) {
  const d = new Date(s);
  if (isNaN(d.getTime())) return null;
  return d;
}

function formatLocal_(d, tz) {
  return Utilities.formatDate(d, tz, "yyyy-MM-dd HH:mm:ss");
}

function startOfDay_(d, tz) {
  const s = Utilities.formatDate(d, tz, "yyyy-MM-dd");
  return new Date(s + "T00:00:00");
}

function addDays_(d, days) {
  const x = new Date(d.getTime());
  x.setDate(x.getDate() + days);
  return x;
}

function makeCatId_(label, rfid) {
  if (label) return "LABEL:" + label;
  if (rfid !== "" && rfid !== null && rfid !== undefined) return "RFID:" + String(rfid);
  return "UNKNOWN";
}

function makeDisplayName_(label, rfid) {
  if (label) return label;
  if (rfid !== "" && rfid !== null && rfid !== undefined) return "RFID " + String(rfid);
  return "Unknown";
}

function isUnknownCat_(catId) {
  return !catId || String(catId).toUpperCase() === "UNKNOWN";
}

/* -----------------------------
 * Sessions
 * ----------------------------- */

function buildSessions_(events, tz) {
  const lastExitByCat = new Map();
  const rows = [];

  for (const e of events) {
    const cat = e.cat_id;
    if (isUnknownCat_(cat)) continue;

    if (e.event_trigger_source === "Exit Allowed") {
      lastExitByCat.set(cat, e);
    } else if (e.event_trigger_source === "Entry Allowed") {
      const exit = lastExitByCat.get(cat);
      if (exit && exit.event_time.getTime() < e.event_time.getTime()) {
        const mins = (e.event_time.getTime() - exit.event_time.getTime()) / 60000;
        const exitDateLocal = Utilities.formatDate(exit.event_time, tz, "yyyy-MM-dd");

        rows.push([
          cat,
          exit.display_name,
          exit.rfid_code,
          exit.event_time_utc_str,
          e.event_time_utc_str,
          round1_(mins),
          exitDateLocal
        ]);
        lastExitByCat.delete(cat);
      }
    }
  }

  return { rows };
}

/* -----------------------------
 * Stats (two windows)
 * - Averages divide by min(30, days-of-available-data in that window)
 * ----------------------------- */

function buildStats_(sessions, last30Start, prev30Start, today0, tz) {
  // Determine window days based on earliest session exit in each window.
  const daysLast = computeWindowDaysFromSessions_(sessions.rows, last30Start, today0, tz);
  const daysPrev = computeWindowDaysFromSessions_(sessions.rows, prev30Start, last30Start, tz);

  const byCat = new Map();

  for (const r of sessions.rows) {
    const cat = r[0];
    if (isUnknownCat_(cat)) continue;

    const display = r[1];
    const exitTime = parseIsoUtc_(r[3]);
    const mins = Number(r[5] || 0);

    if (!byCat.has(cat)) {
      byCat.set(cat, {
        cat_id: cat,
        display_name: display,
        lastTrips: 0,
        lastMins: 0,
        prevTrips: 0,
        prevMins: 0
      });
    }

    const bucket = byCat.get(cat);

    if (exitTime >= last30Start && exitTime < today0) {
      bucket.lastTrips += 1;
      bucket.lastMins += mins;
    } else if (exitTime >= prev30Start && exitTime < last30Start) {
      bucket.prevTrips += 1;
      bucket.prevMins += mins;
    }
  }

  const last30Rows = [];
  const prev30Rows = [];

  for (const rec of byCat.values()) {
    // Last 30 window
    const tripsDayLast = (daysLast > 0) ? (rec.lastTrips / daysLast) : 0;
    const minsTripLast = rec.lastTrips ? (rec.lastMins / rec.lastTrips) : 0;
    const minsDayLast = (daysLast > 0) ? (rec.lastMins / daysLast) : 0;

    last30Rows.push([
      rec.cat_id,
      rec.display_name,
      daysLast,
      rec.lastTrips,
      round2_(tripsDayLast),
      round1_(rec.lastMins),
      round1_(minsTripLast),
      round1_(minsDayLast)
    ]);

    // Previous 30 window
    const tripsDayPrev = (daysPrev > 0) ? (rec.prevTrips / daysPrev) : 0;
    const minsTripPrev = rec.prevTrips ? (rec.prevMins / rec.prevTrips) : 0;
    const minsDayPrev = (daysPrev > 0) ? (rec.prevMins / daysPrev) : 0;

    prev30Rows.push([
      rec.cat_id,
      rec.display_name,
      daysPrev,
      rec.prevTrips,
      round2_(tripsDayPrev),
      round1_(rec.prevMins),
      round1_(minsTripPrev),
      round1_(minsDayPrev)
    ]);
  }

  last30Rows.sort((a, b) => String(a[1]).localeCompare(String(b[1])));
  prev30Rows.sort((a, b) => String(a[1]).localeCompare(String(b[1])));

  return { last30Rows, prev30Rows };
}

function computeWindowDaysFromSessions_(sessionRows, windowStart, windowEnd, tz) {
  // Find earliest exit within [windowStart, windowEnd)
  let earliest = null;
  for (const r of sessionRows) {
    const exitTime = parseIsoUtc_(r[3]);
    if (!exitTime) continue;
    if (exitTime >= windowStart && exitTime < windowEnd) {
      if (!earliest || exitTime < earliest) earliest = exitTime;
    }
  }
  if (!earliest) return 0;

  const earliestDay0 = startOfDay_(earliest, tz);
  const ms = windowEnd.getTime() - earliestDay0.getTime();
  const days = Math.ceil(ms / 86400000);
  return Math.min(30, Math.max(1, days));
}

/* -----------------------------
 * Contraband (ignore UNKNOWN)
 * ----------------------------- */

function buildContraband_(events, last30Start) {
  const perCat = new Map();
  const incidents = [];

  for (const e of events) {
    if (isUnknownCat_(e.cat_id)) continue;

    const klass = (e.event_classification || "").toLowerCase();
    const isContraband = klass.includes("contraband");
    if (!isContraband) continue;

    if (!perCat.has(e.cat_id)) {
      perCat.set(e.cat_id, { cat_id: e.cat_id, display_name: e.display_name, last30: 0, all: 0 });
    }
    const rec = perCat.get(e.cat_id);
    rec.all += 1;
    if (e.event_time >= last30Start) rec.last30 += 1;

    if (e.event_time >= last30Start) {
      incidents.push([
        e.event_time_utc_str,
        e.cat_id,
        e.display_name,
        e.rfid_code,
        e.event_trigger_source,
        e.event_classification,
        e.event_id
      ]);
    }
  }

  const summaryRows = Array.from(perCat.values())
    .sort((a, b) => a.display_name.localeCompare(b.display_name))
    .map(r => [r.cat_id, r.display_name, r.last30, r.all]);

  incidents.sort((a, b) => parseIsoUtc_(b[0]).getTime() - parseIsoUtc_(a[0]).getTime());

  return { summaryRows, incidentRows: incidents };
}

/* -----------------------------
 * Cats list (keeps UNKNOWN here)
 * ----------------------------- */

function buildCats_(events, sessions, tz) {
  const byCat = new Map();

  for (const e of events) {
    if (!byCat.has(e.cat_id)) {
      byCat.set(e.cat_id, {
        cat_id: e.cat_id,
        display_name: e.display_name,
        rfid_code: e.rfid_code,
        firstSeen: e.event_time,
        lastSeen: e.event_time,
        totalEvents: 0,
        totalTrips: 0
      });
    }
    const rec = byCat.get(e.cat_id);
    rec.totalEvents += 1;
    if (e.event_time < rec.firstSeen) rec.firstSeen = e.event_time;
    if (e.event_time > rec.lastSeen) rec.lastSeen = e.event_time;

    if (e.cat_label) rec.display_name = e.cat_label;
    if (e.rfid_code !== "" && e.rfid_code !== null && e.rfid_code !== undefined) rec.rfid_code = e.rfid_code;
  }

  for (const r of sessions.rows) {
    const cat = r[0];
    const rec = byCat.get(cat);
    if (rec) rec.totalTrips += 1;
  }

  const rows = [];
  for (const rec of byCat.values()) {
    rows.push([
      rec.cat_id,
      rec.display_name,
      rec.rfid_code,
      formatLocal_(rec.firstSeen, tz),
      formatLocal_(rec.lastSeen, tz),
      rec.totalEvents,
      rec.totalTrips
    ]);
  }

  rows.sort((a, b) => String(b[4]).localeCompare(String(a[4])));
  return rows;
}

/* -----------------------------
 * Status (NEW): inside/outside + last change
 * - Derived from most recent Entry/Exit event per known cat.
 * - If last trigger is Exit Allowed => Outside
 * - If last trigger is Entry Allowed => Inside
 * ----------------------------- */

function buildStatus_(events, tz) {
  const latestByCat = new Map();

  for (const e of events) {
    if (isUnknownCat_(e.cat_id)) continue;

    const trigger = e.event_trigger_source;
    if (trigger !== "Exit Allowed" && trigger !== "Entry Allowed") continue;

    const prev = latestByCat.get(e.cat_id);
    if (!prev || e.event_time > prev.event_time) latestByCat.set(e.cat_id, e);
  }

  const now = new Date();
  const rows = [];

  for (const [catId, e] of latestByCat.entries()) {
    const status = (e.event_trigger_source === "Exit Allowed") ? "Outside" : "Inside";
    const minsSince = (now.getTime() - e.event_time.getTime()) / 60000;

    rows.push([
      catId,
      e.display_name,
      e.rfid_code,
      status,
      e.event_time_utc_str,
      formatLocal_(e.event_time, tz),
      round1_(minsSince),
      e.event_trigger_source
    ]);
  }

  // Stable by name
  rows.sort((a, b) => String(a[1]).localeCompare(String(b[1])));

  return { rows };
}

/* -----------------------------
 * Writing helpers
 * ----------------------------- */

function writeSheet_(ss, name, header, rows) {
  const sh = getOrCreateSheet_(ss, name);
  sh.clear();

  sh.getRange(1, 1, 1, header.length).setValues([header]);
  if (rows.length) sh.getRange(2, 1, rows.length, header.length).setValues(rows);

  sh.setFrozenRows(1);
  sh.autoResizeColumns(1, Math.min(header.length, 12));
  return sh;
}

function getOrCreateSheet_(ss, name) {
  return ss.getSheetByName(name) || ss.insertSheet(name);
}

/* -----------------------------
 * Small utils
 * ----------------------------- */

function round1_(x) { return Math.round(x * 10) / 10; }
function round2_(x) { return Math.round(x * 100) / 100; }