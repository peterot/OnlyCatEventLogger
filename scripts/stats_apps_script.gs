/**
 * OnlyCat Stats Builder (v2 - no Charts)
 *
 * Creates/refreshes these sheets:
 * - Sessions
 * - Stats - Last 30
 * - Stats - Previous 30
 * - Contraband
 * - Cats
 *
 * Changes:
 * - Removes Charts entirely (no Charts sheet, no chart building)
 * - Ignores cat_id === "UNKNOWN" in Sessions / Stats / Contraband
 * - Per-day averages divide by days-covered-so-far in the period (capped at 30),
 *   instead of always dividing by 30 when you only have a couple of days of data.
 *
 * Assumptions:
 * - You have a data tab with headers including:
 *   event_time_utc, event_trigger_source, event_classification, rfid_code, cat_label
 * - event_time_utc values are ISO strings like 2026-01-21T12:53:27Z
 * - event_trigger_source uses display names: "Exit Allowed" / "Entry Allowed"
 * - event_classification uses display names: e.g. "Contraband"
 */

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

  // Build sessions (Exit -> next Entry per cat), excluding UNKNOWN
  const sessions = buildSessions_(data.events, tz);

  // Window boundaries in sheet timezone (today at 00:00 local)
  const today0 = startOfDay_(new Date(), tz);
  const last30Start = addDays_(today0, -30);
  const prev30Start = addDays_(today0, -60);

  // Days covered so far (capped at 30) based on earliest session exit we’ve seen
  const earliestExit = earliestSessionExit_(sessions.rows); // Date or null
  const daysLast = daysCoveredSoFar_(earliestExit, last30Start, today0, 30, tz);
  const daysPrev = daysCoveredSoFar_(earliestExit, prev30Start, last30Start, 30, tz);

  // Stats tables per cat
  const stats = buildStatsTables_(sessions, last30Start, prev30Start, today0, daysLast, daysPrev);

  // Contraband from raw events (excluding UNKNOWN)
  const contraband = buildContraband_(data.events, last30Start, tz);

  // Cats list from raw events + sessions trip counts (Cats tab can include UNKNOWN)
  const catsRows = buildCats_(data.events, sessions, tz);

  // Write sheets (friendly headers)
  writeSheet_(ss, "Sessions",
    ["Cat ID", "Cat", "RFID", "Exit time (UTC)", "Entry time (UTC)", "Minutes outside", "Exit date (local)"],
    sessions.rows
  );

  writeSheet_(ss, "Stats - Last 30",
    ["Cat ID", "Cat", "Days counted", "Trips", "Trips per day", "Avg minutes per trip", "Minutes outside (total)", "Minutes outside per day"],
    stats.lastRows
  );

  writeSheet_(ss, "Stats - Previous 30",
    ["Cat ID", "Cat", "Days counted", "Trips", "Trips per day", "Avg minutes per trip", "Minutes outside (total)", "Minutes outside per day"],
    stats.prevRows
  );

  // Contraband sheet: summary table + incidents table below
  const contrabandSheet = getOrCreateSheet_(ss, "Contraband");
  contrabandSheet.clear();

  const summaryHeader = ["Cat ID", "Cat", "Contraband (last 30 days)", "Contraband (all time)"];
  const summary = contraband.summaryRows;

  const incidentsHeader = ["Event time (UTC)", "Cat ID", "Cat", "RFID", "Trigger", "Classification", "Event ID"];
  const incidents = contraband.incidentRows;

  contrabandSheet.getRange(1, 1, 1, summaryHeader.length).setValues([summaryHeader]);
  if (summary.length) contrabandSheet.getRange(2, 1, summary.length, summaryHeader.length).setValues(summary);

  const startRow = 3 + Math.max(summary.length, 1);
  contrabandSheet.getRange(startRow, 1, 1, incidentsHeader.length).setValues([incidentsHeader]);
  if (incidents.length) contrabandSheet.getRange(startRow + 1, 1, incidents.length, incidentsHeader.length).setValues(incidents);

  contrabandSheet.setFrozenRows(1);
  contrabandSheet.autoResizeColumns(1, Math.min(incidentsHeader.length, 12));

  writeSheet_(ss, "Cats",
    ["Cat ID", "Cat", "RFID", "First seen (local)", "Last seen (local)", "Total events", "Total outside trips"],
    catsRows
  );

  SpreadsheetApp.getUi().alert("OnlyCat stats refreshed.");
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

    const eventId = (idx.event_id >= 0) ? (row[idx.event_id] ?? "") : "";

    events.push({
      event_time_utc_str: String(eventTimeStr),
      event_time: eventTime,
      event_time_local_str: formatLocal_(eventTime, tz),
      event_trigger_source: trigger,
      event_classification: klass,
      event_id: eventId,
      rfid_code: (rfid ?? ""),
      cat_label: label,
      cat_id: catId,
      display_name: makeDisplayName_(label, rfid)
    });
  }

  // Sort by event_time
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

/* -----------------------------
 * Sessions
 * ----------------------------- */

function buildSessions_(events, tz) {
  // For each cat, pair Exit Allowed -> next Entry Allowed
  // Ignore UNKNOWN cats entirely
  const lastExitByCat = new Map();
  const rows = [];

  for (const e of events) {
    const cat = e.cat_id;
    if (cat === "UNKNOWN") continue;

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

function earliestSessionExit_(sessionRows) {
  // sessionRows cols: 3 = exit_time_utc
  let earliest = null;
  for (const r of sessionRows) {
    const d = parseIsoUtc_(r[3]);
    if (!d) continue;
    if (!earliest || d.getTime() < earliest.getTime()) earliest = d;
  }
  return earliest;
}

function daysCoveredSoFar_(earliestExit, periodStart, periodEnd, capDays, tz) {
  // periodEnd is exclusive boundary (e.g. today0)
  if (!earliestExit) return 0;

  // Effective start is the later of periodStart, and the first day we have any data for (based on earliest exit).
  let effectiveStart = periodStart;
  const earliestDay0 = startOfDay_(earliestExit, tz);
  if (earliestDay0.getTime() > periodStart.getTime()) effectiveStart = earliestDay0;

  if (effectiveStart.getTime() >= periodEnd.getTime()) return 0;

  const msPerDay = 24 * 60 * 60 * 1000;
  const days = Math.ceil((periodEnd.getTime() - effectiveStart.getTime()) / msPerDay);

  return Math.max(0, Math.min(capDays, days));
}

/* -----------------------------
 * Stats
 * ----------------------------- */

function buildStatsTables_(sessions, last30Start, prev30Start, today0, daysLast, daysPrev) {
  // sessions.rows columns:
  // 0 cat_id, 1 display_name, 2 rfid, 3 exit_time_utc, 4 entry_time_utc, 5 minutes_out, 6 exit_date_local
  const byCat = new Map();

  for (const r of sessions.rows) {
    const cat = r[0];
    if (cat === "UNKNOWN") continue;

    const display = r[1];
    const exitUtcStr = r[3];
    const exitTime = parseIsoUtc_(exitUtcStr);
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

  const lastRows = [];
  const prevRows = [];

  for (const rec of byCat.values()) {
    // Last period
    const tripsDayLast = (daysLast > 0) ? (rec.lastTrips / daysLast) : 0;
    const minsTripLast = rec.lastTrips ? (rec.lastMins / rec.lastTrips) : 0;
    const minsDayLast = (daysLast > 0) ? (rec.lastMins / daysLast) : 0;

    lastRows.push([
      rec.cat_id,
      rec.display_name,
      daysLast,
      rec.lastTrips,
      round2_(tripsDayLast),
      round1_(minsTripLast),
      round1_(rec.lastMins),
      round1_(minsDayLast)
    ]);

    // Previous period
    const tripsDayPrev = (daysPrev > 0) ? (rec.prevTrips / daysPrev) : 0;
    const minsTripPrev = rec.prevTrips ? (rec.prevMins / rec.prevTrips) : 0;
    const minsDayPrev = (daysPrev > 0) ? (rec.prevMins / daysPrev) : 0;

    prevRows.push([
      rec.cat_id,
      rec.display_name,
      daysPrev,
      rec.prevTrips,
      round2_(tripsDayPrev),
      round1_(minsTripPrev),
      round1_(rec.prevMins),
      round1_(minsDayPrev)
    ]);
  }

  lastRows.sort((a, b) => String(a[1]).localeCompare(String(b[1])));
  prevRows.sort((a, b) => String(a[1]).localeCompare(String(b[1])));

  return { lastRows, prevRows };
}

/* -----------------------------
 * Contraband
 * ----------------------------- */

function buildContraband_(events, last30Start, tz) {
  const perCat = new Map();
  const incidents = [];

  for (const e of events) {
    if (e.cat_id === "UNKNOWN") continue;

    const klass = (e.event_classification || "").toLowerCase();
    const isContraband = (klass.indexOf("contraband") >= 0);
    if (!isContraband) continue;

    if (!perCat.has(e.cat_id)) {
      perCat.set(e.cat_id, {
        cat_id: e.cat_id,
        display_name: e.display_name,
        last30: 0,
        all: 0
      });
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

  // Sort incidents newest first
  incidents.sort((a, b) => parseIsoUtc_(b[0]).getTime() - parseIsoUtc_(a[0]).getTime());

  return { summaryRows, incidentRows: incidents };
}

/* -----------------------------
 * Cats list
 * ----------------------------- */

function buildCats_(events, sessions, tz) {
  const byCat = new Map();

  // Total events and first/last seen from raw events
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

  // Total outside trips from sessions (sessions already excludes UNKNOWN)
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

  // Most recent first
  rows.sort((a, b) => String(b[4]).localeCompare(String(a[4])));
  return rows;
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