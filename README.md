# OnlyCatEventLogger

Spring Boot 3.x (Java 17) starter that listens to the OnlyCat catflap Socket.IO API and appends every event to a Google Sheet (the Sheet is the source of truth).

## Project layout
- `src/main/java/com/onlycat/ingest` – app entrypoint and configuration
- `onlycat/onlycat` – Socket.IO client that streams events
- `onlycat/service` – event normalization + dedupe
- `onlycat/sheets` – Google Sheets appender
- `application.yml` – configuration placeholders

## Scaffold (for reference)
Using Spring Boot CLI (install via `brew install springboot` on macOS):
```bash
spring init --build=gradle --java-version=17 --dependencies=actuator,configuration-processor OnlyCatEventLogger
```
(The project here is already laid out; the command is documented for future regeneration.)

## Prerequisites
- Java 17+
- Gradle 8+ (or use a wrapper if you add one later)
- Spring Boot CLI (only needed if you want to regenerate via the command above)
- Google Cloud project with Sheets API enabled
- Service Account JSON credentials (do **not** commit); share the target Google Sheet with the service account email

## Google Sheets setup
1) Create/choose a Google Cloud project. Enable “Google Sheets API”.  
2) Create a Service Account. Generate a JSON key and download it locally (e.g., `~/secrets/onlycat-sa.json`).  
3) Create a Google Sheet. Copy its ID from the URL. Share the sheet with the service account email so it can append rows.  
4) Decide your tab name (e.g., `OnlyCatEvents`); ensure the sheet has no header or let the app write it. The header columns are written automatically when the sheet is empty.

## Config
Copy the template and keep your real secrets out of git:
```bash
cp src/main/resources/application.example.yml src/main/resources/application.yml
```
`.gitignore` already excludes `src/main/resources/application.yml`. Override via env vars as needed:
```yaml
onlycat:
  gatewayUrl: https://gateway.onlycat.com
  token: "${ONLYCAT_TOKEN:replace-with-token}"
  requestDeviceListEvent: getDevices  # smoke-test emit on connect

sheets:
  credentialsPath: "/path/to/service-account.json"
  spreadsheetId: "your-sheet-id"
  sheetName: "OnlyCatEvents"
  appendRange: "OnlyCatEvents!A1"
```
Recommended: export `ONLYCAT_TOKEN` rather than storing it in the file.

## Run
```bash
./gradlew bootRun
# or
./gradlew test
```
Ensure `sheets.credentialsPath` points to an existing Service Account JSON before running; the app will fail fast if the file is missing.
Actuator health: `curl http://localhost:8080/actuator/health`

## Behaviour notes
- Connects to `gateway.onlycat.com` via Socket.IO with reconnection/backoff. Token is sent but never logged.  
- Registers a catch-all handler when available; otherwise listens to common event names and `message`. Logs the first two payload samples at INFO.  
- If the library does not expose a catch-all, a low-level packet interceptor captures every event name/payload and forwards it for normalization. The first 50 intercepted events log at INFO (`Packet intercept nsp=... type=... event=...`) to help verify event names; the rest log at DEBUG.  
- Emits a smoke-test event (default `getDevices`) after connect for quick feedback.  
- Each inbound event is normalized into `OnlyCatEvent` and appended to Sheets in near-real-time. Value input option is `RAW` to avoid Sheets re-formatting the JSON.  
- Dedupe: in-memory LRU (512 keys) keyed by hash(raw_json + event_time) to reduce duplicates after reconnects.  
- `raw_json` is compacted JSON and truncated to 45k characters to stay under the Google Sheets cell limit; truncation is indicated with `...(truncated)`.  
- Header columns (auto-created if the sheet is empty):
  1. ingested_at_utc  
  2. event_time_utc  
  3. event_type  
  4. direction  
  5. cat_name  
  6. cat_id  
  7. device_name  
  8. device_id  
  9. outcome  
  10. prey_detected  
  11. raw_json  

## Secrets
- `onlycat.token` is sensitive; prefer env vars.  
- `sheets.credentialsPath` must point to the service account JSON; ensure the file permissions are locked down locally.

## Tests
Run `./gradlew test` to exercise the dedupe cache and event normalization samples.
