# OnlyCatEventLogger

Listens to the OnlyCat catflap Socket.IO API and appends every event to a Google Sheet (the Sheet is the source of truth).

## Prerequisites
- Java 17+
- Gradle 8+ (or use a wrapper if you add one later)
- Google Cloud project with Sheets API enabled
- Service Account JSON credentials (do **not** commit); share the target Google Sheet with the service account email

## Step 1: Set up your Google Sheet
1) Create a Google Cloud project (or use an existing one) and enable the **Google Sheets API**.  
2) Create a **Service Account** and download its key:  
   - In Google Cloud Console, go to **IAM & Admin → Service Accounts**.  
   - Click **Create Service Account**, give it a name, and finish the wizard.  
   - Open the new service account, go to the **Keys** tab.  
   - Click **Add Key → Create new key → JSON**, then download the file.  
   - Store it somewhere safe (example: `~/secrets/onlycat-sa.json`).  
3) Create a new Google Sheet (or pick an existing one). Copy the **Spreadsheet ID** from the URL.  
4) Share the Sheet with the service account email (so it can append rows).  
5) Decide the **tab name** you want to write to (example: `OnlyCatEvents`).  
   - If the tab is empty, the app will write the header row for you.

## Step 2: Add your config to the project
Copy the example config file:
```bash
cp src/main/resources/application.example.yml src/main/resources/application.yml
```

Open `src/main/resources/application.yml` and fill in these fields:
```yaml
onlycat:
  token: "${ONLYCAT_TOKEN:replace-with-token}"

sheets:
  credentialsPath: "/path/to/service-account.json"
  spreadsheetId: "your-sheet-id"
  sheetName: "OnlyCatEvents"
  appendRange: "OnlyCatEvents!A1"
```

Tips:
- Prefer setting `ONLYCAT_TOKEN` as an environment variable instead of storing it in the file.
- `credentialsPath` must point to the JSON file you downloaded in step 1.
- `spreadsheetId` is the long ID in the Sheet URL.
- `sheetName` and `appendRange` must match your tab name.

Optional: Some cats can have multiple RFID chips (like mine) to merge multiple cat labels into one, edit `src/main/resources/cat-label-mapping.yml`:
```yaml
cat-label-mapping:
  aliases:
    "Cleo": "Cleo"
    "Cleo 2": "Cleo"
```

## Step 3a: Run once (foreground)
```bash
./gradlew bootRun
```
This runs in your terminal and will stop if you close the terminal or log out.
Make sure `sheets.credentialsPath` points to a real JSON file before running.

## Step 3b: Run in the background (auto-start on reboot)
This installs the app to `/Applications/OnlyCatEventLogger` and runs it at boot, even when no user is logged in.

1) Build a runnable jar:
```bash
./gradlew bootJar
```

2) Install the app to `/Applications`:
Shortcut: run the installer script to do everything automatically:
```bash
scripts/install_launchd.sh
```

If you don't want to run the script you can follow the detailed steps below.

```bash
sudo mkdir -p /Applications/OnlyCatEventLogger
sudo cp build/libs/*.jar /Applications/OnlyCatEventLogger/OnlyCatEventLogger.jar
sudo cp src/main/resources/application.yml /Applications/OnlyCatEventLogger/application.yml
```

3) Create a launchd job at `/Library/LaunchDaemons/com.onlycat.eventlogger.plist` (template in `scripts/com.onlycat.eventlogger.plist`):

Save that file, then load it:
```bash
sudo launchctl load -w /Library/LaunchDaemons/com.onlycat.eventlogger.plist
```

To stop it:
```bash
sudo launchctl unload -w /Library/LaunchDaemons/com.onlycat.eventlogger.plist
```

Notes:
- Edit `/Applications/OnlyCatEventLogger/application.yml` when you need to update config.
- Logs will appear in `/Library/Logs/OnlyCatEventLogger.out.log` and `/Library/Logs/OnlyCatEventLogger.err.log`.

## Step 4 (optional): Add the Google Sheets Apps Script
This adds helpful summary sheets (Sessions, Stats, Contraband, Cats) to your spreadsheet.

1) In your Google Sheet, go to **Extensions → Apps Script**.  
2) Replace the contents with `scripts/apps_script.js` from this repo.  
3) Save, then reload the Google Sheet.  
4) You should see a new menu: **OnlyCat → Refresh stats**.

Notes:
- The script looks for a data tab with headers including:  
  `event_time_utc`, `event_trigger_source`, `event_classification`, `rfid_code`, `cat_label`
- These headers are written automatically by the app when the sheet is empty.

## Troubleshooting
- **No rows appear in the Sheet**: Confirm the app is running and connected, then check that `sheets.spreadsheetId` matches the long ID in the Sheet URL (between `/d/` and `/edit`).  
- **“Permission denied” or “The caller does not have permission”**: The Sheet must be shared with the **service account email** found inside your JSON file under `client_email`.  
- **Apps Script menu doesn’t show up**: In the Sheet, go to **Extensions → Apps Script**, save the script, then reload the Sheet tab in your browser.  
- **“Google Sheets API has not been used” error**: In Google Cloud Console, open **APIs & Services → Enabled APIs & services** and enable **Google Sheets API**.  
- **“File not found” for credentialsPath**: Double-check the absolute path and that the JSON file exists on this machine.  
- **Sheet/tab name mismatch**: `sheets.sheetName` and the tab label in the Sheet must match exactly (case-sensitive).  

## Project layout
- `src/main/java/com/onlycat/ingest` – app entrypoint and configuration
- `onlycat/onlycat` – Socket.IO client that streams events
- `onlycat/service` – event normalization + dedupe
- `onlycat/sheets` – Google Sheets appender
- `application.yml` – configuration placeholders

## Behaviour notes
- Connects to `gateway.onlycat.com` via Socket.IO with reconnection/backoff. Token is sent but never logged.  
- Registers a catch-all handler when available; otherwise listens to common event names and `message`. Logs the first two payload samples at INFO.  
- If the library does not expose a catch-all, a low-level packet interceptor captures every event name/payload and forwards it for normalization. The first 50 intercepted events log at INFO (`Packet intercept nsp=... type=... event=...`) to help verify event names; the rest log at DEBUG.  
- Forced listen-only: no outbound emits after connect. The smoke-test/subscription fields remain in config for potential future use but are inactive.  
- Each inbound event is normalized into `OnlyCatEvent` and appended to Sheets in near-real-time. Value input option is `RAW` to avoid Sheets re-formatting the JSON.  
- Dedupe: in-memory LRU (512 keys) keyed by hash(raw_json + event_time) to reduce duplicates after reconnects.  
- `raw_json` is compacted JSON and truncated to 45k characters to stay under the Google Sheets cell limit; truncation is indicated with `...(truncated)`.  
- Header columns (auto-created if the sheet is empty):
  1. ingested_at_utc  
  2. event_time_utc  
  3. event_name  
  4. event_type  
  5. event_id  
  6. event_trigger_source  
  7. event_classification  
  8. global_id  
  9. device_id  
  10. rfid_code  
  11. cat_label  

## Tests
Run `./gradlew test` to exercise the dedupe cache and event normalization samples.
