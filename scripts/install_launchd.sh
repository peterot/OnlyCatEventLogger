#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/Applications/OnlyCatEventLogger"
JAR_PATH="${APP_DIR}/OnlyCatEventLogger.jar"
YAML_PATH="${APP_DIR}/application.yml"
PLIST_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/com.onlycat.eventlogger.plist"
PLIST_DST="/Library/LaunchDaemons/com.onlycat.eventlogger.plist"

./gradlew bootJar

sudo mkdir -p "${APP_DIR}"
sudo cp build/libs/*.jar "${JAR_PATH}"
sudo cp src/main/resources/application.yml "${YAML_PATH}"

sudo cp "${PLIST_SRC}" "${PLIST_DST}"
sudo chown root:wheel "${PLIST_DST}"
sudo chmod 644 "${PLIST_DST}"

sudo launchctl unload -w "${PLIST_DST}" >/dev/null 2>&1 || true
sudo launchctl load -w "${PLIST_DST}"

echo "Installed and started com.onlycat.eventlogger"
echo "Logs: /Library/Logs/OnlyCatEventLogger.out.log and /Library/Logs/OnlyCatEventLogger.err.log"
