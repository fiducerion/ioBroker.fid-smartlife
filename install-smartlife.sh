#!/bin/bash
# install-smartlife.sh - Installer/Updater fuer iobroker.fid-smartlife
#
# Usage:  bash install-smartlife.sh /pfad/zur/fid-smartlife.zip
#
# Modi: install | update | recovery
#  - install : Adapter-Ordner gibt's noch nicht UND keinen Eintrag in Object-DB
#  - update  : Adapter-Ordner und Object-DB-Record existieren
#  - recovery: Object-DB hat Adapter aber Ordner fehlt (z.B. nach Debian-Update)
#
set -e
ZIP="$1"
TARGET="/opt/iobroker/node_modules/iobroker.fid-smartlife"
IOB_USER="iobroker"
IOB_GROUP="iobroker"
TMPDIR="/tmp/fid-smartlife-$$"

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
grn()   { printf '\033[32m%s\033[0m\n' "$*"; }
yel()   { printf '\033[33m%s\033[0m\n' "$*"; }
blu()   { printf '\033[34m%s\033[0m\n' "$*"; }
green() { grn "$@"; }
iob()   { sudo -u "$IOB_USER" iobroker "$@"; }

MANIFEST=(
  "package.json" "io-package.json" "main.js" "README.md"
  "lib/tuyaCloud.js" "lib/specMapper.js" "lib/tuyaLocal.js" "lib/lanDiscovery.js"
  "admin/jsonConfig.json"
  "admin/fid-smartlife.png"
  "admin/i18n/de/translations.json"
  "admin/i18n/en/translations.json"
)

[ -z "$ZIP" ]    && { red "Usage: bash install-smartlife.sh <pfad_zur_zip>"; exit 1; }
[ ! -f "$ZIP" ] && { red "ZIP nicht gefunden: $ZIP"; exit 1; }

blu "Fiducerion Smartlife Installer"
blu "  ZIP    = $ZIP"
blu "  TARGET = $TARGET"
echo

INSTALL_MODE="update"

# Recovery: Object-DB hat den Adapter, aber Ordner fehlt
if [ ! -d "$TARGET" ]; then
  if sudo -u "$IOB_USER" iobroker object get system.adapter.fid-smartlife.0 >/dev/null 2>&1; then
    yel "==> RECOVERY-Modus: Object-DB hat den Adapter, aber Ordner fehlt."
    INSTALL_MODE="recovery"
  fi
fi

# Erstinstallation: weder Ordner noch DB-Record
if [ ! -d "$TARGET" ] && [ "$INSTALL_MODE" = "update" ]; then
  yel "==> Erstinstallation: lege $TARGET an."
  INSTALL_MODE="install"
fi

# Ziel-Verzeichnis
if [ ! -d "$TARGET" ]; then
  sudo mkdir -p "$TARGET"
  sudo chown "$IOB_USER:$IOB_GROUP" "$TARGET"
fi

# Adapter stoppen falls laufend
yel "==> Stoppe Adapter (falls aktiv)..."
iob stop fid-smartlife.0 2>/dev/null || true

# ZIP entpacken
yel "==> Entpacke ZIP nach $TMPDIR..."
mkdir -p "$TMPDIR"
unzip -q -o "$ZIP" -d "$TMPDIR"

# Source-Root im TMPDIR finden
SRC="$TMPDIR/iobroker.fid-smartlife"
[ ! -d "$SRC" ] && SRC="$TMPDIR"

# Manifest pruefen
for f in "${MANIFEST[@]}"; do
  if [ ! -f "$SRC/$f" ]; then
    red "  FEHLT in ZIP: $f"
    exit 1
  fi
done
grn "  OK: alle ${#MANIFEST[@]} Files in der ZIP vorhanden."

# Files kopieren
yel "==> Kopiere Files nach $TARGET ..."
for f in "${MANIFEST[@]}"; do
  sudo mkdir -p "$TARGET/$(dirname "$f")"
  sudo cp "$SRC/$f" "$TARGET/$f"
done
sudo chown -R "$IOB_USER:$IOB_GROUP" "$TARGET"
grn "  OK: alle ${#MANIFEST[@]} Files am Ziel."

VERSION=$(grep '"version"' "$TARGET/io-package.json" | head -1 | sed 's/.*"version": *"\([^"]*\)".*/\1/')
grn "  io-package.json version: $VERSION"

# npm install nur bei Bedarf
yel "==> npm install pruefen..."
NEED_NPM=0
[ ! -d "$TARGET/node_modules" ] && NEED_NPM=1
[ "$TARGET/package.json" -nt "$TARGET/node_modules/.package-lock.json" ] 2>/dev/null && NEED_NPM=1
if [ "$NEED_NPM" = "1" ]; then
  yel "  package.json geaendert oder node_modules fehlt - npm install..."
  (cd "$TARGET" && sudo -u "$IOB_USER" npm install --omit=dev --no-audit --no-fund)
  sudo chown -R "$IOB_USER:$IOB_GROUP" "$TARGET/node_modules"
else
  grn "  uebersprungen"
fi

# Aufraeumen
rm -rf "$TMPDIR"

# Bei Erstinstall: Adapter registrieren + Instance anlegen
if [ "$INSTALL_MODE" = "install" ]; then
  yel "==> Registriere Adapter in ioBroker..."
  iob url "$TARGET" 2>/dev/null || true

  if ! iob object get system.adapter.fid-smartlife.0 >/dev/null 2>&1; then
    yel "==> Lege Instance fid-smartlife.0 an..."
    iob add fid-smartlife -h $(hostname) 2>/dev/null || iob add fid-smartlife 2>/dev/null || true
    sleep 1
  fi

  grn ""
  grn "  WICHTIG: Im Admin-UI musst du jetzt"
  grn "    1. Tuya Access ID + Access Secret eintragen"
  grn "    2. Region waehlen"
  grn "    3. Speichern"
  grn ""
fi

# Recovery: installedVersion in der DB setzen
if [ "$INSTALL_MODE" = "recovery" ]; then
  yel "==> Recovery: setze installedVersion in der Object-DB auf $VERSION ..."
  sudo -u "$IOB_USER" iobroker object set system.adapter.fid-smartlife \
    common.installedVersion="$VERSION" 2>/dev/null || true
  grn "    installedVersion=$VERSION gesetzt."
fi

# ---- installedFrom via npm-Tarball - echte Reboot-Resistenz ----
TARBALL_PATH="/opt/iobroker/.fid-smartlife-tarball.tgz"
yel "==> Erzeuge npm-Tarball fuer Recovery: $TARBALL_PATH"
pushd "$TARGET" > /dev/null
sudo -u "$IOB_USER" npm pack 2>/dev/null > /tmp/fid-smartlife-pack.out || true
PACKED=$(sudo -u "$IOB_USER" sh -c "ls -t iobroker.fid-smartlife-*.tgz 2>/dev/null | head -1")
popd > /dev/null
if [ -n "$PACKED" ] && [ -f "$TARGET/$PACKED" ]; then
  sudo mv "$TARGET/$PACKED" "$TARBALL_PATH"
  sudo chown "$IOB_USER:$IOB_GROUP" "$TARBALL_PATH"
  grn "  Tarball: $(ls -lh $TARBALL_PATH | awk '{print $5}')"
  INSTALLED_FROM_VALUE="file:$TARBALL_PATH"
else
  yel "  Tarball-Erzeugung fehlgeschlagen, Fallback auf Pfad-Referenz"
  INSTALLED_FROM_VALUE="$TARGET"
fi

yel "==> setze installedFrom=$INSTALLED_FROM_VALUE ..."
sudo -u "$IOB_USER" iobroker object set system.adapter.fid-smartlife \
  common.installedFrom="$INSTALLED_FROM_VALUE" 2>/dev/null || true
sudo -u "$IOB_USER" iobroker object set system.adapter.fid-smartlife \
  common.installedVersion="$VERSION" 2>/dev/null || true

# ---- Watchdog-Cron installieren ----
WATCHDOG_SCRIPT="/opt/iobroker/.fid-smartlife-watchdog.sh"
yel "==> Installiere Watchdog: $WATCHDOG_SCRIPT"
sudo tee "$WATCHDOG_SCRIPT" > /dev/null <<'WATCHDOG_EOF'
#!/bin/bash
# fid-smartlife Watchdog
# Konservatives Recovery: nur restaurieren wenn die HAUPT-Files weg sind.
# KEIN automatisches 'npm install' (das blockiert die DB und triggert Loops).
# KEIN automatisches 'iobroker restart' (das macht js-controller selber).
# Stattdessen: nur Files zurueckspielen und einen Marker setzen damit
# Bernd manuell entscheiden kann.
TARGET="/opt/iobroker/node_modules/iobroker.fid-smartlife"
TARBALL="/opt/iobroker/.fid-smartlife-tarball.tgz"
SENTINEL="$TARGET/lib/tuyaCloud.js"
LOGFILE="/opt/iobroker/log/fid-smartlife-watchdog.log"
COOLDOWN_FILE="/opt/iobroker/.fid-smartlife-watchdog.cooldown"
COOLDOWN_SEC=3600

ts() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(ts)] $1" >> "$LOGFILE"; }

# Alles ok -> sofort raus, kein log
if [ -f "$SENTINEL" ]; then
  exit 0
fi

# Anti-Loop: wenn wir vor weniger als COOLDOWN_SEC schon restauriert haben,
# nicht nochmal. Sonst kann eine kaputte Tarball-Installation einen
# Endlos-Restore-Loop ausloesen.
if [ -f "$COOLDOWN_FILE" ]; then
  LAST_TS=$(stat -c %Y "$COOLDOWN_FILE" 2>/dev/null || echo 0)
  NOW=$(date +%s)
  AGE=$((NOW - LAST_TS))
  if [ "$AGE" -lt "$COOLDOWN_SEC" ]; then
    # Trotz Cooldown loggen damit Bernd sieht dass weiter was kaputt ist
    log "Sentinel $SENTINEL fehlt aber Cooldown aktiv (${AGE}s < ${COOLDOWN_SEC}s) - kein Eingriff"
    exit 0
  fi
fi

log "FAIL: $SENTINEL fehlt - starte konservatives Recovery (NUR Files, KEIN npm, KEIN restart)"
if [ ! -f "$TARBALL" ]; then
  log "  Kein Tarball bei $TARBALL - manueller Eingriff noetig"
  exit 1
fi

TMPDIR="/tmp/fid-smartlife-restore-$$"
mkdir -p "$TMPDIR"
if ! tar -xzf "$TARBALL" -C "$TMPDIR" 2>/dev/null; then
  log "  Tarball-Extraktion fehlgeschlagen - manueller Eingriff noetig"
  rm -rf "$TMPDIR"
  exit 1
fi
SRC="$TMPDIR/package"
[ -d "$SRC" ] || SRC="$TMPDIR"

mkdir -p "$TARGET"
cp -a "$SRC/." "$TARGET/"
chown -R iobroker:iobroker "$TARGET"
rm -rf "$TMPDIR"
touch "$COOLDOWN_FILE"
log "  Files wiederhergestellt. node_modules nicht angefasst."
log "  Falls Adapter trotzdem nicht startet -> manuell: bash install-smartlife.sh"
WATCHDOG_EOF
sudo chmod +x "$WATCHDOG_SCRIPT"
sudo chown root:root "$WATCHDOG_SCRIPT"

CRON_LINE="0 * * * * $WATCHDOG_SCRIPT"
if ! sudo crontab -l 2>/dev/null | grep -qF "$WATCHDOG_SCRIPT"; then
  (sudo crontab -l 2>/dev/null; echo "$CRON_LINE") | sudo crontab -
  grn "  Watchdog-Cron installiert: $CRON_LINE"
else
  yel "  Watchdog-Cron schon vorhanden."
fi

# Upload + Start
yel "==> iobroker upload fid-smartlife..."
iob upload fid-smartlife

yel "==> Starte Adapter..."
iob start fid-smartlife.0 2>/dev/null || true

grn ""
grn "===================================================="
grn "  Update fertig - Mode: $INSTALL_MODE"
grn "  Version:        $VERSION"
grn "===================================================="
