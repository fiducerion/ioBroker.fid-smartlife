/* iobroker.fid-smartlife - main.js
 *
 * Cloud-First Tuya/Smartlife Adapter. Portiert vom TuyaCloudReplace v2.5.2-Skript.
 *
 * Lifecycle:
 *   1. onReady: Config laden, Cloud-Client erstellen, Discovery
 *   2. Pro Geraet: Spec laden, States anlegen, initialen Status holen
 *   3. Subscriptions setzen (alle .* States auf Write hoeren)
 *   4. Periodisches Polling alle pollIntervalSec
 *   5. Periodische Rediscovery alle rediscoverIntervalMin
 *
 * State-Layout:
 *   fid-smartlife.0.<deviceId>._name          Anzeigename
 *   fid-smartlife.0.<deviceId>.online         Cloud-online
 *   fid-smartlife.0.<deviceId>._noCloudStatusPoll  pro Geraet abschaltbar
 *   fid-smartlife.0.<deviceId>.<dpCanon>      pro DP ein State
 *   fid-smartlife.0.<deviceId>.on             Alias (wenn ableitbar)
 *   fid-smartlife.0.<deviceId>.brightness     Alias
 *   fid-smartlife.0.<deviceId>.color_temp_k   Alias
 *   ...
 */
'use strict';

// === KRITISCH: process-level Error-Handler MUSS vor allen requires registriert werden ===
// tuyapi-Library emit-t Errors auf internen Socket-Objects die wir mit normalem
// try/catch nicht zuverlaessig fangen koennen. Wenn ein v3.4/v3.5-Device offline
// ist (EHOSTUNREACH, connection timed out, etc.), crashed sonst der ganze Adapter.
//
// Wir registrieren den Handler hier ganz am Anfang, vor allen require()s, damit er
// garantiert aktiv ist bevor tuyapi geladen werden kann.
if (!global._fidSmartlifeSocketErrorHandlerInstalled) {
  global._fidSmartlifeSocketErrorHandlerInstalled = true;

  const isSocketError = (err) => {
    const msg = String(err && err.message || err);
    const stack = String(err && err.stack || '');
    return (
      /Error from socket/i.test(msg) ||
      /EHOSTUNREACH|ECONNREFUSED|ECONNRESET|ETIMEDOUT|EPIPE|ENETUNREACH|ENOTFOUND|ECONNABORTED/i.test(msg) ||
      /connection timed out|connection refused|connection reset|socket hang up/i.test(msg) ||
      /read ECONNRESET|write EPIPE/i.test(msg) ||
      /tuyapi[\/\\]index\.js/i.test(stack) ||
      /TuyaDevice instance/i.test(stack) ||
      /Emitted 'error' event on TuyaDevice/i.test(stack)
    );
  };

  process.on('uncaughtException', (err) => {
    if (isSocketError(err)) {
      // Silent ignore - harmlos. Device ist offline oder WLAN flackert.
      return;
    }
    // Echte unerwartete Errors NUR loggen, NICHT re-throwen.
    // Re-throw waere "korrekt" macht aber den Adapter unbenutzbar.
    try {
      console.error('[fid-smartlife] uncaughtException (non-socket):', err);
    } catch (e) { /* ignore */ }
  });

  process.on('unhandledRejection', (reason) => {
    if (isSocketError(reason)) return;
    try {
      console.error('[fid-smartlife] unhandledRejection (non-socket):', reason);
    } catch (e) { /* ignore */ }
  });
}

const utils      = require('@iobroker/adapter-core');
const TuyaCloud  = require('./lib/tuyaCloud');
const TuyaPulsar = require('./lib/tuyaPulsar').TuyaPulsar;
const tuyaLocal  = require('./lib/tuyaLocal');
const tuyaLocal35 = require('./lib/tuyaLocal35');  // v3.4 + v3.5 via tuyapi-lib
const lanDiscovery = require('./lib/lanDiscovery');
const sm         = require('./lib/specMapper');
const enhanced   = require('./lib/enhanced');

class FiducerionSmartlife extends utils.Adapter {

  constructor(options) {
    super(Object.assign({}, options, { name: 'fid-smartlife' }));
    this.on('ready',         this.onReady.bind(this));
    this.on('unload',        this.onUnload.bind(this));
    this.on('stateChange',   this.onStateChange.bind(this));
    this.on('message',       this.onMessage.bind(this));

    /** @type {TuyaCloud|null} */
    this.cloud = null;
    this.pulsar = null;
    this.pulsarStats = { msgRx: 0, decryptOk: 0, decryptFail: 0, lastMsgTs: 0 };

    /** TuyaAppCloud (Smart-Life Email/Password Login) - v0.8.6 */
    this.appCloud = null;
    this.appCloudTimer = null;
    this._appCloudTestRunning = false;

    /** Map deviceId -> { name, defs, alias, dpMeta, canonToReal, noCloudStatusPoll, local, raw } */
    this.devices = new Map();

    /** Set zur Erkennung von Writes die wir selber gerade schreiben (Optimistic ACK)
     *  damit wir nicht in einer Schleife enden. */
    this.ignoreNextChange = new Set();

    /** Debounce-Timer pro <deviceId>::<codeCanon> */
    this.writeTimers = Object.create(null);

    /** LAN-Discovery: cached records by deviceId, falls Gerät vor Cloud-Discovery announced */
    this.discoveryCache = Object.create(null);

    /** LAN-Discovery-Listener-Handle */
    this.lanListener = null;

    /** Lifecycle */
    this.pollTimer    = null;
    this.rediscoverTimer = null;
    this.shuttingDown = false;
  }

  /**
   * Liefert die richtige Local-Implementation basierend auf der Protocol-Version.
   * - v3.3 -> eigene Implementation (lib/tuyaLocal.js)
   * - v3.4 / v3.5 -> via tuyapi-Library (lib/tuyaLocal35.js)
   */
  _getLocalImpl(version) {
    const v = String(version || '3.3');
    if (v === '3.4' || v === '3.5') return tuyaLocal35;
    return tuyaLocal;
  }

  /**
   * True wenn die Protocol-Version lokal unterstuetzt wird.
   * Aktuell: 3.3 (eigen) + 3.4 + 3.5 (via tuyapi).
   */
  _isLocalVersionSupported(version) {
    const v = String(version || '3.3');
    return v === '3.3' || v === '3.4' || v === '3.5';
  }

  async onReady() {
    // ==== Self-Check: sind die kritischen Files alle da? ====
    // Wenn ein OS-Update / apt-Hook npm-prune ausgeloest hat, sind Teile von
    // node_modules weg. Hart abbrechen statt sich durchwurschteln.
    if (!this._selfCheck()) {
      this.terminate ? this.terminate('Self-check failed', 13) : process.exit(13);
      return;
    }

    try {
      await this.setStateAsync('info.connection', { val: false, ack: true });

      // Cloud-Quota-Status-State (v0.6.5)
      await this.setObjectNotExistsAsync('info.cloudQuotaPaused', {
        type: 'state',
        common: { name: 'Cloud-Quota erschoepft - Schreiben pausiert', type: 'boolean', role: 'indicator', read: true, write: false, def: false },
        native: {}
      });
      await this.setStateAsync('info.cloudQuotaPaused', { val: false, ack: true }).catch(() => {});

      const cfg = this.config || {};
      if (!cfg.clientId || !cfg.clientSecret) {
        this.log.error('Bitte Tuya Access ID + Access Secret in der Adapter-Konfiguration eintragen.');
        return;
      }

      // Falls clientSecret aus einer alten io-package.json mit encryptedNative verschluesselt
      // ist, hier nachtraeglich entschluesseln. Bei neueren io-packages (ohne encryptedNative)
      // ist this.config.clientSecret bereits Klartext.
      let secret = String(cfg.clientSecret || '');
      try {
        if (typeof this.getEncryptedConfig === 'function' && secret.length > 0) {
          const decrypted = await this.getEncryptedConfig('clientSecret', secret).catch(() => null);
          if (decrypted && decrypted !== secret && decrypted.length > 0) {
            this.log.debug('clientSecret war verschluesselt, entschluesselt fuer Cloud-Auth');
            secret = decrypted;
          }
        }
      } catch (e) { /* fallthrough mit ggf. verschluesseltem secret - Cloud-Call wird dann scheitern und der User sieht den Fehler */ }

      // No-poll Liste parsen
      this.noPollSet = new Set(
        String(cfg.noCloudStatusPollIds || '').split(',')
          .map(s => s.trim()).filter(Boolean)
      );

      this.cloud = new TuyaCloud({
        clientId: cfg.clientId,
        clientSecret: secret,
        region: cfg.region || 'eu',
        requestTimeoutMs: Number(cfg.requestTimeoutMs) || 12000,
        logger: (lvl, msg) => this.log[lvl] && this.log[lvl]('[cloud] ' + msg)
      });

      // Commands-States subscriben (Buttons im Admin)
      this.subscribeStates('commands.*');
      // Alle Geraete-States subscriben (Schreibbefehle vom User)
      // Wir filtern in onStateChange selbst nach writable + nicht-leading-underscore.
      this.subscribeStates('*');

      // LAN-Discovery starten (laeuft im Hintergrund parallel zur Cloud-Discovery)
      if (cfg.enableLanDiscovery !== false) {
        this.lanListener = lanDiscovery.start({
          onRecord: (rec) => this.onLanDiscoveryRecord(rec).catch(e => this.log.debug('LAN-record: ' + e.message)),
          logger: (lvl, msg) => this.log[lvl] && this.log[lvl]('[lan] ' + msg)
        });
      }

      this.log.info('Starte Discovery ...');
      await this.discoverAll();

      // Lokale IPs aus iobroker.tuya importieren falls Adapter parallel laeuft
      if (cfg.importLocalFromTuyaAdapter !== false) {
        await this.importLocalFromTuyaAdapter();
      }

      await this.setStateAsync('info.connection', { val: true, ack: true });
      this.log.info('Initial-Setup fertig: ' + this.devices.size + ' Geraete');

      // Migration v0.6.4: alle Devices mit privater IP + localKey + version 3.3
      // einmalig auf noLocalConnection=false stellen, damit lokales DP_QUERY
      // versucht wird. User-Override bleibt erhalten falls Migration schon lief.
      await this.migrateNoLocalConnectionV064();

      // Migration v0.6.5: Reset aller Auto-Failover-Markierungen.
      // Hintergrund: in v0.6.4 hat die Auto-Failover-Logik ("3x lokal fail ->
      // noLocalConnection=true") bei einem temporaeren Netz-Ruckler reihenweise
      // Devices auf cloud-only umgestellt. Das hat die Tuya-Cloud-Quota
      // erschoepft (98 Devices -> Burst). v0.6.5 setzt einmalig alle wieder
      // auf false und nutzt den neuen smarten Failover (temporaer, 5min).
      await this.migrateResetFailoverV065();

      // Migration v0.9.2: Reset aller heute akkumulierter Cloud-Only-Flags
      // wegen zu aggressivem 3x-Schwellwert. v0.9.2 hat jetzt 10x + Auto-Recovery.
      await this.migrateResetFailoverV092();

      // Periodisches Polling
      const pollMs = Math.max(10, Number(cfg.pollIntervalSec) || 60) * 1000;
      this.pollTimer = this.setInterval(() => this.pollAll().catch(e => this.log.warn('Poll-Cycle: ' + e.message)), pollMs);

      // Periodische Rediscovery
      const rdMin = Number(cfg.rediscoverIntervalMin);
      if (rdMin && rdMin > 0) {
        this.rediscoverTimer = this.setInterval(() => {
          this.log.info('Periodische Rediscovery ...');
          this.discoverAll().catch(e => this.log.warn('Rediscover: ' + e.message));
        }, rdMin * 60 * 1000);
      }

      // ---- Pulsar/MQTT Push-Subscriber (v0.7.0) ----
      // Wenn aktiviert: holt Status-Updates per MQTT-Push statt per Cloud-Polling.
      // Massiv weniger Cloud-Quota - besonders fuer Battery/Sub-Devices wichtig.
      // Default false bis User in Tuya IoT Platform Message Service abonniert hat.
      if (cfg.enablePulsar) {
        await this.startPulsar(secret).catch(e =>
          this.log.warn('Pulsar-Start fehlgeschlagen (Adapter laeuft normal weiter): ' + (e.message || e))
        );
      }

      // ---- App-Cloud (Smart-Life Email/Password) (v0.8.6) ----
      // Wenn aktiviert: nutzt Smart-Life-App-API als zusaetzliche Status-Quelle.
      // Hauptzweck heute: Online-Check vor Cloud-Write damit wir keine
      // Cloud-Quota fuer offline Devices verschwenden.
      if (cfg.appCloudEnabled && cfg.appCloudEmail && cfg.appCloudPassword) {
        await this.startAppCloud().catch(e =>
          this.log.warn('App-Cloud-Start fehlgeschlagen (Adapter laeuft normal weiter): ' + (e.message || e))
        );
      }

    } catch (e) {
      this.log.error('onReady fehlgeschlagen: ' + (e && e.stack || e));
      await this.setStateAsync('info.connection', { val: false, ack: true }).catch(() => {});
      await this.setStateAsync('info.lastError',  { val: String(e && e.message || e), ack: true }).catch(() => {});
    }
  }

  /**
   * Prueft ob die fuer den Adapter kritischen Files existieren. Wenn nicht
   * (z.B. nach npm-prune oder defektem Update), Watchdog triggern und Fehler
   * loggen. Return true = OK, false = Fail.
   */
  _selfCheck() {
    try {
      const fs = require('fs');
      const path = require('path');
      // Nur eigene Source-Files pruefen. node_modules-Check ist eine schlechte Idee
      // weil:
      //  - require() weiter unten wuerde es eh werfen
      //  - fehlende node_modules sind ein anderes Problem (apt-hook npm prune)
      //    das durch Reinstall geheilt werden muss, nicht durch Watchdog-Loop
      const required = [
        'lib/tuyaCloud.js',
        'lib/specMapper.js',
        'lib/tuyaLocal.js',
        'lib/lanDiscovery.js'
      ];
      const missing = [];
      for (const r of required) {
        const full = path.join(__dirname, r);
        if (!fs.existsSync(full)) missing.push(r);
      }

      if (missing.length) {
        this.log.error('======================================================');
        this.log.error('SELF-CHECK FEHLGESCHLAGEN: ' + missing.length + ' kritische Source-Files fehlen.');
        this.log.error('Manueller Eingriff noetig:');
        this.log.error('  bash /opt/iobroker/.fid-smartlife-watchdog.sh');
        this.log.error('  oder Neu-Installation: bash install-smartlife.sh /tmp/fid-sl.zip');
        this.log.error('Fehlt:');
        for (const m of missing) this.log.error('  - ' + m);
        this.log.error('======================================================');
        // WICHTIG: NICHT automatisch den Watchdog triggern. Das hat in v0.5.1
        // einen Restart-Loop verursacht der die ioredis-Connection von
        // js-controller umgebracht hat ("DB closed" bei allen Adaptern).
        return false;
      }
      return true;
    } catch (e) {
      this.log.warn('Self-Check selbst fehlgeschlagen: ' + (e && e.message || e));
      return true;
    }
  }

  async onUnload(cb) {
    try {
      this.shuttingDown = true;
      if (this.pollTimer) { this.clearInterval(this.pollTimer); this.pollTimer = null; }
      if (this.rediscoverTimer) { this.clearInterval(this.rediscoverTimer); this.rediscoverTimer = null; }
      if (this.lanListener) { try { this.lanListener.stop(); } catch (e) {} this.lanListener = null; }
      if (this.pulsar) { try { this.pulsar.stop(); } catch (e) {} this.pulsar = null; }
      if (this.appCloudTimer) { this.clearInterval(this.appCloudTimer); this.appCloudTimer = null; }
      // Alle ausstehenden Debounce-Timer clearen
      for (const k of Object.keys(this.writeTimers)) {
        try { clearTimeout(this.writeTimers[k]); } catch (e) {}
      }
      await this.setStateAsync('info.connection', { val: false, ack: true }).catch(() => {});
      cb();
    } catch (e) { cb(); }
  }

  // -------------- Discovery & State-Anlage --------------

  async discoverAll() {
    try {
      const list = await this.cloud.listDevices();
      this.log.info('Discovery: ' + list.length + ' Geraete von Tuya geliefert');

      for (const raw of list) {
        if (this.shuttingDown) return;
        try { await this.buildDevice(raw); }
        catch (e) { this.log.warn('Device-Setup ' + raw.id + ' fehlgeschlagen: ' + e.message); }
      }

      await this.setStateAsync('info.deviceCount',   { val: this.devices.size, ack: true });
      await this.setStateAsync('info.lastDiscovery', { val: new Date().toISOString(), ack: true });

      // Legacy-Cleanup: alte Code-Name-States + _local.* Channel-Objekte aus
      // v0.3.x/v0.4.0 wegraeumen die durch den tuya-Style-Refactor obsolet sind
      try {
        const removed = await this._cleanupLegacyStates();
        if (removed > 0) this.log.info('Legacy-Cleanup: ' + removed + ' obsolete States/Channels geloescht');
      } catch (e) {
        this.log.warn('Legacy-Cleanup: ' + (e && e.message || e));
      }

      // Initial-Poll fuer alle frisch geladenen Geraete (parallel um nicht ewig zu brauchen)
      this.log.info('Initial-Status holen ...');
      const initIds = Array.from(this.devices.keys());
      const initParallel = Math.max(1, Math.min(5, Number(this.config.maxParallelPolls) || 3));
      let n = 0, i = 0;
      const initWorkers = Array.from({ length: initParallel }, () => (async () => {
        while (i < initIds.length && !this.shuttingDown) {
          const idx = i++;
          try { await this.pollDevice(initIds[idx]); n++; }
          catch (e) { /* schon geloggt in pollDevice */ }
        }
      })());
      await Promise.all(initWorkers);
      this.log.info('Initial-Status: ' + n + '/' + this.devices.size + ' OK');

    } catch (e) {
      this.log.error('Discovery: ' + e.message);
      await this.setStateAsync('info.lastError', { val: 'discovery: ' + e.message, ack: true }).catch(() => {});
      throw e;
    }
  }

  /**
  /**
   * Loescht obsolete States aus alten Adapter-Versionen.
   *
   * SAFE-MODE (v0.7.4): loescht NUR die alten _local.* Channels aus v0.3/0.4.
   * Alles andere wird in Ruhe gelassen - damit verlieren wir keine Historie und
   * keine fehlerhaft-aus-Cloud-gefilterten Devices.
   *
   * Frueher hat das auch andere "obsolete" Top-Level-States geloescht, aber
   * das hat (a) bei jedem Start gefeuert (Markierungs-States waren nicht in
   * keep_set, daher running ueber Tage Hunderte Operationen) und konnte (b)
   * Devices entstellen wenn die Schema-Definition sich aenderte.
   */
  async _cleanupLegacyStates() {
    const all = await this.getAdapterObjectsAsync().catch(() => null);
    if (!all) return 0;

    let removed = 0;
    for (const [did, dev] of this.devices) {
      const prefix = this.namespace + '.' + did + '.';
      for (const fullId of Object.keys(all)) {
        if (!fullId.startsWith(prefix)) continue;
        const sub = fullId.slice(prefix.length);
        if (!sub) continue;
        const top = sub.split('.')[0];
        // Nur ganz alte _local.* Channels aus v0.3/0.4 wegloeschen
        if (top === '_local') {
          try {
            await this.delObjectAsync(fullId);
            removed++;
          } catch (e) {}
        }
      }
    }
    return removed;
  }

  async buildDevice(raw) {
    const id   = raw.id;
    const name = raw.name || id;
    if (!id) return;

    // Device-Objekt anlegen
    await this.setObjectNotExistsAsync(id, {
      type: 'device',
      common: { name: name },
      native: {}
    });
    await this.extendObjectAsync(id, { common: { name: name } });

    // Spec laden + Raw-Schema aus dem listDevices-Eintrag verwenden +
    // productKey-Fallback. mergeSchemaSources prioritisiert raw.schema vor spec
    // vor SchemaDB.
    const spec = await this.cloud.getSpecification(id);
    const rawSchema = Array.isArray(raw && raw.schema) ? raw.schema : null;
    const productKey = (raw && (raw.productKey || raw.product_id || raw.productId)) || null;
    // Logger-Adapter: this.log ist Object {info, warn, debug, error}, kein bindbares fn
    const _log = (level, msg) => {
      try { (this.log[level] || this.log.debug || (() => {}))(msg); } catch (e) {}
    };
    const defs = sm.mergeSchemaSources(spec, rawSchema, productKey, _log);
    if (this.config && this.config.verboseSchema) {
      this.log.info('Schema ' + name + ' (' + id + '): spec=' + ((spec && (spec.functions||[]).length + (spec.status||[]).length) || 0)
        + ' raw=' + (rawSchema ? rawSchema.length : 0)
        + ' productKey=' + (productKey || '?')
        + ' -> ' + defs.length + ' DPs');
    }
    const defCanonSet = new Set();
    const dpMeta = Object.create(null);
    const canonToReal = Object.create(null);
    const dpIdToCanon = Object.create(null);  // '20' -> 'cur_voltage' fuer write-redirect

    for (const d of defs) {
      const c = sm.canon(d.codeReal || d.code || d.id);
      if (!c) continue;
      defCanonSet.add(c);
      canonToReal[c] = String(d.codeReal || d.code || d.id);
      dpMeta[c] = sm.extractMeta(d);
      if (dpMeta[c].dpId && String(dpMeta[c].dpId) !== c) {
        dpIdToCanon[String(dpMeta[c].dpId)] = c;
      }
    }

    const alias = sm.computeAliases(defCanonSet, dpMeta);

    // Meta-States anlegen
    await this.ensureState(id + '._name', {
      name: 'Name', type: 'string', role: 'text', read: true, write: false
    });
    await this.ensureState(id + '.online', {
      type: 'boolean',
      role: 'indicator.reachable',
      read: true,
      write: false,
      def: false,
      desc: 'Geraet ist lokal erreichbar (mit Hysterese)'
    });
    await this.ensureState(id + '._noCloudStatusPoll', {
      name: 'Disable regular cloud status polling',
      type: 'boolean', role: 'switch', read: true, write: true
    });
    // v0.10.7: Pro-Device-Schalter um Cloud-Write-Fallback zu deaktivieren.
    // Wenn true: bei Local-Fail wird KEIN Cloud-Versuch unternommen (spart
    // Cloud-Quota fuer Devices die wirklich auf Cloud angewiesen sind).
    // Sinnvoll fuer Devices die zuverlaessig lokal erreichbar sein SOLLTEN
    // (z.B. Rollladen, Schalter im Haus). Default: false (= alte Behaviour).
    await this.ensureState(id + '._noCloudWrite', {
      name: 'Disable cloud-write fallback when local fails',
      type: 'boolean', role: 'switch', read: true, write: true,
      desc: 'Wenn true: bei lokalen Schreibfehlern kein Cloud-Fallback. Spart Quota.'
    });

    await this.setStateAsync(id + '._name', { val: name, ack: true });
    // online wird NICHT mehr aus raw.online (Cloud-Online) gesetzt - das passiert
    // jetzt im pollAll basierend auf lokaler Erreichbarkeit mit Hysterese.

    // Default-Wert fuer _noCloudStatusPoll setzen falls in der noPoll-Liste
    if (this.noPollSet.has(id)) {
      const cur = await this.getStateAsync(id + '._noCloudStatusPoll');
      if (!cur || cur.val !== true) {
        await this.setStateAsync(id + '._noCloudStatusPoll', { val: true, ack: true });
      }
    }

    // DP-States anlegen
    for (const c of defCanonSet) {
      await this.ensureCodeState(id, name, c, dpMeta[c]);
    }

    // Alias-States anlegen
    for (const aliasKey of Object.keys(alias)) {
      if (aliasKey.startsWith('_')) continue;  // _modeType etc sind Meta
      await this.ensureAliasState(id, name, aliasKey, alias);
    }

    // Local-Channel + States anlegen
    await this.ensureLocalStates(id, name, raw);

    // Aktuelle Local-Werte aus States/raw lesen
    const local = await this.readLocalStates(id, raw);

    // Falls LAN-Discovery vor Cloud-Setup einen Eintrag geliefert hat -> IP+Version uebernehmen
    const cached = this.discoveryCache[id];
    if (cached) {
      if (cached.ip && local.ip !== cached.ip) {
        local.ip = cached.ip;
        await this.setStateAsync(id + '.ip', { val: cached.ip, ack: true });
      }
      if (cached.version && local.version !== cached.version) {
        local.version = cached.version;
        await this.setStateAsync(id + '.localVersion', { val: cached.version, ack: true });
      }
      await this.setStateAsync(id + '.localLastSeen', { val: new Date(cached.ts).toISOString(), ack: true });
      await this.setStateAsync(id + '.localSource',   { val: cached.source, ack: true });
    }

    // In Registry speichern
    const cur = await this.getStateAsync(id + '._noCloudStatusPoll');
    this.devices.set(id, {
      id: id,
      name: name,
      raw: raw,
      defs: defs,
      defCanonSet: defCanonSet,
      dpMeta: dpMeta,
      canonToReal: canonToReal,
      dpIdToCanon: dpIdToCanon,
      alias: alias,
      noCloudStatusPoll: !!(cur && cur.val),
      local: local
    });
  }

  // -------------- Local-Channel + States --------------

  /**
   * Tuya-Style: keine _local.* Channel-States mehr. Die LAN-Settings (key,
   * version, port, source, preferLocal) leben jetzt direkt unter dem Device:
   *   <dev>.ip                  IP-Adresse
   *   <dev>.noLocalConnection   Top-Level (= !preferLocal), writable
   *   <dev>.localKey            (writable, password-like)
   *   <dev>.localVersion        (writable, default '3.3')
   *   <dev>.localPort           (writable, default 6668)
   *   <dev>.localSource         (read-only, woher die IP kommt)
   *   <dev>.localLastResult     (read-only)
   *   <dev>.localLastSeen       (read-only)
   * 'online' wird woanders schon angelegt.
   */
  async ensureLocalStates(deviceId, deviceName, raw) {
    const def = (id, common, value) => this.setObjectNotExistsAsync(deviceId + '.' + id, {
      type: 'state',
      common: Object.assign({ name: id, read: true, write: false }, common),
      native: {}
    }).then(async () => {
      // value ist Default - nur schreiben wenn State noch keinen Wert hat.
      // Vorher wurde IMMER geschrieben -> localVersion=3.5 wurde bei jedem
      // Adapter-Start auf 3.3 zurueckgesetzt!
      if (typeof value !== 'undefined') {
        const cur = await this.getStateAsync(deviceId + '.' + id).catch(() => null);
        if (!cur || cur.val === undefined || cur.val === null || cur.val === '') {
          await this.setStateAsync(deviceId + '.' + id, { val: value, ack: true });
        }
      }
    });

    await def('ip',                 { type: 'string',  role: 'info.ip',          write: true });
    await def('noLocalConnection',  { type: 'boolean', role: 'switch.enable',    write: true }, false);
    await def('localKey',           { type: 'string',  role: 'text',             write: true }, String(raw.local_key || ''));
    await def('localVersion',       { type: 'string',  role: 'text',             write: true }, '3.3');
    await def('localPort',          { type: 'number',  role: 'value',            write: true }, tuyaLocal.DEFAULT_PORT);
    await def('localSource',        { type: 'string',  role: 'text' });
    await def('localLastResult',    { type: 'string',  role: 'text' });
    await def('localLastSeen',      { type: 'string',  role: 'text' });
  }

  async readLocalStates(deviceId, raw) {
    const v = async (k, fallback) => {
      const s = await this.getStateAsync(deviceId + '.' + k).catch(() => null);
      return (s && s.val !== undefined && s.val !== null && s.val !== '') ? s.val : fallback;
    };
    // raw.ip aus Cloud listDevices Response als Fallback - oft hat Tuya da
    // schon die IP fuer online Devices.
    const rawIp = (raw && (raw.ip || raw.local_ip || raw.device_ip)) || '';
    const ip                = String(await v('ip', rawIp));
    const key               = String(await v('localKey', raw && raw.local_key || ''));
    const version           = String(await v('localVersion', '3.3'));
    const port              = Number(await v('localPort', tuyaLocal.DEFAULT_PORT));
    const noLocal           = !!(await v('noLocalConnection', false));
    const preferLocal       = !noLocal;
    const source            = String(await v('localSource', ''));
    // v0.10.7: pro-Device "kein Cloud-Write-Fallback" Schalter
    const noCloudWrite      = !!(await v('_noCloudWrite', false));

    // Falls ip aus raw kam aber State noch leer ist: setzen
    if (rawIp && tuyaLocal.isPrivateIp(rawIp)) {
      const curIp = await this.getStateAsync(deviceId + '.ip').catch(() => null);
      if (!curIp || !curIp.val) {
        await this.setStateAsync(deviceId + '.ip', { val: rawIp, ack: true }).catch(() => {});
      }
    }

    return { ip, key, version, port, preferLocal, source, noCloudWrite };
  }

  async ensureState(id, common) {
    const c = Object.assign({
      type: 'mixed', role: 'state', read: true, write: false
    }, common);
    await this.setObjectNotExistsAsync(id, { type: 'state', common: c, native: {} });
  }

  async ensureCodeState(deviceId, deviceName, codeCanon, meta) {
    if (!codeCanon) return;
    meta = meta || { type: 'string', writable: false };

    // Min/Max mit Scale rueckrechnen
    let min = meta.min, max = meta.max;
    if (meta.type === 'number' && typeof meta.scale === 'number' && meta.scale > 0) {
      const factor = Math.pow(10, meta.scale);
      if (typeof min === 'number') min /= factor;
      if (typeof max === 'number') max /= factor;
    }

    const usesDpsId = !!(meta.dpId && String(meta.dpId) !== codeCanon);
    const primaryPath = usesDpsId ? String(meta.dpId) : codeCanon;
    // tuya-Adapter-Konvention: common.name = code (englisch), chinese Name optional in description
    const displayName = meta.friendly || codeCanon;

    const common = {
      name: displayName,
      type: meta.type,
      role: sm.roleFor(codeCanon, meta.type),
      read: true,
      write: !!meta.writable
    };
    if (typeof meta.unit !== 'undefined') common.unit = meta.unit;
    if (meta.writable) {
      if (typeof min === 'number') common.min = min;
      if (typeof max === 'number') common.max = max;
    }
    if (Array.isArray(meta.enums) && meta.enums.length) {
      // tuya-Style: enum -> number mit common.states {idx: label}
      const st = {};
      meta.enums.forEach((val, idx) => { st[idx] = val; });
      common.states = st;
    }
    if (meta.isBitmap) {
      common.type = 'number';
      common.role = 'state';
    }
    if (meta.encoding === 'base64') {
      common.encoding = 'base64';
    }
    // Chinesischer Name als desc (informativ)
    if (meta.chineseName && meta.chineseName !== displayName) {
      common.desc = meta.chineseName;
    }

    const fullId = deviceId + '.' + primaryPath;
    const native = { code: codeCanon, dpId: meta.dpId || null };
    // setObjectAsync ueberschreibt das Object komplett - das ist hier gewuenscht
    // damit Schema-Updates (neue enums, geaenderte Typen, neue Namen) wirklich
    // im Objekt-Store landen. setObjectNotExistsAsync wuerde alte States stehen
    // lassen mit veralteten/chinesischen Namen oder zu kurzer enum-Liste.
    // Wenn schon existiert: native+common.history beibehalten damit User-Edits
    // (z.B. eigener Name, history-Einstellungen) nicht weggeraeumt werden.
    const existing = await this.getObjectAsync(fullId).catch(() => null);
    if (existing && existing.common) {
      // History/custom-Felder preserven
      if (existing.common.custom) common.custom = existing.common.custom;
    }
    await this.setObjectAsync(fullId, {
      type: 'state',
      common: common,
      native: Object.assign({}, (existing && existing.native) || {}, native)
    }).catch(() => {});

    // Bitmap-Unterstaates anlegen (tuya-Style: <primary>-0, <primary>-1, ...)
    if (meta.isBitmap && Array.isArray(meta.bitmapLabels)) {
      for (let i = 0; i < meta.bitmapLabels.length; i++) {
        const label = meta.bitmapLabels[i];
        const bitFullId = deviceId + '.' + primaryPath + '-' + i;
        const bitCommon = {
          name: codeCanon + (meta.chineseName ? ' ' + meta.chineseName : '') + ' ' + label,
          type: 'boolean', role: 'indicator',
          read: true, write: false
        };
        const bitNative = { bitmapParent: primaryPath, bitIndex: i, label: label };
        const exB = await this.getObjectAsync(bitFullId).catch(() => null);
        if (exB && exB.common && exB.common.custom) bitCommon.custom = exB.common.custom;
        await this.setObjectAsync(bitFullId, {
          type: 'state', common: bitCommon, native: bitNative
        }).catch(() => {});
      }
    }

    // Enhanced-postprocessor-derived states (color-rgb, phase-power etc.)
    const enh = enhanced.getEnhanced(meta.dpId, codeCanon);
    if (enh) {
      for (const def of enh) {
        const derivedKey = primaryPath + def.postfix;
        const derivedFullId = deviceId + '.' + derivedKey;
        const derivedCommon = Object.assign({ name: displayName + ' ' + def.postfix }, def.common);
        const derivedNative = { derivedFrom: primaryPath, code: codeCanon };
        const exD = await this.getObjectAsync(derivedFullId).catch(() => null);
        if (exD && exD.common && exD.common.custom) derivedCommon.custom = exD.common.custom;
        await this.setObjectAsync(derivedFullId, {
          type: 'state', common: derivedCommon, native: derivedNative
        }).catch(() => {});
      }
    }
  }

  async ensureAliasState(deviceId, deviceName, aliasKey, aliasMeta) {
    // Spezielle Eintraege in alias-Objekt, keine echten States
    if (aliasKey.startsWith('_')) return;

    const ALIAS_COMMONS = {
      on:           { type: 'boolean', role: 'switch', read: true, write: true },
      brightness:   { type: 'number',  role: 'level.dimmer', min: 0, max: 100, read: true, write: true },
      color_temp_k: { type: 'number',  role: 'level.color.temperature', read: true, write: true },
      color_rgb:    { type: 'string',  role: 'level.color.rgb', read: true, write: true },
      mode:         { type: 'string',  role: 'state', read: true, write: true },
      temperature:  { type: 'number',  role: 'value.temperature', read: true, write: false },
      humidity:     { type: 'number',  role: 'value.humidity', read: true, write: false },
      battery:      { type: 'number',  role: 'value.battery', read: true, write: false }
    };
    const c = Object.assign({}, ALIAS_COMMONS[aliasKey]);
    if (!c.type) return;

    // mode kann sowohl string (work_mode='colour') als auch number (mode=0/1/2) sein
    if (aliasKey === 'mode' && aliasMeta && aliasMeta._modeType === 'number') {
      c.type = 'number';
    }

    await this.setObjectNotExistsAsync(deviceId + '.' + aliasKey, {
      type: 'state',
      common: Object.assign({ name: deviceName + ' - ' + aliasKey }, c),
      native: {}
    });
  }

  // -------------- Polling --------------

  /**
   * Einmalige Migration v0.6.4: setzt fuer alle bekannten Devices mit privater IP
   * und localKey + version 3.3 den State noLocalConnection auf false. Lokales
   * DP_QUERY (cmd 0x0a) wird dann aktiv. Geraete die NICHT lokal erreichbar sind,
   * werden vom Adapter selbst per Auto-Failover-Logik (3x fail) zurueck auf true
   * gesetzt.
   * Wird nur einmal pro Adapter-Lifetime ausgefuehrt - markiert via Info-State.
   */
  async migrateNoLocalConnectionV064() {
    try {
      const marker = await this.getStateAsync('info.localMigrationV064').catch(() => null);
      if (marker && marker.val === true) {
        this.log.debug('Migration V064 schon erledigt - skip');
        return;
      }
      await this.setObjectNotExistsAsync('info.localMigrationV064', {
        type: 'state',
        common: { name: 'Migration v0.6.4 noLocalConnection-Reset done', type: 'boolean', role: 'indicator', read: true, write: false, def: false },
        native: {}
      });

      let migrated = 0;
      for (const id of this.devices.keys()) {
        const dev = this.devices.get(id);
        const lc = dev.local || {};
        if (!lc.ip || !lc.key) continue;
        if (!tuyaLocal.isPrivateIp(lc.ip)) continue;
        if (lc.version !== '3.3' && lc.version !== '3.4' && lc.version !== '3.5') continue;
        // Nur Devices wo noLocalConnection aktuell true ist
        const cur = await this.getStateAsync(id + '.noLocalConnection').catch(() => null);
        if (cur && cur.val === true) {
          await this.setStateAsync(id + '.noLocalConnection', { val: false, ack: true });
          dev.local.preferLocal = true;
          migrated++;
        }
      }
      await this.setStateAsync('info.localMigrationV064', { val: true, ack: true });
      this.log.info('Migration V064: ' + migrated + ' Geraete auf lokales Polling umgestellt (noLocalConnection: true -> false)');
    } catch (e) {
      this.log.warn('migrateNoLocalConnectionV064: ' + e.message);
    }
  }

  // -------------- Pulsar / MQTT Push-Subscriber (v0.7.0) --------------

  /**
   * Startet den TuyaPulsar Subscriber. Holt Auth-Config vom Tuya open-hub,
   * verbindet sich per MQTT und subscribed auf das device-event topic.
   * Bei jeder Status-Aenderung kommt eine push-Message - wir muessen nicht mehr
   * pollen.
   */
  async startPulsar(secret) {
    if (this.pulsar) {
      this.log.warn('Pulsar laeuft schon - skip start');
      return;
    }
    // Quota-State + Status-States anlegen
    await this.setObjectNotExistsAsync('info.pulsarConnected', {
      type: 'state',
      common: { name: 'Pulsar MQTT-Push aktiv', type: 'boolean', role: 'indicator.connected', read: true, write: false, def: false },
      native: {}
    });
    await this.setObjectNotExistsAsync('info.pulsarMessages', {
      type: 'state',
      common: { name: 'Pulsar empfangene Messages (Counter)', type: 'number', role: 'value', read: true, write: false, def: 0 },
      native: {}
    });
    await this.setObjectNotExistsAsync('info.pulsarRawRx', {
      type: 'state',
      common: { name: 'Pulsar RAW MQTT Messages (vor Decrypt/Parse) - Diagnose', type: 'number', role: 'value', read: true, write: false, def: 0 },
      native: {}
    });
    await this.setObjectNotExistsAsync('info.pulsarLastMsg', {
      type: 'state',
      common: { name: 'Pulsar letzte Message Zeitstempel', type: 'string', role: 'value.time', read: true, write: false, def: '' },
      native: {}
    });

    this.pulsar = new TuyaPulsar({
      clientId:     this.config.clientId,
      clientSecret: secret,
      region:       this.config.region || 'eu',
      cloudRef:     this.cloud,
      getCloudToken: async () => {
        if (this.cloud && typeof this.cloud.ensureToken === 'function') {
          return await this.cloud.ensureToken();
        }
        throw new Error('cloud not ready');
      },
      logger: (lvl, msg) => this.log[lvl] && this.log[lvl]('[pulsar] ' + msg)
    });
    this.pulsar.on('connected', async () => {
      await this.setStateAsync('info.pulsarConnected', { val: true, ack: true }).catch(()=>{});
      this.log.info('Pulsar MQTT verbunden - Device-Status-Updates kommen jetzt per Push');
    });
    this.pulsar.on('disconnected', async () => {
      await this.setStateAsync('info.pulsarConnected', { val: false, ack: true }).catch(()=>{});
    });
    this.pulsar.on('message', (parsed) => this.onPulsarMessage(parsed).catch(e =>
      this.log.warn('Pulsar message handler: ' + (e.message || e))
    ));
    this.pulsar.on('rawMessage', (topic, payload) => {
      this.pulsarStats.rawRx = (this.pulsarStats.rawRx || 0) + 1;
      this.setStateAsync('info.pulsarRawRx', { val: this.pulsarStats.rawRx, ack: true }).catch(() => {});
      this.log.info('[pulsar] RAW Message empfangen #' + this.pulsarStats.rawRx
        + ' topic=' + topic + ' size=' + payload.length + ' bytes (vor Parse/Decrypt)');
    });

    await this.pulsar.start();
  }

  /**
   * Verarbeitet eine entschluesselte Pulsar-Message.
   * Format:
   *   {dataId, devId, productKey, status: [{code, value, t, ...}]}
   * oder bei einigen events:
   *   {bizCode: 'online'|'offline', bizData: {...}, devId: '...'}
   */
  async onPulsarMessage(parsed) {
    this.pulsarStats.msgRx++;
    this.pulsarStats.lastMsgTs = Date.now();
    await this.setStateAsync('info.pulsarMessages', { val: this.pulsarStats.msgRx, ack: true }).catch(()=>{});
    await this.setStateAsync('info.pulsarLastMsg',  { val: new Date().toISOString(), ack: true }).catch(()=>{});

    const devId = parsed.devId || parsed.deviceId || (parsed.bizData && parsed.bizData.devId);
    if (!devId) {
      this.log.debug('Pulsar msg ohne devId, keys=' + Object.keys(parsed).join(','));
      return;
    }
    const dev = this.devices.get(devId);
    if (!dev) {
      this.log.debug('Pulsar msg fuer unbekanntes Device ' + devId);
      return;
    }

    // Status-Report verarbeiten
    if (Array.isArray(parsed.status) && parsed.status.length > 0) {
      // Wir konvertieren das Pulsar-Format ({code, value}) in das Format das
      // unsere mirrorStatus erwartet ({code, value})
      const statusArr = parsed.status.map(s => ({ code: s.code, value: s.value }));
      try {
        await this.mirrorStatus(dev, statusArr);
        this.log.debug('Pulsar Status ' + dev.name + ': ' + statusArr.length + ' DPs aktualisiert');
      } catch (e) {
        this.log.debug('mirrorStatus from pulsar failed: ' + e.message);
      }
    }

    // Online/Offline-Events
    if (parsed.bizCode === 'online') {
      await this.setStateAsync(devId + '.online', { val: true, ack: true }).catch(()=>{});
      this.log.debug('Pulsar: ' + dev.name + ' online');
    } else if (parsed.bizCode === 'offline') {
      await this.setStateAsync(devId + '.online', { val: false, ack: true }).catch(()=>{});
      this.log.debug('Pulsar: ' + dev.name + ' offline');
    }
  }

  /**
   * Startet die App-Cloud-Anbindung (v0.8.6).
   * - Laedt persistierte Session aus _appCloudSession state falls vorhanden
   * - Fuehrt initialen fetchAll() durch
   * - Startet periodischen Refresh
   * - Persistiert sid nach erfolgreichem Login
   */
  async startAppCloud() {
    const cfg = this.config;
    const { TuyaAppCloud } = require('./lib/tuyaAppCloud');

    // Persistierte Session laden
    let savedSid = null, savedTs = 0;
    try {
      const st = await this.getStateAsync('_appCloud.session').catch(() => null);
      if (st && st.val && typeof st.val === 'string') {
        const obj = JSON.parse(st.val);
        if (obj && obj.sid && obj.lastLoginTs) {
          savedSid = obj.sid;
          savedTs  = obj.lastLoginTs;
          const ageH = Math.round((Date.now() - savedTs) / 3600000);
          this.log.info('App-Cloud: persistierte Session geladen (Alter: ' + ageH + 'h)');
        }
      }
    } catch (e) { /* ignore */ }

    this.appCloud = new TuyaAppCloud({
      email:    cfg.appCloudEmail,
      password: cfg.appCloudPassword,
      region:   cfg.appCloudRegion || 'EU',
      sid:      savedSid,
      lastLoginTs: savedTs,
      logger:   (lvl, msg) => this.log[lvl] && this.log[lvl](msg)
    });

    // Initial fetch
    try {
      const t0 = Date.now();
      await this.appCloud.fetchAll();
      const stats = this.appCloud.getCachedStats();
      this.log.info('App-Cloud: Initial-Fetch OK (' + (Date.now() - t0) + 'ms): ' +
                    stats.total + ' Devices, ' + stats.online + ' online, ' + stats.offline + ' offline');
      await this._appCloudPersistSession();
      await this._appCloudUpdateInfoStates(stats);
      // v0.8.7: Auch direkt die dps spiegeln (App-Cloud als Status-Quelle)
      await this._appCloudMirrorAllDps();
    } catch (e) {
      this.log.warn('App-Cloud Initial-Fetch fehlgeschlagen: ' + (e.message || e));
      const msg = String(e.message || e).toLowerCase();
      if (msg.includes('auth') || msg.includes('login') || msg.includes('password')) {
        await this.setStateAsync('_appCloud.session', { val: '', ack: true }).catch(()=>{});
      }
      return;
    }

    // Periodischer Refresh
    const refreshSec = Math.max(60, Number(cfg.appCloudRefreshSec) || 300);
    this.appCloudTimer = this.setInterval(async () => {
      try {
        await this.appCloud.fetchAll();
        const stats = this.appCloud.getCachedStats();
        await this._appCloudUpdateInfoStates(stats);
        // v0.8.7: Auch direkt die dps spiegeln - bei aktivem app-Modus die einzige Status-Quelle
        await this._appCloudMirrorAllDps();
        this.log.debug('App-Cloud Refresh: ' + stats.online + '/' + stats.total + ' online, dps gespiegelt');
      } catch (e) {
        this.log.warn('App-Cloud Refresh: ' + (e.message || e));
      }
    }, refreshSec * 1000);
    this.log.info('App-Cloud aktiv (Refresh alle ' + refreshSec + 's). Online-Check vor Cloud-Write ist eingeschaltet.');
  }

  /**
   * v0.8.7: spiegelt die aktuellen dps-Werte aus der App-Cloud in unsere States.
   * Das ist quasi gratis (App-Cloud-Fetch war eh schon gemacht) und ersetzt
   * fuer online-Devices den Cloud-Poll komplett.
   *
   * Wird nach jedem App-Cloud-Fetch aufgerufen.
   */
  async _appCloudMirrorAllDps() {
    if (!this.appCloud || !this.appCloud._lastFetchDevices) return;
    let mirrored = 0, skippedOffline = 0, skippedUnknown = 0, kept = 0;
    const now = Date.now();
    for (const [did, devData] of this.appCloud._lastFetchDevices) {
      const dev = this.devices.get(did);
      if (!dev) { skippedUnknown++; continue; }
      const isOnline = this.appCloud.isDeviceOnline(devData);
      // online-State pflegen - aber: wenn lokal in den letzten 90s gesehen (Hysterese
      // wegen Auto-Failover etc), nicht ueberschreiben mit appCloud-offline.
      // _lastLocalOk wird in pollDevice() gesetzt bei localOk-Erfolg.
      const lastLocalOk = dev._lastLocalOk || 0;
      const recentLocalOk = (now - lastLocalOk) < 90 * 1000;
      if (isOnline) {
        await this.setStateAsync(did + '.online', { val: true, ack: true }).catch(() => {});
      } else if (recentLocalOk) {
        // App-Cloud meint offline aber wir hatten gerade lokal Erfolg
        // -> online lassen (lokal ueberstimmt)
        kept++;
      } else {
        await this.setStateAsync(did + '.online', { val: false, ack: true }).catch(() => {});
      }
      if (!isOnline) { skippedOffline++; continue; }
      if (!devData.dps || typeof devData.dps !== 'object') continue;
      try {
        await this.mirrorStatusDps(dev, devData.dps);
        mirrored++;
      } catch (e) {
        this.log.debug('App-Cloud mirror ' + dev.name + ': ' + e.message);
      }
    }
    this.log.debug('App-Cloud mirror: ' + mirrored + ' dps gespiegelt, ' +
                   skippedOffline + ' offline-skip, ' + kept + ' kept-online (lokal frisch), ' +
                   skippedUnknown + ' unbekannt');
  }

  async _appCloudPersistSession() {
    if (!this.appCloud || !this.appCloud.sid) return;
    try {
      await this.setObjectNotExistsAsync('_appCloud.session', {
        type: 'state',
        common: { name: 'App-Cloud Session (JSON)', type: 'string', role: 'json', read: true, write: false },
        native: {}
      });
      await this.setStateAsync('_appCloud.session', {
        val: JSON.stringify({
          sid:         this.appCloud.sid,
          lastLoginTs: this.appCloud.lastLoginTs
        }),
        ack: true
      });
    } catch (e) { /* ignore */ }
  }

  async _appCloudUpdateInfoStates(stats) {
    try {
      await this.setObjectNotExistsAsync('_appCloud.online', {
        type: 'state',
        common: { name: 'App-Cloud: Anzahl online Devices', type: 'number', role: 'value', read: true, write: false, def: 0 },
        native: {}
      });
      await this.setObjectNotExistsAsync('_appCloud.offline', {
        type: 'state',
        common: { name: 'App-Cloud: Anzahl offline Devices', type: 'number', role: 'value', read: true, write: false, def: 0 },
        native: {}
      });
      await this.setObjectNotExistsAsync('_appCloud.lastFetch', {
        type: 'state',
        common: { name: 'App-Cloud: letzter Fetch', type: 'string', role: 'date', read: true, write: false },
        native: {}
      });
      await this.setStateAsync('_appCloud.online',     { val: stats.online,  ack: true });
      await this.setStateAsync('_appCloud.offline',    { val: stats.offline, ack: true });
      await this.setStateAsync('_appCloud.lastFetch',  { val: new Date(stats.fetchedAt).toISOString(), ack: true });
    } catch (e) { /* ignore */ }
  }

  /**
   * Online-Check fuer ein Device. Liefert:
   *   true  = wahrscheinlich online (App-Cloud sagt so)
   *   false = wahrscheinlich offline (App-Cloud sagt offline)
   *   null  = App-Cloud nicht aktiv oder Device unbekannt -> nicht entscheidbar
   */
  appCloudIsOnline(devId) {
    if (!this.appCloud || !this.appCloud._lastFetchDevices) return null;
    const info = this.appCloud.getDeviceInfo(devId);
    if (!info) return null;
    return info.online;
  }

  /**
   * Migration v0.6.5: Reset aller akkumulierten Auto-Failover-Flags.
   * In v0.6.4 hat der Failover Devices DAUERHAFT cloud-only gemacht.
   * v0.6.5 hat smartere temporaere Failover-Logik - daher einmal alles zurueck.
   * Wird nur einmal ausgefuehrt - markiert via info.localResetV065.
   */
  async migrateResetFailoverV065() {
    try {
      const marker = await this.getStateAsync('info.localResetV065').catch(() => null);
      if (marker && marker.val === true) {
        this.log.debug('Migration V065 schon erledigt - skip');
        return;
      }
      await this.setObjectNotExistsAsync('info.localResetV065', {
        type: 'state',
        common: { name: 'Migration v0.6.5 Failover-Reset done', type: 'boolean', role: 'indicator', read: true, write: false, def: false },
        native: {}
      });

      let reset = 0;
      for (const id of this.devices.keys()) {
        const dev = this.devices.get(id);
        const lc = dev.local || {};
        if (!lc.ip || !lc.key) continue;
        if (!tuyaLocal.isPrivateIp(lc.ip)) continue;
        if (lc.version !== '3.3' && lc.version !== '3.4' && lc.version !== '3.5') continue;
        const cur = await this.getStateAsync(id + '.noLocalConnection').catch(() => null);
        if (cur && cur.val === true) {
          await this.setStateAsync(id + '.noLocalConnection', { val: false, ack: true });
          dev.local.preferLocal = true;
          if (dev._localFailCount) dev._localFailCount = 0;
          if (dev._cloudOnlyUntil) dev._cloudOnlyUntil = 0;
          if (dev._failoverHourCount) dev._failoverHourCount = 0;
          reset++;
        }
      }
      await this.setStateAsync('info.localResetV065', { val: true, ack: true });
      this.log.info('Migration V065: ' + reset + ' Geraete Auto-Failover-Flag zurueckgesetzt (lokales Polling reaktiviert)');
    } catch (e) {
      this.log.warn('migrateResetFailoverV065: ' + e.message);
    }
  }

  /**
   * Migration v0.9.2: Reset aller permanent-Cloud-Only-Flags.
   * v0.6.5 hatte 3x Failover -> permanent. Das war zu aggressiv und hat Devices
   * dauerhaft auf cloud-only umgestellt nur weil sie kurz WLAN-Hickser hatten.
   * v0.9.2 verschiebt die Schwelle auf 10x + Auto-Recovery bei LAN-Discovery.
   * Diese Migration setzt einmalig alle Cloud-Only-Flags zurueck.
   */
  async migrateResetFailoverV092() {
    try {
      const marker = await this.getStateAsync('info.localResetV092').catch(() => null);
      if (marker && marker.val === true) {
        this.log.debug('Migration V092 schon erledigt - skip');
        return;
      }
      await this.setObjectNotExistsAsync('info.localResetV092', {
        type: 'state',
        common: { name: 'Migration v0.9.2 Failover-Reset done', type: 'boolean', role: 'indicator', read: true, write: false, def: false },
        native: {}
      });

      let reset = 0;
      for (const id of this.devices.keys()) {
        const dev = this.devices.get(id);
        const lc = dev.local || {};
        if (!lc.ip || !lc.key) continue;
        if (!tuyaLocal.isPrivateIp(lc.ip)) continue;
        if (lc.version !== '3.3' && lc.version !== '3.4' && lc.version !== '3.5') continue;
        const cur = await this.getStateAsync(id + '.noLocalConnection').catch(() => null);
        if (cur && cur.val === true) {
          await this.setStateAsync(id + '.noLocalConnection', { val: false, ack: true });
          dev.local.preferLocal = true;
          dev._localFailCount = 0;
          dev._cloudOnlyUntil = 0;
          dev._failoverHourCount = 0;
          reset++;
        }
      }
      await this.setStateAsync('info.localResetV092', { val: true, ack: true });
      this.log.info('Migration V092: ' + reset + ' Geraete Failover-Flag zurueckgesetzt (waren auf cloud-only durch alten 3x-Schwellwert)');
    } catch (e) {
      this.log.warn('migrateResetFailoverV092: ' + e.message);
    }
  }

  async pollAll() {
    if (this.shuttingDown) return;
    const cfg = this.config || {};
    const parallel = Math.max(1, Math.min(5, Number(cfg.maxParallelPolls) || 1));
    const ids = Array.from(this.devices.keys());
    let i = 0;
    const workers = Array.from({ length: parallel }, () => (async () => {
      while (i < ids.length && !this.shuttingDown) {
        const idx = i++;
        const id = ids[idx];
        try { await this.pollDevice(id); }
        catch (e) { /* schon geloggt in pollDevice */ }
      }
    })());
    await Promise.all(workers);
  }

  async pollDevice(id) {
    const dev = this.devices.get(id);
    if (!dev) return;
    // Aktuellen _noCloudStatusPoll-Wert respektieren (User kann live umschalten)
    const noPollState = await this.getStateAsync(id + '._noCloudStatusPoll').catch(() => null);
    const skipCloud = noPollState && noPollState.val === true;

    // Schritt 1: Lokales DP_QUERY (cmd 0x0a) versuchen wenn IP+localKey bekannt.
    // Liefert ALLE DPs vom Geraet, nicht nur den Cloud-Status-Subset.
    // User-Flag noLocalConnection respektieren (preferLocal=false -> nicht lokal pollen).
    let localOk = false;
    const localCfg = dev.local || {};
    const ip      = localCfg.ip || (dev.raw && dev.raw.ip);
    const key     = localCfg.key;
    const version = localCfg.version || '3.3';
    const port    = localCfg.port || 6668;
    const preferLocal = localCfg.preferLocal !== false;   // default true

    // Smarter Failover-Check v0.6.5:
    // Falls Device aktuell in temporaerem Cloud-Mode (_cloudOnlyUntil > now),
    // skipped wir den Local-Versuch in diesem Poll-Zyklus. Nach dem Ablauf
    // probieren wir wieder.
    const now = Date.now();
    const inTempCloudMode = (dev._cloudOnlyUntil || 0) > now;

    if (preferLocal && !inTempCloudMode && ip && key && (version === '3.3' || version === '3.4' || version === '3.5')) {
      try {
        // v0.9.1: Poll bleibt knapp - 1 Try mit normalem Timeout.
        // v0.10.0: Routing nach Protocol-Version (3.3=eigene Impl, 3.4/3.5=tuyapi-lib)
        const pollTimeout = Math.max(2500, Number(this.config.localTimeoutMs) || 5000);
        const impl = this._getLocalImpl(version);
        const res = await impl.queryStatus({
          ip: ip,
          localKey: key,
          deviceId: id,
          version: version,
          port: port,
          timeoutMs: pollTimeout
        });
        if (res && res.ok && res.dps) {
          await this.mirrorStatusDps(dev, res.dps);
          localOk = true;
          dev._localFailCount = 0;   // reset
          dev._lastLocalOk = now;    // timestamp fuer Hysterese
          // online=true setzen weil lokal erreichbar
          await this.setStateAsync(id + '.online',           { val: true, ack: true }).catch(() => {});
          await this.setStateAsync(id + '.noLocalConnection',{ val: false, ack: true }).catch(() => {});
          await this.setStateAsync(id + '.localLastResult',  { val: 'queryStatus ok', ack: true }).catch(() => {});
          await this.setStateAsync(id + '.localLastSeen',    { val: new Date().toISOString(), ack: true }).catch(() => {});
          this.log.debug('Local queryStatus ' + dev.name + ': ' + Object.keys(res.dps).length + ' DPs');
        } else {
          dev._localFailCount = (dev._localFailCount || 0) + 1;
          await this.setStateAsync(id + '.localLastResult',  { val: 'queryStatus fail: ' + (res && res.reason || 'unknown'), ack: true }).catch(() => {});
          this.log.debug('Local queryStatus ' + dev.name + ' fail #' + dev._localFailCount + ': ' + (res && res.reason));

          // Online-Hysterese: erst auf false setzen wenn (50+ Fails in Folge)
          // ODER (60 Min ohne lokal-OK). Damit togglen wir nicht bei WLAN-Zickigkeit.
          const lastOk = dev._lastLocalOk || 0;
          const hourWithoutOk = (now - lastOk) > 60 * 60 * 1000;
          const manyFails = dev._localFailCount >= 50;
          if (manyFails || hourWithoutOk) {
            // Nur 1x umschalten - vermeidet redundante State-Updates
            const onlineState = await this.getStateAsync(id + '.online').catch(() => null);
            if (onlineState && onlineState.val === true) {
              await this.setStateAsync(id + '.online', { val: false, ack: true }).catch(() => {});
              this.log.debug('Online-Hysterese: ' + dev.name + ' -> offline (' +
                (manyFails ? 'fails=' + dev._localFailCount : '>1h ohne OK') + ')');
            }
          }

          // Smarter Auto-Failover v0.6.5: 3x in Folge fail -> TEMPORAER 5min cloud
          if (dev._localFailCount >= 3) {
            const hour = Math.floor(now / 3600000);
            if (dev._failoverHour !== hour) {
              dev._failoverHour = hour;
              dev._failoverHourCount = 0;
            }
            dev._failoverHourCount = (dev._failoverHourCount || 0) + 1;
            // Counter NICHT mehr resetten - wir wollen ja den >=50 Schwellenwert
            // fuer die Online-Hysterese behalten. Failover bleibt unabhaengig.

            if (dev._failoverHourCount > 10) {
              // v0.9.2: war 3 - viel zu aggressiv. 10 Fails in 1h ist plausibler
              // fuer ein wirklich kaputtes Device, nicht nur kurze WLAN-Hickser.
              // Plus: Recovery erfolgt automatisch wenn Device wieder LAN-Broadcasts sendet.
              await this.setStateAsync(id + '.noLocalConnection', { val: true, ack: true }).catch(() => {});
              dev.local.preferLocal = false;
              this.log.info('Auto-Failover (permanent): ' + dev.name + ' (' + id + ') 10x Failover in 1h - cloud-only');
            } else {
              dev._cloudOnlyUntil = now + 5 * 60 * 1000;
              this.log.debug('Auto-Failover (temp 5min): ' + dev.name + ' - lokal mehrfach gescheitert (' + dev._failoverHourCount + '/10)');
            }
          }
        }
      } catch (e) {
        dev._localFailCount = (dev._localFailCount || 0) + 1;
        this.log.debug('Local queryStatus ' + dev.name + ' exception: ' + e.message);
      }
    }

    // Schritt 2: Cloud nur wenn lokal fehlgeschlagen ODER User es nicht abgeschaltet hat.
    // Cloud-Polling pausiert wenn Quota erschoepft (v0.6.5).
    if (!localOk && !skipCloud) {
      if (this._cloudQuotaPausedUntil && Date.now() < this._cloudQuotaPausedUntil) {
        // Stilles skip - wir loggen den Quota-Status zentral im sendDeviceCommands
        return;
      }
      // v0.8.7: Wenn App-Cloud aktiv und Device als offline gemeldet -> kein Cloud-Call
      if (this.appCloud && this.config.appCloudEnabled) {
        const online = this.appCloudIsOnline(id);
        if (online === false) {
          // Device offline laut App-Cloud - Cloud-Poll waere Quota-Verschwendung.
          // ABER: nur die online-State setzen wenn wir auch wirklich KEINE lokale
          // Erreichbarkeit haben. Wenn localOk in diesem Cycle ueberstimmt sowieso.
          if (!localOk) {
            await this.setStateAsync(id + '.online', { val: false, ack: true }).catch(() => {});
          }
          return;
        }
        // v0.8.7: Wenn App-Cloud aktiv UND online: Cloud-Poll meist NICHT noetig
        // weil App-Cloud-Refresh die dps schon spiegelt. Wir polln Cloud nur noch
        // im Hybrid-Mode 1x/Stunde fuer Cross-Check.
        const mode = this.config.cloudPollMode || 'auto';
        if (online === true && (mode === 'auto' || mode === 'app-only')) {
          // App-Cloud hat bereits gespiegelt. Skip Cloud-Poll.
          return;
        }
        if (online === true && mode === 'hybrid') {
          // 1x/Stunde Cloud-Cross-Check
          const lastCross = dev._lastCloudCrossCheck || 0;
          const HOUR_MS = 60 * 60 * 1000;
          if ((Date.now() - lastCross) < HOUR_MS) {
            // Noch innerhalb der Stunde - skip
            return;
          }
          dev._lastCloudCrossCheck = Date.now();
          this.log.debug('Hybrid: Cloud-Cross-Check fuer ' + dev.name);
        }
        // mode === 'cloud-only' faellt durch zum normalen Cloud-Poll
      }
      try {
        // Auch Read-Polls zaehlen ins Quota - throttlen wie Writes
        await this._cloudThrottleWait();
        const status = await this.cloud.getStatus(id);
        await this.mirrorStatus(dev, status);
        // online: nur fuer Battery/Sub-Devices (kein lokaler Pfad moeglich) aus Cloud.
        // Bei normalen WiFi-Devices wird .online im lokalen Pfad gesetzt (Hysterese).
        const hasLocalCapability = !!(localCfg.ip && localCfg.key && this._isLocalVersionSupported(version));
        if (!hasLocalCapability && typeof dev.raw.online !== 'undefined') {
          await this.setStateAsync(id + '.online', { val: !!dev.raw.online, ack: true }).catch(() => {});
        }
      } catch (e) {
        const msg = String(e && e.message || e).toLowerCase();
        if (msg.includes('function not support')) {
          dev.noCloudStatusPoll = true;
          await this.setStateAsync(id + '._noCloudStatusPoll', { val: true, ack: true }).catch(() => {});
          this.log.warn('Cloud status poll deaktiviert fuer ' + dev.name + ' (' + id + '): function not support');
          return;
        }
        if (msg.includes('controllable device pool') || msg.includes('quota is insufficient')) {
          // Account-Tagesquota - 6h Pause, sonst spammen wir Tuya weiter
          this._cloudQuotaPausedUntil = Date.now() + 6 * 60 * 60 * 1000;
          this._lastQuotaWarnTs = Date.now();
          await this.setStateAsync('info.cloudQuotaPaused', { val: true, ack: true }).catch(() => {});
          this.log.warn('Cloud-Pool-Quota erschoepft beim Polling: ' + (e.message || e) +
            '. Pausiert fuer 6h. Hinweis: Tuya Account-Tagesquota - Local-Steuerung priorisieren.');
          return;
        }
        if (msg.includes('quota') || msg.includes('exceeded') || msg.includes('rate limit')) {
          this._cloudQuotaPausedUntil = Date.now() + 60 * 60 * 1000;
          this._lastQuotaWarnTs = Date.now();
          await this.setStateAsync('info.cloudQuotaPaused', { val: true, ack: true }).catch(() => {});
          this.log.warn('Cloud-Quota erschoepft beim Polling: ' + (e.message || e) + '. Pausiert fuer 60 Minuten.');
          return;
        }
        if (!localOk) {
          this.log.warn('Poll fehlgeschlagen ' + dev.name + ' (' + id + '): ' + e.message);
        }
      }
    }
  }

  /**
   * Mappt ein DP-Map (lokales Format: {1: false, 2: 20, ...}) in die States.
   * Geht ueber die meta-Daten des Geraets, nicht ueber den canonical-code.
   */
  async mirrorStatusDps(dev, dpsMap) {
    if (!dpsMap || typeof dpsMap !== 'object') return;
    // Map: dpId -> v (gescaled, fuer Status-Pass)
    const valuesByDpId = Object.create(null);

    for (const dpId of Object.keys(dpsMap)) {
      const rawValue = dpsMap[dpId];
      const dpIdStr = String(dpId);

      // Canonical code finden ueber dev.dpIdToCanon
      let c = (dev.dpIdToCanon && dev.dpIdToCanon[dpIdStr]) ? dev.dpIdToCanon[dpIdStr] : dpIdStr;
      let meta = dev.dpMeta[c];

      // Falls die Spec den DP nicht kannte, on-the-fly anlegen
      if (!meta) {
        meta = {
          type: typeof rawValue === 'boolean' ? 'boolean'
              : typeof rawValue === 'number'  ? 'number' : 'string',
          writable: false,
          friendly: 'dp_' + dpIdStr,
          dpId: dpIdStr
        };
        dev.dpMeta[c] = meta;
        if (!dev.canonToReal[c]) dev.canonToReal[c] = dpIdStr;
        if (!dev.dpIdToCanon) dev.dpIdToCanon = Object.create(null);
        dev.dpIdToCanon[dpIdStr] = c;
        await this.ensureCodeState(dev.id, dev.name, c, meta);
      }

      // Wert-Coerce
      let v = rawValue;
      if (meta.type === 'number') {
        if (typeof v === 'number') v = sm.scaleOut(meta, v);
        else if (typeof v === 'string') {
          const n = Number(v);
          if (!isNaN(n) && v.trim() !== '') v = sm.scaleOut(meta, n);
          else { this.log.debug('DPS ' + dev.id + '.' + dpIdStr + ': nicht number-parsebar: ' + v); continue; }
        } else if (typeof v === 'boolean') v = v ? 1 : 0;
        else continue;
      } else if (meta.type === 'boolean') {
        if (typeof v === 'string') v = ['true', '1', 'on'].includes(v.toLowerCase());
        else if (typeof v === 'number') v = v !== 0;
        else v = !!v;
      } else if (meta.type === 'string') {
        if (v === null || typeof v === 'undefined') v = '';
        else if (typeof v !== 'string') v = String(v);
      }

      valuesByDpId[dpIdStr] = v;

      // Primary state schreiben (dpId, im Tuya-Stil)
      await this.setStateAsync(dev.id + '.' + dpIdStr, { val: v, ack: true });

      // Bitmap
      if (meta.isBitmap && typeof v === 'number' && Array.isArray(meta.bitmapLabels)) {
        for (let i = 0; i < meta.bitmapLabels.length; i++) {
          const bit = (v & (1 << i)) !== 0;
          await this.setStateAsync(dev.id + '.' + dpIdStr + '-' + i, { val: bit, ack: true }).catch(() => {});
        }
      }

      // Enhanced postprocessors
      const enh = enhanced.getEnhanced(meta.dpId, c);
      if (enh) {
        for (const def of enh) {
          try {
            const derivedVal = def.fromDp ? def.fromDp(rawValue) : null;
            if (derivedVal === null || typeof derivedVal === 'undefined') continue;
            await this.setStateAsync(dev.id + '.' + dpIdStr + def.postfix, { val: derivedVal, ack: true });
          } catch (e) { /* ignore */ }
        }
      }
    }

    // Aliase mit aktuellen Werten fuettern - basierend auf dpId-Lookup ueber canonToReal
    for (const aliasKey of Object.keys(dev.alias || {})) {
      if (aliasKey.startsWith('_')) continue;
      const targetCode = dev.alias[aliasKey];
      const meta = dev.dpMeta[targetCode];
      if (!meta || !meta.dpId) continue;
      if (!(meta.dpId in valuesByDpId)) continue;
      let aliasVal = valuesByDpId[meta.dpId];

      if (aliasKey === 'brightness' && typeof aliasVal === 'number') {
        if (typeof meta.max === 'number' && meta.max > 100) {
          const min = typeof meta.min === 'number' ? meta.min : 0;
          const max = meta.max;
          if (max !== min) aliasVal = Math.round(((aliasVal - min) / (max - min)) * 100);
        }
      }
      await this.setStateAsync(dev.id + '.' + aliasKey, { val: aliasVal, ack: true });
    }
  }

  async mirrorStatus(dev, statusArr) {
    // Map: canonCode -> aktueller Wert (gescaled, fuer Status-Pass)
    const valuesByCode = Object.create(null);

    for (const s of statusArr || []) {
      const realCode = String(s.code);
      const c = sm.canon(realCode);

      // Falls in der Spec nicht vorhanden, on-the-fly anlegen
      if (!dev.canonToReal[c]) {
        dev.canonToReal[c] = realCode;
        dev.defCanonSet.add(c);
      }
      if (!dev.dpMeta[c]) {
        // Tuya Status liefert manchmal eine dps-Eigenschaft. Falls nicht, faellt
        // der Status auf reinem Code-State zurueck ohne Mirror.
        dev.dpMeta[c] = {
          type: typeof s.value === 'boolean' ? 'boolean'
              : typeof s.value === 'number'  ? 'number'  : 'string',
          writable: false,
          friendly: realCode,
          dpId: (typeof s.dps !== 'undefined') ? String(s.dps)
              : (typeof s.dp !== 'undefined') ? String(s.dp) : undefined
        };
        if (dev.dpMeta[c].dpId && String(dev.dpMeta[c].dpId) !== c) {
          if (!dev.dpIdToCanon) dev.dpIdToCanon = Object.create(null);
          dev.dpIdToCanon[String(dev.dpMeta[c].dpId)] = c;
        }
        await this.ensureCodeState(dev.id, dev.name, c, dev.dpMeta[c]);
      }

      // Wert konvertieren - defensiver Typ-Coerce damit kein "type mismatch" Error
      let v = s.value;
      const meta = dev.dpMeta[c];

      // ENUM: Tuya schickt im Status den String-Namen (z.B. 'cold'). Wir speichern
      // als number (Index). Konvertierung via meta.enums oder common.states.
      if (meta.type === 'number' && Array.isArray(meta.enums) && typeof v === 'string') {
        const idx = meta.enums.indexOf(v);
        if (idx >= 0) {
          v = idx;
        } else {
          // Wert nicht in der Liste - versuche es als reine Zahl zu interpretieren
          const n = Number(v);
          if (!isNaN(n) && v.trim() !== '') {
            v = n;
          } else {
            this.log.debug('ENUM ' + dev.id + '.' + c + ': Wert "' + v + '" nicht in [' + meta.enums.join(',') + '], skip');
            continue;
          }
        }
      } else if (meta.type === 'number') {
        if (typeof v === 'number') {
          v = sm.scaleOut(meta, v);
        } else if (typeof v === 'string') {
          const n = Number(v);
          if (!isNaN(n) && v.trim() !== '') v = sm.scaleOut(meta, n);
          else {
            // Nicht-numerischer String fuer number-State - skip statt crashen
            this.log.debug('Status ' + dev.id + '.' + c + ': string "' + v + '" nicht als number parsebar, ueberspringe');
            continue;
          }
        } else if (typeof v === 'boolean') {
          v = v ? 1 : 0;
        } else {
          continue;  // null/undefined/object fuer number-State -> skip
        }
      } else if (meta.type === 'boolean') {
        if (typeof v === 'string') v = ['true', '1', 'on'].includes(v.toLowerCase());
        else if (typeof v === 'number') v = v !== 0;
        else v = !!v;
      } else if (meta.type === 'string') {
        if (v === null || typeof v === 'undefined') v = '';
        else if (typeof v !== 'string') v = JSON.stringify(v);
      }

      valuesByCode[c] = v;
      // tuya-Style: nur in den primary State schreiben. Wenn DPS-ID vorhanden,
      // ist das <dev>.<dpsId>. Sonst Fallback auf <dev>.<codeCanon>.
      const meta2 = dev.dpMeta[c];
      const primary = (meta2 && meta2.dpId && String(meta2.dpId) !== c) ? String(meta2.dpId) : c;
      await this.setStateAsync(dev.id + '.' + primary, { val: v, ack: true });

      // Bitmap: jedes Bit als <primary>-N boolean state setzen
      if (meta2 && meta2.isBitmap && typeof v === 'number' && Array.isArray(meta2.bitmapLabels)) {
        for (let i = 0; i < meta2.bitmapLabels.length; i++) {
          const bit = (v & (1 << i)) !== 0;
          await this.setStateAsync(dev.id + '.' + primary + '-' + i, { val: bit, ack: true }).catch(() => {});
        }
      }

      // Enhanced-postprocessors: derived states fuettern
      const enh = enhanced.getEnhanced(meta2 && meta2.dpId, c);
      if (enh) {
        // Roh-Wert vom Adapter ist normalerweise das was wir gerade auch in primary
        // geschrieben haben (z.B. der hex-string). Aber wenn das ein number-State
        // ist, hat scaleOut den Wert schon angefasst - wir wollen den UNGESKAlten
        // Original-Roh-Wert. Tuya schickt Color als String, daher meist OK.
        const rawVal = s.value;  // Original aus dem Status-Eintrag
        for (const def of enh) {
          try {
            const derivedVal = def.fromDp ? def.fromDp(rawVal) : null;
            if (derivedVal === null || typeof derivedVal === 'undefined') continue;
            await this.setStateAsync(dev.id + '.' + primary + def.postfix, { val: derivedVal, ack: true });
          } catch (e) {
            this.log.debug('enhanced postprocess ' + dev.id + '.' + primary + def.postfix + ': ' + e.message);
          }
        }
      }
    }

    // Aliase mit aktuellen Werten fuettern - sonst bleiben sie leer und sehen
    // "doppelt aber ohne Wert" aus.
    for (const aliasKey of Object.keys(dev.alias)) {
      if (aliasKey.startsWith('_')) continue;  // _modeType etc sind Meta
      const targetCode = dev.alias[aliasKey];
      if (!(targetCode in valuesByCode)) continue;
      let aliasVal = valuesByCode[targetCode];

      // brightness: Geraete-Skala -> 0-100
      if (aliasKey === 'brightness' && typeof aliasVal === 'number') {
        const meta = dev.dpMeta[targetCode];
        if (meta && typeof meta.max === 'number' && meta.max > 100) {
          const min = typeof meta.min === 'number' ? meta.min : 0;
          const max = meta.max;
          if (max !== min) {
            aliasVal = Math.round(((aliasVal - min) / (max - min)) * 100);
          }
        }
      }

      await this.setStateAsync(dev.id + '.' + aliasKey, { val: aliasVal, ack: true });
    }
  }

  // -------------- Write-Handling --------------

  async onStateChange(id, state) {
    if (!state || state.ack) return;     // nur User-Writes mit ack=false
    if (this.ignoreNextChange.has(id)) {
      this.ignoreNextChange.delete(id);
      return;
    }

    // Commands-Buttons
    if (id === this.namespace + '.commands.rediscover') {
      await this.setStateAsync(id, { val: false, ack: true });
      this.log.info('Manuelle Rediscovery angefordert');
      this.discoverAll().catch(e => this.log.warn('Rediscover: ' + e.message));
      return;
    }
    if (id === this.namespace + '.commands.refreshAll') {
      await this.setStateAsync(id, { val: false, ack: true });
      this.log.info('Manueller refreshAll angefordert');
      this.pollAll().catch(e => this.log.warn('refreshAll: ' + e.message));
      return;
    }
    if (id === this.namespace + '.commands.rescanLan') {
      await this.setStateAsync(id, { val: false, ack: true });
      this.log.info('Rescan-LAN angefordert (re-import aus tuya.0 + Re-Apply Cache)');
      (async () => {
        try {
          if (this.config.importLocalFromTuyaAdapter !== false) {
            await this.importLocalFromTuyaAdapter();
          }
          // Cache-Records nochmal durch-applien
          for (const did of Object.keys(this.discoveryCache)) {
            await this.onLanDiscoveryRecord(this.discoveryCache[did]).catch(() => {});
          }
        } catch (e) { this.log.warn('rescanLan: ' + e.message); }
      })();
      return;
    }
    if (id === this.namespace + '.commands.testAppCloud') {
      await this.setStateAsync(id, { val: false, ack: true });
      if (this._appCloudTestRunning) { return; }
      this._appCloudTestRunning = true;
      this.log.info('=== App-Cloud Test ===');
      (async () => {
        try {
          const cfg = this.config;
          if (!cfg.appCloudEmail || !cfg.appCloudPassword) {
            this.log.warn('App-Cloud Test: email/password fehlen');
            return;
          }
          // Schauen ob ein Debug-Device gesetzt ist
          const debugDevIdState = await this.getStateAsync('commands.debugDeviceId').catch(() => null);
          const debugDevId = debugDevIdState && debugDevIdState.val ? String(debugDevIdState.val).trim() : '';

          if (!this.appCloud) {
            this.log.warn('App-Cloud nicht aktiv - aktiviere appCloudEnabled erst');
            return;
          }

          this.log.info('Force-Refresh App-Cloud...');
          await this.appCloud.fetchAll();
          const stats = this.appCloud.getCachedStats();
          this.log.info('Nach Refresh: ' + stats.total + ' Devices, ' + stats.online + ' online, ' + stats.offline + ' offline');

          if (debugDevId) {
            // Targeted Debug fuer ein Device
            const d = this.appCloud._lastFetchDevices.get(debugDevId);
            if (!d) {
              this.log.warn('Device-ID ' + debugDevId + ' nicht in App-Cloud-Cache!');
              return;
            }
            this.log.info('--- DEBUG Device "' + d.name + '" (' + debugDevId + ') ---');
            this.log.info('  productId    : ' + d.productId);
            this.log.info('  category     : ' + d.category + ' (' + d.categoryCode + ')');
            this.log.info('  devAttribute : ' + d.devAttribute + ' (bin: ' + (d.devAttribute || 0).toString(2) + ')');
            this.log.info('  isDeviceOnline (unser Check): ' + this.appCloud.isDeviceOnline(d));
            this.log.info('  moduleMap (RAW JSON): ' + JSON.stringify(d.moduleMap || {}));
            this.log.info('  dps          : ' + JSON.stringify(d.dps).substring(0, 300));
            this.log.info('  activeTime   : ' + d.activeTime + ' (' + (d.activeTime ? new Date(d.activeTime * 1000).toISOString() : '-') + ')');
            this.log.info('  dpMaxTime    : ' + d.dpMaxTime);
            this.log.info('  localKey     : ' + d.localKey);
            this.log.info('  ip           : ' + d.ip);
            this.log.info('  mac          : ' + d.mac);
            this.log.info('  virtual      : ' + d.virtual);
            this.log.info('  runtimeEnv   : ' + d.runtimeEnv);
            // Plus: was wissen WIR ueber das Device?
            const ourDev = this.devices.get(debugDevId);
            if (ourDev) {
              this.log.info('  --- Unser Device-Cache ---');
              this.log.info('    local.ip      : ' + (ourDev.local && ourDev.local.ip));
              this.log.info('    local.key     : ' + (ourDev.local && ourDev.local.key));
              this.log.info('    local.version : ' + (ourDev.local && ourDev.local.version));
              this.log.info('    local.preferLocal : ' + (ourDev.local && ourDev.local.preferLocal));
              this.log.info('    _lastLocalOk  : ' + (ourDev._lastLocalOk ? new Date(ourDev._lastLocalOk).toISOString() : 'never'));
            }
            this.log.info('--- ENDE ---');
          } else {
            // Listing aller offline Devices
            const offlineDetails = [];
            for (const [did, devData] of this.appCloud._lastFetchDevices) {
              if (this.appCloud.isDeviceOnline(devData)) continue;
              const m = devData.moduleMap || {};
              const isOnlinePerModule = {};
              for (const k of Object.keys(m)) isOnlinePerModule[k] = m[k] ? m[k].isOnline : 'n/a';
              offlineDetails.push({ devId: did, name: devData.name, mods: isOnlinePerModule });
            }
            this.log.info('--- OFFLINE laut isDeviceOnline (' + offlineDetails.length + ') ---');
            for (const od of offlineDetails.slice(0, 25)) {
              this.log.info('  ' + od.name + ' (' + od.devId + ') online=' + JSON.stringify(od.mods));
            }
            this.log.info('Tipp: setze commands.debugDeviceId="<devId>" und triggere testAppCloud erneut fuer Detail-Output.');
          }
          this.log.info('=== App-Cloud Test fertig ===');
        } catch (e) {
          this.log.error('App-Cloud Test FAIL: ' + (e && e.message || e));
        } finally {
          this._appCloudTestRunning = false;
        }
      })();
      return;
    }

    // Path zerlegen: fid-smartlife.0.<deviceId>.<code>
    const prefix = this.namespace + '.';
    if (!id.startsWith(prefix)) return;
    const rest = id.slice(prefix.length);
    const parts = rest.split('.');
    if (parts.length < 2) return;
    const deviceId = parts[0];
    const code     = parts.slice(1).join('.');

    if (code.startsWith('_')) return;            // interne Felder (z.B. _name, _noCloudStatusPoll)
    const dev = this.devices.get(deviceId);
    if (!dev) return;

    // Lokale Settings (writable, kein Device-DP): nur ack-en, kein cloud/local-Write
    const LOCAL_SETTINGS = new Set(['localKey','localVersion','localPort','localSource','localLastResult','localLastSeen','noLocalConnection','ip']);
    if (LOCAL_SETTINGS.has(code)) {
      await this.setStateAsync(id, { val: state.val, ack: true });
      return;
    }

    // Alias?
    if (sm.ALIAS_NAMES.has(code) && dev.alias[code]) {
      await this.handleAliasWrite(dev, code, state.val);
      return;
    }

    // Derived enhanced-state? (z.B. '5-rgb' -> Color schreiben auf DPS-ID 5)
    const enhMatch = enhanced.findEnhancedForDerived(code);
    if (enhMatch && enhMatch.def && enhMatch.def.toDp) {
      const encoded = enhMatch.def.toDp(state.val);
      if (encoded !== null && encoded !== undefined) {
        const targetCanon = dev.dpIdToCanon && dev.dpIdToCanon[enhMatch.dpId];
        if (targetCanon) {
          // Bei Color-Writes optimistisch auch den derived state ack-en
          if (this.config && this.config.optimisticAck) {
            await this.setStateAsync(id, { val: state.val, ack: true });
            await this.setStateAsync(dev.id + '.' + enhMatch.dpId, { val: encoded, ack: true });
          }
          await this.handleDirectWrite(dev, targetCanon, encoded);
        } else {
          this.log.debug('Derived write ' + code + ': kein DPS-ID-Mapping fuer ' + enhMatch.dpId);
        }
      }
      return;
    }

    // DPS-ID-Write? (Primary-State - das ist der "richtige" Write-Pfad jetzt)
    if (dev.dpIdToCanon && dev.dpIdToCanon[code]) {
      const targetCanon = dev.dpIdToCanon[code];
      await this.handleDirectWrite(dev, targetCanon, state.val);
      return;
    }

    // Direkter Code-Name-Write? (Fallback fuer DPs ohne dpId - z.B. extended codes)
    const c = sm.canon(code);
    if (dev.canonToReal[c]) {
      // Pruefen ob's keinen DPS-ID-State gibt der den Code beansprucht - sonst
      // war's ein Write auf einen alten Code-State der durch Orphan-Cleanup
      // gleich verschwindet
      const meta = dev.dpMeta[c];
      if (!meta || !meta.dpId || String(meta.dpId) === c) {
        await this.handleDirectWrite(dev, c, state.val);
      }
      return;
    }
  }

  async handleAliasWrite(dev, aliasKey, value) {
    const targetCode = dev.alias[aliasKey];
    if (!targetCode) return;

    let writeVal = value;
    // brightness: 0-100 -> Geraete-Skala
    if (aliasKey === 'brightness' && typeof value === 'number') {
      const meta = dev.dpMeta[targetCode];
      if (meta && typeof meta.max === 'number' && meta.max > 100) {
        const min = typeof meta.min === 'number' ? meta.min : 0;
        const max = meta.max;
        writeVal = Math.round(min + (value / 100) * (max - min));
      }
    }

    try {
      if (this.config.optimisticAck) {
        // Alias + Ziel-State optimistisch setzen
        await this.setStateAsync(dev.id + '.' + aliasKey, { val: value, ack: true });
        // tuya-Style: primary state ist die DPS-ID wenn vorhanden
        const tMeta = dev.dpMeta[targetCode];
        const tPrimary = (tMeta && tMeta.dpId && String(tMeta.dpId) !== targetCode) ? String(tMeta.dpId) : targetCode;
        await this.setStateAsync(dev.id + '.' + tPrimary, { val: writeVal, ack: true });
      }
      const realCode = dev.canonToReal[targetCode] || targetCode;
      await this.sendDeviceCommands(dev, [{ codeCanon: targetCode, realCode: realCode, value: writeVal }]);
      // Re-poll mit kurzer Verzoegerung
      this.setTimeout(() => this.pollDevice(dev.id).catch(() => {}), 1500);
    } catch (e) {
      this.log.warn('Alias-Write fehlgeschlagen ' + dev.name + '.' + aliasKey + ': ' + e.message);
    }
  }

  async handleDirectWrite(dev, codeCanon, value) {
    // Debounce pro <device>::<code>
    const key = dev.id + '::' + codeCanon;
    if (this.writeTimers[key]) clearTimeout(this.writeTimers[key]);

    const debounceMs = Math.max(0, Number(this.config.writeDebounceMs) || 400);

    this.writeTimers[key] = setTimeout(async () => {
      delete this.writeTimers[key];
      try {
        const meta = dev.dpMeta[codeCanon];
        let writeVal = value;

        // ENUM-Reverse: User schreibt number-Index, Tuya erwartet String-Name
        if (meta && Array.isArray(meta.enums) && typeof value === 'number'
            && Number.isInteger(value) && value >= 0 && value < meta.enums.length) {
          writeVal = meta.enums[value];
        } else if (typeof value === 'number') {
          writeVal = sm.scaleIn(meta, value);
        }

        if (this.config.optimisticAck) {
          const primary = (meta && meta.dpId && String(meta.dpId) !== codeCanon) ? String(meta.dpId) : codeCanon;
          await this.setStateAsync(dev.id + '.' + primary, { val: value, ack: true });
        }
        const realCode = dev.canonToReal[codeCanon] || codeCanon;
        await this.sendDeviceCommands(dev, [{ codeCanon: codeCanon, realCode: realCode, value: writeVal }]);
        // Re-poll mit kurzer Verzoegerung
        this.setTimeout(() => this.pollDevice(dev.id).catch(() => {}), 1500);
      } catch (e) {
        this.log.warn('Direkt-Write fehlgeschlagen ' + dev.name + '.' + codeCanon + ': ' + e.message);
      }
    }, debounceMs);
  }

  /**
   * Zentrale Schreiblogik: erst Local probieren (wenn preferLocal=true), bei
   * Fehler Cloud-Fallback. Sonst direkt Cloud.
   *
   * @param {object} dev          Device aus this.devices
   * @param {Array<{codeCanon:string, realCode:string, value:any}>} pairs
   */
  async sendDeviceCommands(dev, pairs) {
    // Frische Local-Settings aus States lesen (User kann sie live aendern)
    dev.local = await this.readLocalStates(dev.id, dev.raw);

    // Vorab-Check: ohne IP+key+v3.3 hat lokaler Versuch keine Chance.
    // Damit sparen wir den Timeout-Roundtrip + sofort-Cloud-Fallback.
    const canLocal = !!(dev.local.preferLocal && dev.local.ip && dev.local.key && this._isLocalVersionSupported(dev.local.version));
    if (!canLocal) {
      // Diagnose-Log: warum nicht
      const why = [];
      if (!dev.local.preferLocal) why.push('preferLocal=false');
      if (!dev.local.ip)          why.push('no ip');
      if (!dev.local.key)         why.push('no key');
      if (!this._isLocalVersionSupported(dev.local.version)) why.push('version=' + dev.local.version + ' nicht unterstuetzt');
      this.log.info('canLocal=false fuer ' + dev.name + ': ' + why.join(', ') + ' - direkt Cloud-Pfad');
    }

    if (canLocal) {
      // dpsMap: dpId-String -> value
      const dpsMap = {};
      let allDpsKnown = true;
      for (const p of pairs) {
        const meta = dev.dpMeta[p.codeCanon];
        if (!meta || !meta.dpId) { allDpsKnown = false; break; }
        dpsMap[meta.dpId] = p.value;
      }

      if (!allDpsKnown) {
        await this.setStateAsync(dev.id + '.localLastResult', { val: 'local skipped: no dpId mapping', ack: true });
        this.log.info('Local skipped fuer ' + dev.name + ': no dpId mapping fuer mindestens einen Code');
        // Fallthrough auf Cloud
      } else {
        // 3 lokale Versuche bevor wir Cloud nehmen. Erste Versuch mit normalem
        // Timeout. Wenn 1. fail: 200ms warten, dann nochmal mit 1.5x Timeout.
        // Wenn 2. fail: 500ms warten, 2x Timeout. Erst dann Cloud.
        // v0.9.1: 4 Tries (war 3 vorher, 10 in 0.9.0 war zu aggressiv).
        // Worst-Case: 4 × 5s = 20s. Bei excellent WiFi: 1. Try erfolgreich (<500ms).
        // Schnelles Blinken (3s-Zyklen) braucht <1s Latenz - das geht nur wenn
        // Local sofort klappt. Worst-Case Failover zu Cloud nach 20s ist OK.
        const baseTimeout = Number(this.config.localTimeoutMs) || 5000;
        const tries = [
          { delayBeforeMs: 0,    timeoutMs: baseTimeout },              // 0s -> 5s
          { delayBeforeMs: 300,  timeoutMs: baseTimeout },              // 0.3s -> 5s
          { delayBeforeMs: 700,  timeoutMs: Math.round(baseTimeout * 1.2) }, // 1s -> 6s
          { delayBeforeMs: 1500, timeoutMs: Math.round(baseTimeout * 1.4) }  // 1.5s -> 7s
        ];
        this.log.info('Local Write Start fuer ' + dev.name + ' -> ' + dev.local.ip + ':' + dev.local.port + ' dps=' + JSON.stringify(dpsMap));
        let lastReason = 'unknown';
        const tStart = Date.now();
        for (let i = 0; i < tries.length; i++) {
          const t = tries[i];
          if (t.delayBeforeMs > 0) {
            await new Promise(r => setTimeout(r, t.delayBeforeMs));
          }
          const tTry = Date.now();
          const implWrite = this._getLocalImpl(dev.local.version);
          const r = await implWrite.sendCommand({
            ip:        dev.local.ip,
            localKey:  dev.local.key,
            deviceId:  dev.id,
            dpsMap:    dpsMap,
            version:   dev.local.version,
            port:      dev.local.port,
            timeoutMs: t.timeoutMs
          });
          const tDur = Date.now() - tTry;
          this.log.info('Local Try #' + (i + 1) + ' fuer ' + dev.name + ' (v' + dev.local.version + '): ' + (r.ok ? 'OK' : 'FAIL') + ' (' + tDur + 'ms, reason: ' + (r.reason || '-') + ')');
          if (r.ok) {
            const totalMs = Date.now() - tStart;
            const note = (i === 0) ? 'local ok' : ('local ok (retry ' + i + ', ' + totalMs + 'ms)');
            await this.setStateAsync(dev.id + '.localLastResult', { val: note, ack: true });
            return;   // Erfolg - kein Cloud-Versuch
          }
          lastReason = r.reason;
        }
        // Alle 4 Tries failed. Wenn aktuelle Version 3.3 und alle Tries
        // timeout (nicht ENETUNREACH/EHOSTUNREACH/ECONNREFUSED, das wäre
        // Netzwerk-Problem): vermutlich hat das Device per OTA auf v3.4
        // oder v3.5 upgegradet. Probieren wir 1x mit v3.5, dann 1x mit v3.4.
        // Wenn einer der beiden klappt: localVersion permanent updaten.
        const onlyTimeouts = lastReason && /^timeout$/i.test(String(lastReason));
        if (dev.local.version === '3.3' && onlyTimeouts) {
          for (const probeVer of ['3.5', '3.4']) {
            this.log.info('Auto-Version-Probe ' + dev.name + ': versuche v' + probeVer + ' (v3.3 hat ' + lastReason + ')');
            try {
              const probeImpl = this._getLocalImpl(probeVer);
              const pr = await probeImpl.sendCommand({
                ip:        dev.local.ip,
                localKey:  dev.local.key,
                deviceId:  dev.id,
                dpsMap:    dpsMap,
                version:   probeVer,
                port:      dev.local.port,
                timeoutMs: 5000
              });
              if (pr.ok) {
                this.log.info('Auto-Version-Probe ' + dev.name + ': v' + probeVer + ' OK - localVersion permanent aktualisiert');
                await this.setStateAsync(dev.id + '.localVersion', { val: probeVer, ack: true });
                dev.local.version = probeVer;
                await this.setStateAsync(dev.id + '.localLastResult', { val: 'local ok (auto-probe v' + probeVer + ')', ack: true });
                return;  // Erfolg
              }
            } catch (eProbe) { /* try next */ }
          }
        }

        await this.setStateAsync(dev.id + '.localLastResult', { val: 'local fail after 4 tries: ' + lastReason, ack: true });
        const totalMs = Date.now() - tStart;
        // v0.10.7: Wenn _noCloudWrite=true gesetzt ist, KEIN Cloud-Fallback.
        // Spart Cloud-Quota fuer Devices die nur Cloud-Only funktionieren.
        if (dev.local && dev.local.noCloudWrite) {
          this.log.warn('Local FAIL fuer ' + dev.name + ' nach 4 Tries / ' + totalMs + 'ms (' + lastReason + ') - KEIN Cloud-Fallback (_noCloudWrite=true)');
          await this.setStateAsync(dev.id + '.localLastResult', { val: 'local fail, no cloud fallback (_noCloudWrite=true): ' + lastReason, ack: true });
          throw new Error('local fail + no cloud fallback: ' + lastReason);
        }
        this.log.info('Local FAIL fuer ' + dev.name + ' nach 4 Tries / ' + totalMs + 'ms (' + lastReason + ') - fallback zu Cloud');
      }
    } else if (dev.local.preferLocal && !dev.local.ip) {
      // IP fehlt komplett - kein lokaler Versuch
      if (dev.local.noCloudWrite) {
        // v0.10.7: kein Cloud-Fallback - direkt aufgeben
        this.log.warn(dev.name + ': keine lokale IP und _noCloudWrite=true - Schreiben abgebrochen');
        await this.setStateAsync(dev.id + '.localLastResult', { val: 'no local ip + no cloud fallback (_noCloudWrite=true)', ack: true });
        throw new Error('no local ip + no cloud fallback');
      }
      await this.setStateAsync(dev.id + '.localLastResult', { val: 'no local ip - direct cloud', ack: true });
    } else if (dev.local.preferLocal && !this._isLocalVersionSupported(dev.local.version)) {
      // Andere Versionen (z.B. 3.1 oder Sub-Devices) - kein lokaler Support, direkt Cloud
      if (dev.local.noCloudWrite) {
        // v0.10.7: kein Cloud-Fallback
        this.log.warn(dev.name + ': Version ' + dev.local.version + ' nicht lokal unterstuetzt und _noCloudWrite=true - Schreiben abgebrochen');
        await this.setStateAsync(dev.id + '.localLastResult', { val: 'unsupported version + no cloud fallback (_noCloudWrite=true)', ack: true });
        throw new Error('unsupported version + no cloud fallback');
      }
      await this.setStateAsync(dev.id + '.localLastResult', { val: 'version ' + dev.local.version + ' not supported locally - direct cloud', ack: true });
    }

    // Cloud-Pfad: mit Quota-Guard + globalem Throttle (v0.6.5)
    // PLUS v0.8.6: App-Cloud Online-Check. Wenn App-Cloud sagt das Device ist
    // offline -> Cloud-Write skippen (waere eh Fehler + verbrennt Quota).
    // v0.8.8: Aber NUR wenn Device auch nicht lokal erreichbar ist.
    if (this.config.appCloudEnabled && this.appCloud) {
      const onlineStatus = this.appCloudIsOnline(dev.id);
      if (onlineStatus === false) {
        // Device laut App-Cloud offline. Aber moeglicherweise lokal noch da
        // (App-Cloud-Cache ist 5min alt, oder moduleMap-Heuristik versagt).
        // Wir geben dem lokalen Pfad eine letzte Chance falls IP+key+v3.3 da.
        const hasLocalCapability = !!(dev.local && dev.local.ip && dev.local.key && this._isLocalVersionSupported(dev.local.version));
        if (hasLocalCapability && dev.local.preferLocal !== false) {
          this.log.debug('App-Cloud meldet ' + dev.name + ' offline, aber local moeglich - versuche lokal (10 retries)');
          // dpsMap fuer Local-Versuch bauen
          const dpsMap = {};
          let allDpsKnown = true;
          for (const p of pairs) {
            const meta = dev.dpMeta[p.codeCanon];
            if (!meta || !meta.dpId) { allDpsKnown = false; break; }
            dpsMap[meta.dpId] = p.value;
          }
          if (allDpsKnown) {
            // v0.9.1: 4 Tries (war 10, blockiert Adapter zu lang)
            const baseTimeout = Number(this.config.localTimeoutMs) || 5000;
            const tries = [
              { delay: 0,    to: baseTimeout },
              { delay: 300,  to: baseTimeout },
              { delay: 700,  to: Math.round(baseTimeout * 1.2) },
              { delay: 1500, to: Math.round(baseTimeout * 1.4) }
            ];
            const tStart = Date.now();
            let lastReason = 'unknown';
            for (let i = 0; i < tries.length; i++) {
              const t = tries[i];
              if (t.delay > 0) await new Promise(r => setTimeout(r, t.delay));
              try {
                const implAcfb = this._getLocalImpl(dev.local.version);
                const r = await implAcfb.sendCommand({
                  ip:        dev.local.ip,
                  localKey:  dev.local.key,
                  deviceId:  dev.id,
                  dpsMap:    dpsMap,
                  version:   dev.local.version,
                  port:      dev.local.port || 6668,
                  timeoutMs: t.to
                });
                if (r && r.ok) {
                  const totalMs = Date.now() - tStart;
                  await this.setStateAsync(dev.id + '.localLastResult', { val: 'local ok (appCloud-offline fallback, retry ' + i + ', ' + totalMs + 'ms)', ack: true });
                  await this.setStateAsync(dev.id + '.online', { val: true, ack: true });
                  this.log.info('Local-Fallback erfolgreich fuer ' + dev.name + ' nach ' + (i + 1) + ' Versuchen / ' + totalMs + 'ms (App-Cloud sagte offline)');
                  return;
                }
                lastReason = r.reason || 'unknown';
              } catch (eLoc) {
                lastReason = eLoc.message;
              }
            }
            this.log.debug('Local fallback failed for ' + dev.name + ' after 4 tries: ' + lastReason);
          }
        }
        // Lokal auch nicht moeglich/erfolgreich -> wirklich skippen
        const msg = 'App-Cloud meldet Device offline - Cloud-Write skipped';
        this.log.warn('Cloud-Write ' + dev.name + ' uebersprungen: ' + msg);
        await this.setStateAsync(dev.id + '.localLastResult', { val: msg, ack: true }).catch(() => {});
        await this.setStateAsync(dev.id + '.online', { val: false, ack: true }).catch(() => {});
        throw new Error(msg);
      }
    }
    await this._cloudThrottleWait();
    if (this._cloudQuotaPausedUntil && Date.now() < this._cloudQuotaPausedUntil) {
      const minLeft = Math.ceil((this._cloudQuotaPausedUntil - Date.now()) / 60000);
      const msg = 'Cloud Quota erschoepft - Schreiben fuer ' + minLeft + 'min pausiert';
      // Nur 1x pro 10 Min loggen
      const now = Date.now();
      if (!this._lastQuotaWarnTs || (now - this._lastQuotaWarnTs) > 10 * 60000) {
        this._lastQuotaWarnTs = now;
        this.log.warn('Cloud-Write ' + dev.name + ' uebersprungen: ' + msg);
      }
      await this.setStateAsync(dev.id + '.localLastResult', { val: msg, ack: true }).catch(() => {});
      throw new Error(msg);
    }
    // Wenn Backoff vorbei: Quota-State zuruecksetzen
    if (this._cloudQuotaPausedUntil && Date.now() >= this._cloudQuotaPausedUntil) {
      this._cloudQuotaPausedUntil = 0;
      await this.setStateAsync('info.cloudQuotaPaused', { val: false, ack: true }).catch(() => {});
      this.log.info('Cloud-Quota-Pause vorbei - Cloud-Writes wieder aktiv');
    }
    const cloudCmds = pairs.map(p => ({ code: p.realCode, value: p.value }));
    try {
      await this.cloud.sendCommands(dev.id, cloudCmds);
    } catch (eCloud) {
      const m = String(eCloud && eCloud.message || eCloud).toLowerCase();
      const isPoolQuota = m.includes('controllable device pool') || m.includes('quota is insufficient');
      const isRateLimit = m.includes('quota') || m.includes('exceeded') || m.includes('rate limit');

      if (isPoolQuota) {
        // "Controllable device pool quota" - das ist Tuya's Account-Tagesquota,
        // NICHT ein 1h-Rate-Limit. Reset erfolgt im 24h-Sliding-Window. Wir machen
        // 6h Pause damit wir nicht alle 60min wieder in den selben Fehler laufen.
        // Plus: log mit Hinweis dass User Account-Upgrade braucht.
        this._cloudQuotaPausedUntil = Date.now() + 6 * 60 * 60 * 1000;
        this._lastQuotaWarnTs = Date.now();
        await this.setStateAsync('info.cloudQuotaPaused', { val: true, ack: true }).catch(() => {});
        this.log.warn('Cloud-Pool-Quota erschoepft (' + (eCloud.message || eCloud) +
          '). Cloud-Writes fuer 6h pausiert. Hinweis: das ist Tuya\'s Tages-Quota, ' +
          'nicht behebbar durch Warten. Loesungen: (1) lokale Steuerung priorisieren, ' +
          '(2) Tuya IoT Project upgraden, (3) Devices auf separate Tuya-Projekte verteilen.');
      } else if (isRateLimit) {
        // Klassisches Rate-Limit: 1h reicht
        this._cloudQuotaPausedUntil = Date.now() + 60 * 60 * 1000;
        this._lastQuotaWarnTs = Date.now();
        await this.setStateAsync('info.cloudQuotaPaused', { val: true, ack: true }).catch(() => {});
        this.log.warn('Cloud-Rate-Limit (' + (eCloud.message || eCloud) + '). Cloud-Writes pausiert fuer 60 Minuten.');
      }

      // Retry-Local: wenn Cloud quota/rate-limit und Device hat lokale Capability,
      // versuchen wir nochmal lokal. Wir kommen hierhin nur wenn der lokale Pfad
      // vorher entweder gar nicht versucht wurde (v3.4/3.5, kein IP) oder fehlgeschlagen
      // ist. Bei Quota lohnt sich ein letzter lokaler Versuch besser als gar nichts.
      if ((isPoolQuota || isRateLimit) && dev.local.preferLocal && dev.local.ip && dev.local.key && this._isLocalVersionSupported(dev.local.version)) {
        this.log.info('Cloud quota - probiere lokal nochmal fuer ' + dev.name);
        try {
          const dpsMap = {};
          for (const p of pairs) {
            const dpId = (dev.dpMeta && dev.dpMeta[p.realCode] && dev.dpMeta[p.realCode].dpId);
            if (!dpId) throw new Error('no dpId for ' + p.realCode);
            dpsMap[dpId] = p.value;
          }
          const implRetry = this._getLocalImpl(dev.local.version);
          const r = await implRetry.sendCommand({
            ip:        dev.local.ip,
            localKey:  dev.local.key,
            deviceId:  dev.id,
            dpsMap:    dpsMap,
            version:   dev.local.version,
            port:      dev.local.port,
            timeoutMs: 5000
          });
          if (r.ok) {
            await this.setStateAsync(dev.id + '.localLastResult', { val: 'local ok (cloud-quota fallback)', ack: true });
            return;  // Geschafft - kein Fehler weiterreichen
          }
        } catch (eLocal) {
          this.log.debug('Cloud-quota local-retry fail ' + dev.name + ': ' + (eLocal.message || eLocal));
        }
      }

      throw eCloud;
    }
    if (dev.local.preferLocal) {
      await this.setStateAsync(dev.id + '.localLastResult', { val: 'cloud fallback used', ack: true });
    }
  }

  /**
   * Globaler Cloud-Write-Throttle (v0.6.5): max 1 Cloud-Write pro 500ms.
   * Damit ein Burst von 10+ Klicks nicht gleichzeitig die Tuya-Quota knallt.
   */
  async _cloudThrottleWait() {
    const MIN_GAP_MS = 500;
    const now = Date.now();
    const last = this._lastCloudWriteTs || 0;
    const wait = (last + MIN_GAP_MS) - now;
    if (wait > 0) {
      await new Promise(r => setTimeout(r, wait));
    }
    this._lastCloudWriteTs = Date.now();
  }

  // -------------- LAN-Discovery --------------

  async onLanDiscoveryRecord(rec) {
    // Cache aktualisieren - falls Geraet noch nicht in Cloud-Discovery angekommen ist
    this.discoveryCache[rec.id] = rec;

    const dev = this.devices.get(rec.id);
    if (!dev) return;  // Discovery vor Cloud-Setup, kommt spaeter durch buildDevice

    let changed = false;
    if (rec.ip && dev.local && dev.local.ip !== rec.ip) {
      dev.local.ip = rec.ip;
      await this.setStateAsync(dev.id + '.ip', { val: rec.ip, ack: true });
      changed = true;
    }
    if (rec.version && dev.local && dev.local.version !== rec.version) {
      dev.local.version = rec.version;
      await this.setStateAsync(dev.id + '.localVersion', { val: rec.version, ack: true });
      changed = true;
    }
    await this.setStateAsync(dev.id + '.localLastSeen', { val: new Date(rec.ts).toISOString(), ack: true });
    await this.setStateAsync(dev.id + '.localSource',   { val: rec.source, ack: true });
    if (changed) {
      this.log.info('LAN-Discovery: ' + dev.name + ' -> ' + rec.ip + (rec.version ? ' v' + rec.version : ''));
    }

    // v0.9.2: Recovery von permanentem Cloud-Only-Failover.
    // Wenn ein Device wieder Broadcasts sendet (LAN-Discovery hat es gerade
    // gesehen), war es entweder neu im Netz oder ist von schlechtem WLAN
    // erholt. Wir resetten dann noLocalConnection=true automatisch, damit
    // beim naechsten Write/Poll wieder lokal versucht wird.
    try {
      const noLocalState = await this.getStateAsync(dev.id + '.noLocalConnection').catch(() => null);
      if (noLocalState && noLocalState.val === true) {
        await this.setStateAsync(dev.id + '.noLocalConnection', { val: false, ack: true });
        // In-Memory-State auch korrigieren falls dev.local schon da ist
        if (dev.local) dev.local.preferLocal = true;
        // Failover-Counter zuruecksetzen damit wir nicht sofort wieder reinkippen
        dev._localFailCount = 0;
        dev._failoverHourCount = 0;
        dev._cloudOnlyUntil = 0;
        this.log.info('Recovery: ' + dev.name + ' wieder via LAN sichtbar - noLocalConnection zurueck auf false');
      }
    } catch (e) { /* ignore */ }
  }

  // -------------- Message-Handler (iobroker sendto) --------------

  async onMessage(obj) {
    if (!obj || !obj.command) return;
    try {
      if (obj.command === 'importCloudIPs') {
        // Holt fuer alle Devices die noch keine ip haben die ip per
        // /v1.0/devices/{id} aus der Tuya-Cloud. Rate-limited 1/Sek wegen Quota.
        const max = (obj.message && obj.message.max) || 999;
        const result = await this.importCloudIPs(max);
        if (obj.callback) {
          this.sendTo(obj.from, obj.command, result, obj.callback);
        }
        return;
      }
      if (obj.command === 'writeStats') {
        // Statistik wo Writes hingegangen sind in letzter Zeit
        let local = 0, cloud = 0, fail = 0, noIp = 0;
        const noIpDevices = [];
        for (const [id, dev] of this.devices) {
          if (!dev.local) continue;
          const lr = await this.getStateAsync(id + '.localLastResult').catch(() => null);
          if (!lr || !lr.val) continue;
          const v = String(lr.val).toLowerCase();
          if (v.startsWith('local ok')) local++;
          else if (v.includes('cloud')) cloud++;
          else if (v.includes('no local ip')) { noIp++; noIpDevices.push(dev.name || id); }
          else if (v.includes('fail')) fail++;
        }
        const result = {
          local, cloud, fail, noIp,
          noIpDevices: noIpDevices.slice(0, 20),  // begrenzen damit der output nicht uebergeht
          totalDevices: this.devices.size
        };
        if (obj.callback) this.sendTo(obj.from, obj.command, result, obj.callback);
        return;
      }
      if (obj.command === 'getStats') {
        const stats = {
          pulsar:    this.pulsarStats || {},
          devices:   this.devices.size,
          cloudQuotaPaused: !!(this._cloudQuotaPausedUntil && Date.now() < this._cloudQuotaPausedUntil)
        };
        if (obj.callback) {
          this.sendTo(obj.from, obj.command, stats, obj.callback);
        }
        return;
      }
      if (obj.command === 'listDevicesRaw') {
        // Diagnose: was liefert Tuya direkt jetzt - hilft fehlende Devices zu finden
        try {
          const list = await this.cloud.listDevices();
          const summary = list.map(d => ({
            id: d.id,
            name: d.name,
            category: d.category,
            online: !!d.online,
            ip: d.ip || '',
            local_key: d.local_key ? '(' + d.local_key.length + 'chars)' : '',
            product_id: d.product_id || d.productKey || ''
          }));
          if (obj.callback) {
            this.sendTo(obj.from, obj.command, { count: list.length, devices: summary }, obj.callback);
          }
        } catch (e) {
          if (obj.callback) this.sendTo(obj.from, obj.command, { error: e.message }, obj.callback);
        }
        return;
      }
      if (obj.command === 'findMissingDevices') {
        // Vergleicht Cloud-Liste mit Adapter-Objekten - listet die die mal da
        // waren aber jetzt nicht mehr von Tuya geliefert werden
        try {
          const list = await this.cloud.listDevices();
          const cloudIds = new Set(list.map(d => d.id));
          const all = await this.getAdapterObjectsAsync().catch(() => null);
          const localIds = new Set();
          if (all) {
            const prefix = this.namespace + '.';
            for (const fullId of Object.keys(all)) {
              if (!fullId.startsWith(prefix)) continue;
              const obj = all[fullId];
              if (obj.type !== 'device') continue;
              const id = fullId.slice(prefix.length);
              if (id && !id.startsWith('info')) localIds.add(id);
            }
          }
          const missing = [];
          for (const id of localIds) {
            if (!cloudIds.has(id)) {
              // War mal da, ist nicht mehr in Cloud
              const nameObj = all[this.namespace + '.' + id];
              missing.push({ id: id, name: (nameObj && nameObj.common && nameObj.common.name) || id });
            }
          }
          const result = {
            cloudCount: list.length,
            localCount: localIds.size,
            missingCount: missing.length,
            missing: missing
          };
          this.log.info('findMissingDevices: ' + JSON.stringify(result));
          if (obj.callback) this.sendTo(obj.from, obj.command, result, obj.callback);
        } catch (e) {
          if (obj.callback) this.sendTo(obj.from, obj.command, { error: e.message }, obj.callback);
        }
        return;
      }
    } catch (e) {
      this.log.warn('onMessage: ' + (e.message || e));
      if (obj.callback) {
        this.sendTo(obj.from, obj.command, { error: e.message }, obj.callback);
      }
    }
  }

  /**
   * Holt fuer alle Devices die kein .ip haben die IP per Cloud-API.
   * Achtung: Rate-limited (1 Call/Sek). Bei Cloud-Quota-Pause bricht ab.
   * Setzt die IP in den .ip State plus dev.local.ip.
   */
  async importCloudIPs(maxDevices) {
    if (!this.cloud) return { error: 'cloud not ready' };
    if (this._cloudQuotaPausedUntil && Date.now() < this._cloudQuotaPausedUntil) {
      return { error: 'cloud quota paused', skipped: true };
    }
    let ok = 0, fail = 0, alreadySet = 0, withIp = 0, noIp = 0;
    const limit = Math.min(maxDevices || 999, 999);
    const candidates = [];
    for (const [id, dev] of this.devices) {
      if (dev.local && dev.local.ip) { alreadySet++; continue; }
      candidates.push([id, dev]);
    }
    this.log.info('importCloudIPs: starte Cloud-IP-Lookup fuer ' + Math.min(candidates.length, limit) + ' Devices (von ' + candidates.length + ' ohne IP) ...');

    let processed = 0;
    for (const [id, dev] of candidates) {
      if (processed >= limit) break;
      // Quota check
      if (this._cloudQuotaPausedUntil && Date.now() < this._cloudQuotaPausedUntil) {
        this.log.warn('importCloudIPs: Cloud-Quota erschoepft - Abbruch nach ' + processed);
        break;
      }
      processed++;
      try {
        const info = await this.cloud.getDeviceInfo(id);
        if (!info) { fail++; continue; }
        const ip = info.ip || info.local_ip || '';
        if (ip && tuyaLocal.isPrivateIp(ip)) {
          dev.local.ip = ip;
          await this.setStateAsync(id + '.ip', { val: ip, ack: true });
          await this.setStateAsync(id + '.localSource', { val: 'cloud-import', ack: true });
          ok++;
          withIp++;
          this.log.debug('importCloudIPs: ' + dev.name + ' -> ' + ip);
        } else {
          noIp++;
          this.log.debug('importCloudIPs: ' + dev.name + ' (' + id + ') keine ip in Cloud-Response');
        }
      } catch (e) {
        fail++;
        const msg = String(e && e.message || e).toLowerCase();
        if (msg.includes('quota') || msg.includes('exceeded') || msg.includes('rate limit')) {
          this._cloudQuotaPausedUntil = Date.now() + 60 * 60 * 1000;
          await this.setStateAsync('info.cloudQuotaPaused', { val: true, ack: true }).catch(() => {});
          this.log.warn('importCloudIPs: Cloud-Quota erschoepft, pausiere 60min - Abbruch nach ' + processed);
          break;
        }
        this.log.debug('importCloudIPs: ' + id + ' fail: ' + e.message);
      }
      // Rate-limit: 800ms pro Call
      await new Promise(r => setTimeout(r, 800));
    }
    const result = { ok, fail, alreadySet, withIp, noIp, processed, total: this.devices.size };
    this.log.info('importCloudIPs: fertig - ' + JSON.stringify(result));
    return result;
  }

  // -------------- Import lokaler IPs aus iobroker.tuya --------------

  async importLocalFromTuyaAdapter() {
    let n = 0;
    for (const [id, dev] of this.devices) {
      try {
        const ok = await this.importLocalFromTuyaAdapterOne(id, dev);
        if (ok) n++;
      } catch (e) { /* still */ }
    }
    if (n > 0) this.log.info('Local IPs aus tuya.0 importiert: ' + n + ' Geraete');
  }

  async importLocalFromTuyaAdapterOne(id, dev) {
    const base = 'tuya.0.' + id;
    // Object existence check
    const obj = await this.getForeignObjectAsync(base).catch(() => null);
    if (!obj) return false;

    // IP candidates
    let ip = '';
    for (const sub of ['ip', 'local_ip', 'device_ip']) {
      const s = await this.getForeignStateAsync(base + '.' + sub).catch(() => null);
      const v = s && s.val ? String(s.val).trim() : '';
      if (v && tuyaLocal.isPrivateIp(v)) { ip = v; break; }
    }
    // Native fallback
    if (!ip && obj.native) {
      const v = String(obj.native.ip || obj.native.localIp || '').trim();
      if (tuyaLocal.isPrivateIp(v)) ip = v;
    }
    if (!ip) return false;

    // Version (selten in States, oft in native)
    let version = '';
    for (const sub of ['version', 'protocol', 'ver']) {
      const s = await this.getForeignStateAsync(base + '.' + sub).catch(() => null);
      const v = s && s.val ? String(s.val).trim() : '';
      if (v) { version = v; break; }
    }
    if (!version && obj.native) version = String(obj.native.version || obj.native.ver || '').trim();

    // In Local-States schreiben (nicht ueberschreiben wenn schon was anderes drin)
    const ipState = await this.getStateAsync(id + '.ip').catch(() => null);
    if (!ipState || !ipState.val) {
      await this.setStateAsync(id + '.ip', { val: ip, ack: true });
      if (dev && dev.local) dev.local.ip = ip;
    }
    if (version) {
      const verState = await this.getStateAsync(id + '.localVersion').catch(() => null);
      const curVer = verState && verState.val ? String(verState.val) : '';
      // Schreiben wenn noch leer ODER wenn Tuya eine neuere Protocol-Version
      // angibt (z.B. nach OTA-Upgrade von 3.3 auf 3.5). Vorher wurde nur
      // einmalig geschrieben und nie ueberschrieben - das hat zu Mismatches
      // gefuehrt (curVer="3.3" obwohl Device inzwischen "3.5" ist).
      const newerProto = (version === '3.4' || version === '3.5') && curVer === '3.3';
      if (!curVer || newerProto) {
        await this.setStateAsync(id + '.localVersion', { val: version, ack: true });
        if (dev && dev.local) dev.local.version = version;
        if (newerProto) {
          this.log.info('Import: ' + (dev && dev.name) + ' localVersion ' + curVer + ' -> ' + version + ' (tuya.0 hat neuere Protocol-Version)');
        }
      }
    }
    await this.setStateAsync(id + '.localSource', { val: 'tuya.0', ack: true });
    return true;
  }
}

if (require.main !== module) {
  module.exports = (options) => new FiducerionSmartlife(options);
} else {
  new FiducerionSmartlife();
}
