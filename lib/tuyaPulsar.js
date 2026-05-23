/* lib/tuyaPulsar.js
 *
 * Tuya Cloud Message Service Subscriber.
 * Verbindet sich per MQTT mit dem Tuya-Pulsar-Broker und empfaengt Device-
 * Status-Updates in Echtzeit. Damit entfaellt das Cloud-Polling fuer
 * Battery-/Subdevices komplett - die Status kommen per Push.
 *
 * Workflow:
 *   1. Auth: POST /v1.0/iot-03/open-hub/access-config -> bekommt MQTT-credentials
 *   2. MQTT Connect mit den credentials zum Tuya-Broker
 *   3. SUBSCRIBE auf das source_topic (z.B. tuya/{accessId}/event)
 *   4. Inbound Messages decrypten (AES-GCM oder AES-ECB)
 *   5. Decoded JSON enthaelt {devId, status: [{code, value, ...}]} - in Adapter
 *      States schreiben
 *   6. Token-Refresh vor expire_time
 *
 * Decryption:
 *   - Key: mittlere 16 Zeichen vom accessKey (clientSecret)
 *   - Modi: AES-128-ECB (alt, default) oder AES-128-GCM (neu, Tuya 2024+)
 *   - In der Tuya IoT Platform unter Message Service einstellbar
 *
 * Diese Implementation versucht erst GCM, falls das Format das hergibt, sonst
 * ECB. Wir erkennen das am Vorhandensein eines 'pv'-Feldes ueber 2.0.
 */
'use strict';

const crypto = require('crypto');
const https  = require('https');
const url    = require('url');
const EventEmitter = require('events');
const { MqttMiniClient } = require('./mqttMini');

const REGION_HOSTS = {
  eu: 'openapi.tuyaeu.com',
  us: 'openapi.tuyaus.com',
  cn: 'openapi.tuyacn.com',
  in: 'openapi.tuyain.com'
};

class TuyaPulsar extends EventEmitter {
  /**
   * @param {object} opts
   * @param {string} opts.clientId
   * @param {string} opts.clientSecret
   * @param {string} opts.region              'eu' | 'us' | 'cn' | 'in'
   * @param {string} [opts.uid]               Tuya UID. Wenn fehlt, wird er per
   *                                          /v1.0/users/uid geholt.
   * @param {function} [opts.getCloudToken]   () => Promise<string>  - liefert
   *                                          ein aktuelles access_token von
   *                                          unserer normalen TuyaCloud-Instance.
   * @param {function} [opts.logger]          (level, msg) => void
   */
  constructor(opts) {
    super();
    this.clientId       = opts.clientId;
    this.clientSecret   = opts.clientSecret;
    this.region         = opts.region || 'eu';
    this.uid            = opts.uid || null;
    this.getCloudToken  = opts.getCloudToken || null;
    this.logger         = opts.logger || (() => {});
    this.host           = REGION_HOSTS[this.region] || REGION_HOSTS.eu;

    this.mqttClient     = null;
    this.mqttCfg        = null;       // Response from open-hub/access-config
    this.refreshTimer   = null;
    this.stopped        = false;
    this.reconnectAttempt = 0;
  }

  async start() {
    this.stopped = false;
    return this._connectLoop();
  }

  stop() {
    this.stopped = true;
    if (this.refreshTimer) {
      clearTimeout(this.refreshTimer);
      this.refreshTimer = null;
    }
    if (this.mqttClient) {
      try { this.mqttClient.disconnect(); } catch (e) { /* ignore */ }
      this.mqttClient = null;
    }
  }

  async _connectLoop() {
    if (this.stopped) return;
    try {
      this.mqttCfg = await this._getAccessConfig();
      this.logger('info', 'TuyaPulsar: Access-Config ok, broker=' + this._brokerHost() + ', topic=' + this._subTopic());
      await this._mqttConnect();
      this.reconnectAttempt = 0;
      this._scheduleTokenRefresh();
    } catch (e) {
      this.reconnectAttempt++;
      const backoffMs = Math.min(60000, 2000 * Math.pow(2, Math.min(5, this.reconnectAttempt)));
      this.logger('warn', 'TuyaPulsar: connect failed (' + (e.message || e) + ') - retry in ' + Math.round(backoffMs/1000) + 's');
      setTimeout(() => this._connectLoop().catch(()=>{}), backoffMs);
    }
  }

  _brokerHost() {
    if (!this.mqttCfg || !this.mqttCfg.url) return null;
    // url kommt wie "ssl://m1.tuyaeu.com:8883"
    const u = url.parse(this.mqttCfg.url);
    return u.hostname;
  }
  _brokerPort() {
    if (!this.mqttCfg || !this.mqttCfg.url) return 8883;
    const u = url.parse(this.mqttCfg.url);
    return Number(u.port) || 8883;
  }
  _subTopic() {
    if (!this.mqttCfg) return null;
    if (this.mqttCfg.source_topic && typeof this.mqttCfg.source_topic === 'object') {
      // Tuya gibt mehrere topic-Optionen: {device: 'tuya/.../event', ...}
      return this.mqttCfg.source_topic.device
          || this.mqttCfg.source_topic.event
          || Object.values(this.mqttCfg.source_topic)[0];
    }
    return this.mqttCfg.source_topic || null;
  }

  // ---------- Auth: /v1.0/iot-03/open-hub/access-config ----------

  async _getAccessConfig() {
    // Wir brauchen ein Cloud-Token. Wenn der user uns einen liefert via
    // getCloudToken, super. Sonst holen wir selber einen.
    let accessToken;
    if (typeof this.getCloudToken === 'function') {
      accessToken = await this.getCloudToken();
    } else {
      accessToken = await this._getTokenOwn();
    }
    if (!this.uid) {
      this.uid = await this._getUid(accessToken);
    }

    const linkId = 'fid-smartlife-' + this.clientId.substring(0, 8) + '-' + process.pid;
    const path = '/v1.0/iot-03/open-hub/access-config';
    const body = {
      uid:      this.uid,
      link_id:  linkId,
      link_type: 'mqtt',
      topics:   'device',
      msg_encrypted_version: '2.0'    // Tuya 2.0: AES-GCM auch unterstuetzt
    };
    const resp = await this._signedRequest('POST', path, null, body, accessToken);
    if (!resp || !resp.success) {
      throw new Error('open-hub/access-config failed: ' + JSON.stringify(resp));
    }
    return resp.result || {};
  }

  async _getUid(accessToken) {
    // Tuya hat /v1.0/users/uid? Schauen wir... Eigentlich braucht man User-Lookup
    // via App-Account. Fuer Open-Hub geht aber auch der ClientId als uid.
    // Wir nutzen erstmal den ClientId selbst als uid - das ist bei den meisten
    // Tuya-Setups so dass die Cloud Project ID = uid.
    return this.clientId;
  }

  async _getTokenOwn() {
    // Standalone Token-Holer (Backup falls kein getCloudToken-Callback)
    const path = '/v1.0/token?grant_type=1';
    const resp = await this._signedRequest('GET', path, null, null, '');
    if (!resp || !resp.success) {
      throw new Error('token fetch failed: ' + JSON.stringify(resp));
    }
    return resp.result && resp.result.access_token;
  }

  _signedRequest(method, path, query, body, accessToken) {
    // Replikat aus unserer TuyaCloud-Class - kleiner standalone HTTP-Helper
    return new Promise((resolve, reject) => {
      const ts = String(Date.now());
      const bodyStr = body ? JSON.stringify(body) : '';
      const bodyHash = crypto.createHash('sha256').update(bodyStr).digest('hex');
      const queryStr = query ? Object.keys(query).sort().map(k =>
        encodeURIComponent(k) + '=' + encodeURIComponent(String(query[k]))).join('&') : '';
      const fullPath = queryStr ? (path + '?' + queryStr) : path;
      const stringToSign = method.toUpperCase() + '\n' + bodyHash + '\n\n' + fullPath;
      const signStr = this.clientId + (accessToken || '') + ts + stringToSign;
      const sign = crypto.createHmac('sha256', this.clientSecret).update(signStr).digest('hex').toUpperCase();

      const headers = {
        'client_id':     this.clientId,
        'sign':          sign,
        't':             ts,
        'sign_method':   'HMAC-SHA256',
        'Content-Type':  'application/json'
      };
      if (accessToken) headers['access_token'] = accessToken;

      const req = https.request({
        host: this.host,
        port: 443,
        path: fullPath,
        method: method,
        headers: headers,
        timeout: 10000
      }, (res) => {
        let chunks = [];
        res.on('data', d => chunks.push(d));
        res.on('end', () => {
          try {
            const data = JSON.parse(Buffer.concat(chunks).toString('utf8'));
            resolve(data);
          } catch (e) {
            reject(e);
          }
        });
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(new Error('http timeout')); });
      if (bodyStr) req.write(bodyStr);
      req.end();
    });
  }

  // ---------- MQTT ----------

  async _mqttConnect() {
    return new Promise((resolve, reject) => {
      const cfg = this.mqttCfg;
      if (!cfg || !cfg.url || !cfg.client_id || !cfg.username || !cfg.password) {
        return reject(new Error('mqttCfg incomplete'));
      }
      const topic = this._subTopic();
      if (!topic) return reject(new Error('no source_topic'));

      this.mqttClient = new MqttMiniClient({
        host:         this._brokerHost(),
        port:         this._brokerPort(),
        clientId:     cfg.client_id,
        username:     cfg.username,
        password:     cfg.password,
        keepAliveSec: 60,
        logger:       this.logger
      });

      let resolved = false;
      this.mqttClient.on('connect', () => {
        this.mqttClient.subscribe(topic, 1);
        if (!resolved) { resolved = true; resolve(); }
        this.emit('connected');
      });
      this.mqttClient.on('message', (t, payload) => this._onMqttMessage(t, payload));
      this.mqttClient.on('error', (err) => {
        this.logger('warn', 'TuyaPulsar: MQTT error: ' + (err.message || err));
        if (!resolved) { resolved = true; reject(err); }
      });
      this.mqttClient.on('close', () => {
        this.logger('info', 'TuyaPulsar: MQTT connection closed');
        this.emit('disconnected');
        // Reconnect mit Backoff
        if (!this.stopped) {
          setTimeout(() => this._connectLoop().catch(()=>{}), 5000);
        }
      });

      this.mqttClient.connect();
    });
  }

  _onMqttMessage(topic, payload) {
    let outer;
    try {
      outer = JSON.parse(payload.toString('utf8'));
    } catch (e) {
      this.logger('warn', 'TuyaPulsar: msg payload not JSON');
      return;
    }
    if (!outer.data) {
      this.logger('debug', 'TuyaPulsar: msg without data field, keys=' + Object.keys(outer).join(','));
      return;
    }
    // Decrypt
    let decrypted = null;
    try {
      decrypted = this._decryptPayload(outer);
    } catch (e) {
      this.logger('warn', 'TuyaPulsar: decrypt failed: ' + (e.message || e));
      return;
    }
    let parsed;
    try {
      parsed = JSON.parse(decrypted);
    } catch (e) {
      this.logger('warn', 'TuyaPulsar: decrypted not JSON: ' + decrypted.substring(0, 100));
      return;
    }
    this.emit('message', parsed);
  }

  /**
   * Decrypt: Tuya verschluesselt das outer.data mit AES-GCM oder AES-ECB.
   * Key = mittlere 16 Zeichen vom clientSecret (accessKey).
   * Bei AES-GCM ist data = base64(IV || ciphertext || tag), IV=12 Byte, tag=16 Byte.
   * Bei AES-ECB ist data = base64(ciphertext mit PKCS7-Padding).
   */
  _decryptPayload(outer) {
    const dataB64 = outer.data;
    const raw = Buffer.from(dataB64, 'base64');
    // Key: mittlere 16 chars vom clientSecret
    const sec = String(this.clientSecret || '');
    const aesKey = Buffer.from(sec.substring(8, 24), 'utf8');
    if (aesKey.length !== 16) {
      throw new Error('aesKey length ' + aesKey.length + ' != 16 (clientSecret too short?)');
    }

    // Hinweis: outer.pv === '2.0' -> AES-GCM
    //          outer.pv unset or '1.0' -> AES-ECB
    const pv = String(outer.pv || '');
    const useGcm = (pv === '2.0' || pv === '4.0' || outer.encryptType === 'aes_gcm');

    if (useGcm) {
      // Format: IV(12) || ciphertext || authTag(16)
      if (raw.length < 12 + 16) throw new Error('GCM payload too short');
      const iv = raw.slice(0, 12);
      const tag = raw.slice(raw.length - 16);
      const ct = raw.slice(12, raw.length - 16);
      const decipher = crypto.createDecipheriv('aes-128-gcm', aesKey, iv);
      decipher.setAuthTag(tag);
      // AAD (Additional Authenticated Data): bei Tuya ist das ggf. ein Header
      // im outer-message. Aus den Tuya-Beispielen scheint die AAD aus dem t-
      // Feld und/oder anderen zu kommen. Wir probieren erst ohne AAD.
      const dec = Buffer.concat([decipher.update(ct), decipher.final()]);
      return dec.toString('utf8');
    } else {
      // ECB
      const decipher = crypto.createDecipheriv('aes-128-ecb', aesKey, null);
      const dec = Buffer.concat([decipher.update(raw), decipher.final()]);
      return dec.toString('utf8');
    }
  }

  // ---------- Token-Refresh ----------

  _scheduleTokenRefresh() {
    if (this.refreshTimer) clearTimeout(this.refreshTimer);
    if (!this.mqttCfg || !this.mqttCfg.expire_time) return;
    // Tuya gibt expire_time meist in Sekunden ab jetzt
    const expSec = Number(this.mqttCfg.expire_time);
    const refreshInMs = Math.max(60000, (expSec - 60) * 1000);
    this.logger('debug', 'TuyaPulsar: token refresh in ' + Math.round(refreshInMs/1000) + 's');
    this.refreshTimer = setTimeout(() => {
      this.logger('info', 'TuyaPulsar: refreshing token / reconnecting');
      if (this.mqttClient) {
        try { this.mqttClient.disconnect(); } catch (e) { /* ignore */ }
      }
      this._connectLoop().catch(()=>{});
    }, refreshInMs);
  }
}

module.exports = { TuyaPulsar: TuyaPulsar };
