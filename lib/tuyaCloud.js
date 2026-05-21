/* lib/tuyaCloud.js
 *
 * Tuya OpenAPI v2 Client - 1:1 aus dem TuyaCloudReplace v2.5.2-Skript portiert.
 * Macht raw HTTPS-Requests, kein npm-Dependency, kein Magic.
 *
 * Authentifizierung: HMAC-SHA256 per Doc
 *   https://developer.tuya.com/en/docs/cloud/0a30fc557f
 *
 * Token-Lifecycle: cloudEnsureToken() refresht automatisch wenn <30s vor Ablauf.
 */
'use strict';

const https  = require('https');
const crypto = require('crypto');

const REGION_HOSTS = {
  eu: 'openapi.tuyaeu.com',
  us: 'openapi.tuyaus.com',
  cn: 'openapi.tuyacn.com',
  in: 'openapi.tuyain.com'
};

function regionHost(region) {
  return REGION_HOSTS[region] || REGION_HOSTS.eu;
}

function sha256Hex(s) {
  return crypto.createHash('sha256').update(s || '').digest('hex');
}
function hmacHex(key, data) {
  return crypto.createHmac('sha256', key).update(data).digest('hex').toUpperCase();
}
function canonicalQuery(query) {
  if (!query) return '';
  const keys = Object.keys(query).sort();
  return keys.map(k => encodeURIComponent(k) + '=' + encodeURIComponent(String(query[k]))).join('&');
}
function stringToSign(method, path, query, body) {
  const bodyHash = sha256Hex(body ? JSON.stringify(body) : '');
  const qs = canonicalQuery(query || {});
  const url = qs ? path + '?' + qs : path;
  return method.toUpperCase() + '\n' + bodyHash + '\n\n' + url;
}

class TuyaCloud {
  /**
   * @param {object} opts
   * @param {string} opts.clientId
   * @param {string} opts.clientSecret
   * @param {string} opts.region          'eu' | 'us' | 'cn' | 'in'
   * @param {number} [opts.requestTimeoutMs]
   * @param {function} [opts.logger]      (level, msg) => void
   */
  constructor(opts) {
    this.clientId       = opts.clientId;
    this.clientSecret   = opts.clientSecret;
    this.region         = opts.region || 'eu';
    this.requestTimeoutMs = opts.requestTimeoutMs || 12000;
    this.logger         = opts.logger || (() => {});

    this.token   = null;
    this.tokenExp = 0;
  }

  /**
   * Macht den raw HTTP-Request mit Tuya-Signatur.
   */
  request(method, path, query, body, useToken) {
    return new Promise((resolve, reject) => {
      const qs = canonicalQuery(query || {});
      const fullPath = qs ? path + '?' + qs : path;
      const t = Date.now().toString();
      const signStr = stringToSign(method, path, query || {}, body);
      const tokenForSign = useToken ? (this.token || '') : '';
      const sign = hmacHex(
        this.clientSecret,
        this.clientId + tokenForSign + t + signStr
      );

      const opts = {
        hostname: regionHost(this.region),
        path: fullPath,
        method: method,
        headers: {
          client_id: this.clientId,
          sign_method: 'HMAC-SHA256',
          t: t,
          sign: sign,
          'Content-Type': 'application/json'
        }
      };
      if (useToken && this.token) opts.headers.access_token = this.token;

      let settled = false;
      let req;
      const done = (err, value) => {
        if (settled) return;
        settled = true;
        try { if (req) req.destroy(); } catch (e) {}
        if (err) reject(err); else resolve(value);
      };

      req = https.request(opts, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          let parsed;
          try { parsed = JSON.parse(data); }
          catch (e) { return done(new Error('HTTP ' + res.statusCode + ' / invalid JSON: ' + data.slice(0, 200))); }
          if (parsed.success === false) {
            const err = new Error(parsed.msg || parsed.message || data);
            err.code = parsed.code;
            err.tuyaResponse = parsed;
            return done(err);
          }
          done(null, parsed);
        });
        res.on('error', e => done(new Error(e.message || String(e))));
      });

      req.setTimeout(this.requestTimeoutMs, () => {
        done(new Error('Request timeout after ' + this.requestTimeoutMs + 'ms: ' + method + ' ' + path));
      });
      req.on('error', e => done(new Error(e.message || String(e))));

      if (body) {
        try { req.write(JSON.stringify(body)); }
        catch (e) { done(new Error('Request body error: ' + (e.message || String(e)))); return; }
      }
      req.end();
    });
  }

  async ensureToken() {
    const now = Date.now();
    if (this.token && now < this.tokenExp - 30000) return this.token;

    const res = await this.request('GET', '/v1.0/token', { grant_type: 1 }, null, false);
    const accessToken = res && res.result && res.result.access_token;
    const expireTime  = Number((res && res.result && res.result.expire_time) || 0);
    if (!accessToken) throw new Error('Kein access_token von Tuya erhalten');
    this.token   = accessToken;
    this.tokenExp = now + expireTime * 1000;
    return this.token;
  }

  /** Listet alle Geraete des Accounts (paginiert ueber has_more/last_row_key) */
  async listDevices() {
    await this.ensureToken();
    let hasMore = true, lastKey = '';
    let all = [];
    while (hasMore) {
      const q = { size: 100 };
      if (lastKey) q.last_row_key = lastKey;
      const res = await this.request('GET', '/v1.0/iot-01/associated-users/devices', q, null, true);
      const r = (res && res.result) || {};
      all = all.concat(r.devices || []);
      hasMore = !!r.has_more;
      lastKey = r.last_row_key || '';
    }
    return all;
  }

  /** Live-Status eines Geraets als Array von {code, value, ...} */
  async getStatus(deviceId) {
    await this.ensureToken();
    const res = await this.request('GET', '/v1.0/devices/' + deviceId + '/status', null, null, true);
    return (res && res.result) || [];
  }

  /** Specification (functions + status) - liefert null bei Fehler statt zu werfen */
  async getSpecification(deviceId) {
    try {
      await this.ensureToken();
      const res = await this.request('GET', '/v1.0/iot-03/devices/' + deviceId + '/specification', null, null, true);
      return (res && res.result) || null;
    } catch (e) {
      this.logger('debug', 'getSpecification ' + deviceId + ': ' + e.message);
      return null;
    }
  }

  /** Schreibt commands an ein Geraet. commands: [{code, value}, ...] */
  async sendCommands(deviceId, commands) {
    await this.ensureToken();
    await this.request('POST', '/v1.0/devices/' + deviceId + '/commands', null, { commands }, true);
  }
}

module.exports = TuyaCloud;
