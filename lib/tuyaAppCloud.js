/* lib/tuyaAppCloud.js  (v0.8.6)
 *
 * Tuya App Cloud Wrapper.
 * Login via Smart-Life-App-API mit Email+Passwort.
 *
 * Features:
 *   - Session-Persistenz: sid wird gecacht, kein Re-Login solange gueltig.
 *   - Online-Check via moduleMap.{mcu,wifi}.isOnline (das ist der echte
 *     Online-Indikator den Tuya in der App-API liefert).
 *   - getDeviceInfo(devId) liefert {online, dps, name, localKey, ...}.
 *
 * Session-Lifetime: typ. 30 Tage. Wir invalidieren auf API-Fehler (auth) und
 * re-loggen dann automatisch. Damit kommt nur SELTEN eine "neues Geraet"
 * Notification in der Smart Life App.
 */
'use strict';

// Default-Keys fuer die offizielle Smart-Life-App (EU-Region).
// Oeffentlich bekannt aus rospogrigio/localtuya issue #1188.
const DEFAULT_APP_KEYS = {
  key:      'ekmnwp9f5pnh3trdtpgy',
  secret:   'r3me7ghmxjevrvnpemwmhw3fxtacphyg',
  secret2:  'jfg5rs5kkmrj5mxahugvucrsvw43t48x',
  certSign: '0F:C3:61:99:9C:C0:C3:5B:A8:AC:A5:7D:AA:55:93:A2:0C:F5:57:27:70:2E:A8:5A:D7:B3:22:89:49:F8:88:FE'
};

const SESSION_MAX_AGE_MS = 29 * 24 * 60 * 60 * 1000;  // 29d, etwas weniger als 30d Tuya-Limit

class TuyaAppCloud {
  constructor(opts = {}) {
    this.email    = opts.email || '';
    this.password = opts.password || '';
    this.region   = (opts.region || 'EU').toUpperCase();
    this.keys     = opts.keys || DEFAULT_APP_KEYS;
    this.logger   = opts.logger || (() => {});
    this.sid          = opts.sid || null;
    this.lastLoginTs  = opts.lastLoginTs || 0;
    this.api          = null;
    this._lastFetchDevices = null;
    this._lastFetchTs      = 0;
  }

  _log(lvl, msg) { try { this.logger(lvl, '[appCloud] ' + msg); } catch (e) {} }

  _ensureApi() {
    if (this.api) return this.api;
    const Cloud = require('@tuyapi/cloud');
    this.api = new Cloud({
      key:           this.keys.key,
      secret:        this.keys.secret,
      secret2:       this.keys.secret2,
      certSign:      this.keys.certSign,
      apiEtVersion:  '0.0.1',
      region:        this.region
    });
    if (this.sid) this.api.sid = this.sid;
    return this.api;
  }

  async ensureLoggedIn(forceFresh = false) {
    if (!this.email || !this.password) throw new Error('appCloud: email/password fehlen');
    const now = Date.now();
    if (!forceFresh && this.sid && (now - this.lastLoginTs) < SESSION_MAX_AGE_MS) {
      this._log('debug', 'Session noch gueltig (Alter: ' + Math.round((now - this.lastLoginTs) / 3600000) + 'h)');
      this._ensureApi();
      return { sid: this.sid, sessionAge: now - this.lastLoginTs, fresh: false };
    }
    this.api = null;
    this._ensureApi();
    this._log('info', 'Frischer Login: ' + this.email + ' / region=' + this.region);
    const sid = await this.api.loginEx({ email: this.email, password: this.password });
    this.sid = sid;
    this.lastLoginTs = now;
    this._log('info', 'Login OK, sid=' + (typeof sid === 'string' ? sid.substring(0, 12) + '...' : '?'));
    return { sid, sessionAge: 0, fresh: true };
  }

  async _requestWithReloginGuard(req) {
    try {
      return await this.api.request(req);
    } catch (e) {
      const msg = String(e && e.message || e).toLowerCase();
      const isAuthErr = msg.includes('sid') || msg.includes('session') ||
                        msg.includes('login') || msg.includes('expired') ||
                        msg.includes('auth') || msg.includes('unauthor');
      if (!isAuthErr) throw e;
      this._log('warn', 'Session-expired, Re-Login: ' + e.message);
      this.sid = null;
      this.lastLoginTs = 0;
      await this.ensureLoggedIn(true);
      return await this.api.request(req);
    }
  }

  async listGroups() {
    await this.ensureLoggedIn();
    const groups = await this._requestWithReloginGuard({ action: 'tuya.m.location.list' });
    return Array.isArray(groups) ? groups : [];
  }

  async listDevicesInGroup(groupId) {
    await this.ensureLoggedIn();
    const devices = await this._requestWithReloginGuard({
      action: 'tuya.m.my.group.device.list',
      gid:    groupId
    });
    return Array.isArray(devices) ? devices : [];
  }

  async fetchAll() {
    const groups = await this.listGroups();
    const allDevices = [];
    for (const g of groups) {
      try {
        const devs = await this.listDevicesInGroup(g.groupId);
        for (const d of devs) allDevices.push({ ...d, groupId: g.groupId, groupName: g.name });
      } catch (e) { this._log('warn', 'Group ' + g.name + ': ' + e.message); }
    }
    this._lastFetchTs = Date.now();
    this._lastFetchDevices = new Map();
    for (const d of allDevices) this._lastFetchDevices.set(d.devId, d);
    return { groups, devices: allDevices, fetchedAt: this._lastFetchTs };
  }

  getDeviceInfo(devId) {
    if (!this._lastFetchDevices) return null;
    const d = this._lastFetchDevices.get(devId);
    if (!d) return null;
    return {
      name:      d.name,
      online:    this.isDeviceOnline(d),
      dps:       d.dps || {},
      localKey:  d.localKey || '',
      mac:       d.mac || '',
      productId: d.productId || '',
      category:  d.category || ''
    };
  }

  /**
   * Online-Check via moduleMap.wifi.isOnline (das ist der echte Online-Indikator).
   * Bei normalen WiFi-Devices: wifi.isOnline = ist Verbindung zur Tuya-Cloud da.
   * Bei BLE-Devices / Sub-Devices: nimmt ersten online-Modul.
   */
  isDeviceOnline(device) {
    if (!device || typeof device !== 'object') return false;
    const m = device.moduleMap;
    if (!m || typeof m !== 'object') return false;
    if (m.wifi && m.wifi.isOnline === true) return true;
    for (const k of Object.keys(m)) {
      if (m[k] && m[k].isOnline === true) return true;
    }
    return false;
  }

  getCachedStats() {
    if (!this._lastFetchDevices) return { total: 0, online: 0, offline: 0, fetchedAt: 0, ageSec: 0 };
    let online = 0, offline = 0;
    for (const d of this._lastFetchDevices.values()) {
      if (this.isDeviceOnline(d)) online++; else offline++;
    }
    return {
      total:     this._lastFetchDevices.size,
      online,
      offline,
      fetchedAt: this._lastFetchTs,
      ageSec:    Math.round((Date.now() - this._lastFetchTs) / 1000)
    };
  }
}

module.exports = { TuyaAppCloud, DEFAULT_APP_KEYS };
