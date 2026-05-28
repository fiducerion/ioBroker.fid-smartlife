/* lib/tuyaLocal35.js
 *
 * Local Protocol Wrapper fuer Tuya v3.4 und v3.5.
 * Nutzt die TuyAPI-Library (npm: tuyapi) die diese neueren Protokolle bereits
 * unterstuetzt.
 *
 * Identische API-Signatur wie tuyaLocal.js (sendCommand, queryStatus):
 *   - sendCommand({ip, localKey, deviceId, dpsMap, version, port, timeoutMs})
 *     -> {ok: bool, reason: string}
 *   - queryStatus({ip, localKey, deviceId, version, port, timeoutMs})
 *     -> {ok: bool, dps?: object, reason?: string}
 *
 * Wir oeffnen pro Call eine neue TCP-Verbindung, machen den Crypto-Handshake,
 * senden den Command/Query, warten auf Antwort, schliessen wieder. Das ist
 * NICHT super-effizient bei vielen schnellen Calls (besser waere persistent),
 * aber konsistent mit dem v3.3-Verhalten und einfacher zu maintainen.
 *
 * Persistent-Sockets koennen in einer spaeteren Version nachgereicht werden.
 */
'use strict';

const DEFAULT_PORT = 6668;

/**
 * Sendet einen oder mehrere DPs an ein Device (v3.4 oder v3.5).
 * @returns {Promise<{ok: boolean, reason: string}>}
 */
async function sendCommand(opts) {
  const TuyAPI = require('tuyapi');
  const timeoutMs = Math.max(2000, Number(opts.timeoutMs) || 5000);
  let device = null;
  let asyncError = null;
  try {
    device = new TuyAPI({
      id:       opts.deviceId,
      key:      opts.localKey,
      ip:       opts.ip,
      port:     Number(opts.port) || DEFAULT_PORT,
      version:  String(opts.version),  // '3.4' oder '3.5'
      issueRefreshOnConnect: false
    });

    // KRITISCH: error-Handler MUSS direkt nach new TuyAPI() registriert werden
    // und NIE entfernt werden (auch nicht im finally-Block!) - sonst kommt der
    // 'Error from socket' als uncaughtException und killt den Adapter.
    // Late-Errors (z.B. timeout 10s nach disconnect()) muessen weiter gefangen
    // werden.
    device.on('error', (e) => { asyncError = e; /* swallow */ });

    // Connect-Timeout
    await Promise.race([
      device.connect(),
      new Promise((_, rej) => setTimeout(() => rej(new Error('connect timeout')), timeoutMs))
    ]);

    if (asyncError) throw asyncError;

    // Multi-DPS-Set
    const setOpts = (Object.keys(opts.dpsMap).length > 1)
      ? { multiple: true, data: opts.dpsMap }
      : { dps: Number(Object.keys(opts.dpsMap)[0]), set: opts.dpsMap[Object.keys(opts.dpsMap)[0]] };

    await Promise.race([
      device.set(setOpts),
      new Promise((_, rej) => setTimeout(() => rej(new Error('set timeout')), timeoutMs))
    ]);

    if (asyncError) throw asyncError;

    return { ok: true, reason: 'ok' };
  } catch (e) {
    return { ok: false, reason: (e && e.message) || String(e) };
  } finally {
    if (device) {
      // WICHTIG: KEIN removeAllListeners! Late-Errors koennen noch kommen.
      // disconnect ist trotzdem ok - wenn das Socket noch was sendet bevor
      // close klappt, faengt unser noch-aktiver error-Handler das ab.
      try { device.disconnect(); } catch (e) { /* ignore */ }
    }
  }
}

/**
 * Status-Query fuer v3.4/v3.5. Liefert die aktuellen DPs.
 * @returns {Promise<{ok: boolean, dps?: object, reason?: string}>}
 */
async function queryStatus(opts) {
  const TuyAPI = require('tuyapi');
  const timeoutMs = Math.max(2000, Number(opts.timeoutMs) || 5000);
  let device = null;
  let asyncError = null;
  try {
    device = new TuyAPI({
      id:       opts.deviceId,
      key:      opts.localKey,
      ip:       opts.ip,
      port:     Number(opts.port) || DEFAULT_PORT,
      version:  String(opts.version),
      issueRefreshOnConnect: false
    });

    // KRITISCH: siehe sendCommand-Kommentar. NIE entfernen, nie ueberschreiben.
    device.on('error', (e) => { asyncError = e; /* swallow */ });

    await Promise.race([
      device.connect(),
      new Promise((_, rej) => setTimeout(() => rej(new Error('connect timeout')), timeoutMs))
    ]);

    if (asyncError) throw asyncError;

    // get() ohne Args -> alle DPs
    const data = await Promise.race([
      device.get({ schema: true }),  // schema:true -> volles dps-Object
      new Promise((_, rej) => setTimeout(() => rej(new Error('get timeout')), timeoutMs))
    ]);

    if (asyncError) throw asyncError;

    // TuyAPI gibt {devId, dps} zurueck. Wir wollen nur dps.
    const dps = data && data.dps ? data.dps : data;

    if (!dps || typeof dps !== 'object') {
      return { ok: false, reason: 'no dps in response' };
    }

    return { ok: true, dps };
  } catch (e) {
    return { ok: false, reason: (e && e.message) || String(e) };
  } finally {
    if (device) {
      // KEIN removeAllListeners! Siehe sendCommand-Kommentar.
      try { device.disconnect(); } catch (e) { /* ignore */ }
    }
  }
}

module.exports = {
  DEFAULT_PORT,
  sendCommand,
  queryStatus
};
