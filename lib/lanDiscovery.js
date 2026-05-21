/* lib/lanDiscovery.js
 *
 * UDP-Broadcast-Listener fuer Tuya LAN-Discovery.
 *
 * Tuya-Geraete senden periodisch (alle ~5s) JSON-Pakete per UDP-Broadcast:
 *   - Port 6666: unencrypted v3.1 announce
 *   - Port 6667: encrypted v3.3 announce (mit AES-128-ECB-Default-Key)
 *   - Port 7000: alternative
 *
 * Jedes Paket enthaelt mindestens gwId/devId, ip, version.
 *
 * Wir parsen sowohl direkten JSON als auch JSON-Snippets in Binary-Frames
 * (matchen via Regex {...}-Bloecke). Encrypted v3.3-Pakete koennen wir
 * dekrypten (Default-Key 'yGAdlopoPVldABfn'), aber das ist optional - in
 * unserem Setup haben wir die IP normalerweise schon via Cloud-Sync.
 *
 * Aus dem TuyaCloudReplace v2.5.2-Skript portiert.
 */
'use strict';

const dgram  = require('dgram');
const crypto = require('crypto');

const DEFAULT_PORTS = [6666, 6667, 7000];

// Tuya broadcasted v3.3-Pakete auf 6667 mit fixed Default-Key
const UDP_DEFAULT_KEY = Buffer.from('yGAdlopoPVldABfn', 'utf8');

function isPrivateIp(ip) {
  const s = String(ip || '').trim();
  return /^10\./.test(s)
      || /^192\.168\./.test(s)
      || /^172\.(1[6-9]|2\d|3[0-1])\./.test(s);
}

function tryDecryptUdp(buf) {
  // Versuch: AES-128-ECB mit dem Tuya Default-UDP-Key
  try {
    if (buf.length < 24) return null;
    // Header skip wie Tuya: prefix(4) seq(4) cmd(4) length(4) -> Rest ist Payload+CRC+Suffix
    // Bei manchen v3.3 ist nach 16 Bytes Header direkt der Cipher (kein '3.3'+12 davor).
    const body = buf.subarray(20, buf.length - 8);
    if (body.length === 0 || body.length % 16 !== 0) return null;
    const decipher = crypto.createDecipheriv('aes-128-ecb', UDP_DEFAULT_KEY, null);
    decipher.setAutoPadding(true);
    const plain = Buffer.concat([decipher.update(body), decipher.final()]);
    return plain.toString('utf8');
  } catch (e) {
    return null;
  }
}

function tryExtractJsonObjects(text) {
  const out = [];
  const trimmed = String(text).trim();
  try {
    const direct = JSON.parse(trimmed);
    if (direct && typeof direct === 'object') out.push(direct);
  } catch (e) { /* not json */ }
  const matches = String(text).match(/\{[\s\S]*?\}/g) || [];
  for (const m of matches) {
    try { const p = JSON.parse(m); if (p && typeof p === 'object') out.push(p); }
    catch (e) {}
  }
  return out;
}

function recordFromObj(obj, fallbackIp, source) {
  if (!obj || typeof obj !== 'object') return null;
  const id = String(obj.gwId || obj.devId || obj.deviceId || obj.id || '').trim();
  if (!id) return null;
  const ip = String(obj.ip || obj.ipaddr || fallbackIp || '').trim();
  if (!ip || !isPrivateIp(ip)) return null;
  const version = String(obj.version || obj.ver || obj.protocol || '').trim() || undefined;
  return { id, ip, version, ts: Date.now(), source, raw: obj };
}

/**
 * @param {object} opts
 * @param {function} opts.onRecord  (rec) => void   wird pro discovered Geraet aufgerufen
 * @param {function} opts.logger    (level, msg) => void
 * @param {number[]} [opts.ports]   default [6666, 6667, 7000]
 */
function start(opts) {
  const onRecord = opts.onRecord || (() => {});
  const logger   = opts.logger   || (() => {});
  const ports    = opts.ports    || DEFAULT_PORTS;
  const sockets  = [];

  for (const port of ports) {
    try {
      const sock = dgram.createSocket({ type: 'udp4', reuseAddr: true });
      sock.on('error', (err) => {
        logger('warn', 'LAN-Discovery Socket ' + port + ': ' + err.message);
      });
      sock.on('message', (msg, rinfo) => {
        // Versuch 1: als Plain-UTF8 lesen
        let text = '';
        try { text = msg.toString('utf8'); } catch (e) {}
        const objs = tryExtractJsonObjects(text);

        // Versuch 2: encrypted (v3.3 auf 6667 / 7000)
        if (objs.length === 0 && (port === 6667 || port === 7000)) {
          const dec = tryDecryptUdp(msg);
          if (dec) {
            const dObjs = tryExtractJsonObjects(dec);
            for (const o of dObjs) objs.push(o);
          }
        }

        for (const o of objs) {
          const rec = recordFromObj(o, rinfo.address, 'udp:' + port);
          if (rec) {
            try { onRecord(rec); }
            catch (e) { logger('debug', 'onRecord handler: ' + e.message); }
          }
        }
      });
      sock.bind(port, () => {
        try { sock.setBroadcast(true); } catch (e) {}
        logger('info', 'LAN-Discovery aktiv auf UDP ' + port);
      });
      sockets.push(sock);
    } catch (e) {
      logger('warn', 'LAN-Discovery bind UDP ' + port + ': ' + e.message);
    }
  }

  return {
    stop: () => {
      for (const s of sockets) {
        try { s.close(); } catch (e) {}
      }
      sockets.length = 0;
    }
  };
}

module.exports = { start, isPrivateIp };
