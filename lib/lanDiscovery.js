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

// Tuya broadcasted v3.3-Pakete auf 6667 mit fixed Default-Key.
// WICHTIG: Der AES-Key ist NICHT der String 'yGAdlopoPVldABfn' direkt,
// sondern dessen MD5-Hash (16 Bytes Binary). Das ist seit Tuya 2018 so und
// in TuyAPI/iobroker.tuya genau dokumentiert.
const UDP_DEFAULT_KEY = crypto.createHash('md5').update('yGAdlopoPVldABfn', 'utf8').digest();

function isPrivateIp(ip) {
  const s = String(ip || '').trim();
  return /^10\./.test(s)
      || /^192\.168\./.test(s)
      || /^172\.(1[6-9]|2\d|3[0-1])\./.test(s);
}

/**
 * Decrypt Tuya UDP broadcast packet (v3.3+, port 6667/7000).
 *
 * Frame structure (TuyAPI-compatible):
 *   0x000055AA  prefix (4B)
 *   sequence    (4B)
 *   command     (4B)
 *   length      (4B)  - length of (retcode + payload + crc + suffix)
 *   [retcode]   (4B)  - sometimes present (broadcasts ohne)
 *   payload     (?B)  - AES-128-ECB encrypted, optional "3.3" prefix + 12 padding
 *   crc         (4B)
 *   suffix      (4B)  0x0000AA55
 *
 * @returns {string|null} decrypted JSON-string, or null on failure
 */
function tryDecryptUdp(buf) {
  if (buf.length < 24) return null;

  // Plausibility check: prefix should be 0x000055AA
  if (buf.readUInt32BE(0) !== 0x000055AA) {
    // Not a Tuya frame - skip
    return null;
  }
  // Suffix at end
  if (buf.readUInt32BE(buf.length - 4) !== 0x0000AA55) {
    return null;
  }

  // Length field at offset 12
  const lenField = buf.readUInt32BE(12);
  // Payload starts at 16, has length (lenField - 8) since lenField includes CRC(4) + suffix(4).
  // If retcode is present (cmd != BROADCAST), payload starts at 20 instead.
  // We try both offsets.
  const offsets = [16, 20];
  for (const off of offsets) {
    let body;
    try {
      body = buf.subarray(off, buf.length - 8);
    } catch (e) { continue; }
    if (body.length === 0) continue;
    // Manchmal hat Tuya bei v3.3 noch '3.3' + 12 padding bytes als prefix vor dem ciphertext
    // v3.4/v3.5 broadcasts mit AES-GCM koennen wir hier nicht entschluesseln -
    // ist OK, fid-smartlife erkennt die Version per Auto-Probe (siehe main.js).
    let cipher = body;
    if (body.length >= 15 && body.slice(0, 3).toString('utf8') === '3.3') {
      cipher = body.subarray(15);
    }
    if (cipher.length === 0 || cipher.length % 16 !== 0) continue;
    try {
      const decipher = crypto.createDecipheriv('aes-128-ecb', UDP_DEFAULT_KEY, null);
      decipher.setAutoPadding(true);
      const plain = Buffer.concat([decipher.update(cipher), decipher.final()]);
      const txt = plain.toString('utf8');
      if (txt.indexOf('{') >= 0 && txt.indexOf('}') > 0) {
        return txt;
      }
    } catch (e) { /* try next offset */ }
  }
  return null;
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
  const stats    = { rx: 0, plainOk: 0, decryptOk: 0, decryptFail: 0, records: 0 };

  // Stats periodisch loggen (Diagnose-Helfer)
  const statsTimer = setInterval(() => {
    if (stats.rx > 0) {
      logger('info', '[lan] Stats: rx=' + stats.rx + ' plainOk=' + stats.plainOk
        + ' decryptOk=' + stats.decryptOk + ' decryptFail=' + stats.decryptFail
        + ' records=' + stats.records);
    }
  }, 60000);

  for (const port of ports) {
    try {
      const sock = dgram.createSocket({ type: 'udp4', reuseAddr: true });
      sock.on('error', (err) => {
        logger('warn', 'LAN-Discovery Socket ' + port + ': ' + err.message);
      });
      sock.on('message', (msg, rinfo) => {
        stats.rx++;
        // Versuch 1: als Plain-UTF8 lesen
        let text = '';
        try { text = msg.toString('utf8'); } catch (e) {}
        let objs = tryExtractJsonObjects(text);
        if (objs.length > 0) stats.plainOk++;

        // Versuch 2: encrypted (v3.3 auf 6667 / 7000)
        if (objs.length === 0 && (port === 6667 || port === 7000)) {
          const dec = tryDecryptUdp(msg);
          if (dec) {
            const dObjs = tryExtractJsonObjects(dec);
            if (dObjs.length > 0) {
              stats.decryptOk++;
              for (const o of dObjs) objs.push(o);
            } else {
              stats.decryptFail++;
            }
          } else {
            stats.decryptFail++;
          }
        }

        for (const o of objs) {
          const rec = recordFromObj(o, rinfo.address, 'udp:' + port);
          if (rec) {
            stats.records++;
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
      clearInterval(statsTimer);
      for (const s of sockets) {
        try { s.close(); } catch (e) {}
      }
      sockets.length = 0;
    },
    getStats: () => Object.assign({}, stats)
  };
}

module.exports = { start, isPrivateIp };
