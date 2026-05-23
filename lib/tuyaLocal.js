/* lib/tuyaLocal.js
 *
 * Tuya Local Protocol v3.3 Sender.
 *
 * Basiert 1:1 auf dem TuyaCloudReplace v2.5.2-Skript-Code von Bernd, aber:
 *  - echtes CRC32 (zlib-konform) statt 4 Null-Bytes (manche Geraete pruefen CRC)
 *  - sauber als Module gekapselt
 *  - mit Timeout + Auto-Disconnect nach Response
 *
 * Was funktioniert:
 *   - Single-Command "Set DPS" (cmd 7)
 *   - Status-Read via DP_QUERY (cmd 10) - v0.6.3 (NEU)
 *   - AES-128-ECB Encryption mit Geraete-localKey
 *   - Private-IP Check
 *
 * Was (noch) nicht:
 *   - Protokoll 3.1 (anderes Padding) und 3.4 (HMAC-SHA256 statt CRC32)
 *   - Persistente TCP-Verbindung
 */
'use strict';

const crypto = require('crypto');
const net    = require('net');

const DEFAULT_PORT       = 6668;
const DEFAULT_TIMEOUT_MS = 2500;

// Tuya benutzt den klassischen zlib-CRC32 (Polynomial 0xEDB88320, reflected).
const CRC32_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let j = 0; j < 8; j++) {
      c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
    }
    t[i] = c;
  }
  return t;
})();
function crc32(buf) {
  let c = 0xFFFFFFFF;
  for (let i = 0; i < buf.length; i++) {
    c = CRC32_TABLE[(c ^ buf[i]) & 0xFF] ^ (c >>> 8);
  }
  return (c ^ 0xFFFFFFFF) >>> 0;
}

function isPrivateIp(ip) {
  const s = String(ip || '').trim();
  return /^10\./.test(s)
      || /^192\.168\./.test(s)
      || /^172\.(1[6-9]|2\d|3[0-1])\./.test(s);
}

function ensureAesKey16(key) {
  const b = Buffer.from(String(key || ''), 'utf8');
  if (b.length === 16) return b;
  if (b.length > 16)   return b.subarray(0, 16);
  const out = Buffer.alloc(16, 0);
  b.copy(out);
  return out;
}

function aesEncryptEcb(data, key) {
  const cipher = crypto.createCipheriv('aes-128-ecb', ensureAesKey16(key), null);
  cipher.setAutoPadding(true);
  return Buffer.concat([cipher.update(data, 'utf8'), cipher.final()]);
}

function aesDecryptEcb(data, key) {
  const dec = crypto.createDecipheriv('aes-128-ecb', ensureAesKey16(key), null);
  dec.setAutoPadding(true);
  return Buffer.concat([dec.update(data), dec.final()]);
}

/**
 * Baut ein DP_QUERY-Paket (cmd 0x0a) fuer Tuya v3.3 - liefert ALLE DPs zurueck.
 * Body ist nur {gwId, devId, t, dps:{}, uid:''}.
 */
function packQuery33(devId, localKey, seq) {
  const nowS = Math.floor(Date.now() / 1000);
  const payloadJson = JSON.stringify({
    gwId:  devId,
    devId: devId,
    uid:   '',
    t:     String(nowS)
  });
  const encrypted = aesEncryptEcb(payloadJson, localKey);

  // DP_QUERY hat KEINEN '3.3' Version-Header vor dem Cipher
  // (im Gegensatz zu SET cmd 7)
  const innerPayload = encrypted;

  const PREFIX = 0x000055aa;
  const SUFFIX = 0x0000aa55;
  const CMD_DP_QUERY = 0x0a;
  const seqN = (typeof seq === 'number') ? seq : 0;

  const totalPayloadLen = innerPayload.length + 4 + 4;

  const header = Buffer.alloc(16);
  header.writeUInt32BE(PREFIX,           0);
  header.writeUInt32BE(seqN,             4);
  header.writeUInt32BE(CMD_DP_QUERY,     8);
  header.writeUInt32BE(totalPayloadLen, 12);

  const crcBase = Buffer.concat([header, innerPayload]);
  const crcVal  = crc32(crcBase);
  const crcBuf  = Buffer.alloc(4);
  crcBuf.writeUInt32BE(crcVal, 0);
  const suffixBuf = Buffer.alloc(4);
  suffixBuf.writeUInt32BE(SUFFIX, 0);

  return Buffer.concat([header, innerPayload, crcBuf, suffixBuf]);
}

/**
 * Parsed eine empfangene Response. Format:
 *   prefix(4) seq(4) cmd(4) length(4) returnCode(4) [versionHeader(15)] payload crc(4) suffix(4)
 *
 * Returnt {ok, dps} - dps = { '1': false, '2': 20, ... } oder leer wenn nicht parsebar.
 */
function parseResponse33(buf, localKey) {
  if (!buf || buf.length < 20) return { ok: false, reason: 'too short' };
  const PREFIX = 0x000055aa;
  const SUFFIX = 0x0000aa55;
  const prefix = buf.readUInt32BE(0);
  if (prefix !== PREFIX) return { ok: false, reason: 'bad prefix' };
  const len = buf.readUInt32BE(12);
  // Total packet = 16 (header) + len  (len enthaelt returnCode + payload + crc + suffix)
  const total = 16 + len;
  if (buf.length < total) return { ok: false, reason: 'truncated' };

  const returnCode = buf.readUInt32BE(16);
  // payload: bytes 20 bis 20+len-4-4-4 (also -returnCode -crc -suffix)
  let payloadStart = 20;
  let payloadEnd   = 16 + len - 8;   // -4 crc -4 suffix
  if (returnCode === 0) {
    // success - kein versionHeader davor (bei DP_QUERY response)
  } else {
    // bei manchen Responses ist returnCode nicht 0 (z.B. 1 = error)
    // Wir versuchen trotzdem den Payload zu lesen
  }

  // Payload kann mit '3.3' beginnen + 12 Reserved Bytes (status response)
  let raw = buf.subarray(payloadStart, payloadEnd);
  if (raw.length > 15 && raw.subarray(0, 3).toString('utf8') === '3.3') {
    raw = raw.subarray(15);
  }

  // Decrypt
  let decrypted;
  try {
    decrypted = aesDecryptEcb(raw, localKey).toString('utf8');
  } catch (e) {
    // Manchmal ist es plain JSON (kein Encrypt) bei error responses
    try {
      const txt = raw.toString('utf8');
      const j = JSON.parse(txt);
      return { ok: false, reason: 'plain error: ' + JSON.stringify(j) };
    } catch (e2) {
      return { ok: false, reason: 'decrypt failed: ' + e.message };
    }
  }

  try {
    const obj = JSON.parse(decrypted);
    if (obj && typeof obj === 'object' && obj.dps && typeof obj.dps === 'object') {
      return { ok: true, dps: obj.dps };
    }
    return { ok: false, reason: 'no dps in response', raw: obj };
  } catch (e) {
    return { ok: false, reason: 'json parse: ' + e.message };
  }
}

/**
 * Baut ein vollstaendiges Tuya v3.3 SET-Paket:
 *   prefix(4)  seq(4)  cmd(4)  length(4)  '3.3'(3)  reserved(12)  encrypted  crc(4)  suffix(4)
 *
 * @param {string} devId
 * @param {string} localKey
 * @param {object} dpsMap  { '1': true, '2': 50, ... }
 * @param {number} [seq]   optional sequence number
 */
function pack33(devId, localKey, dpsMap, seq) {
  const nowS = Math.floor(Date.now() / 1000);
  const payloadJson = JSON.stringify({
    devId: devId,
    gwId:  devId,
    uid:   '',
    t:     String(nowS),
    dps:   dpsMap
  });

  const encrypted = aesEncryptEcb(payloadJson, localKey);

  // Fuer SET (cmd 7) bei version 3.3: '3.3' + 12 Reserved-Bytes vor dem Cipher.
  // Diese 15 Bytes liegen UNVERSCHLUESSELT vor dem Cipher.
  const versionHeader = Buffer.concat([
    Buffer.from('3.3', 'utf8'),
    Buffer.alloc(12, 0)
  ]);
  const innerPayload = Buffer.concat([versionHeader, encrypted]);

  // Outer-Header
  const PREFIX = 0x000055aa;
  const SUFFIX = 0x0000aa55;
  const CMD_SET = 0x07;
  const seqN = (typeof seq === 'number') ? seq : 0;

  const totalPayloadLen = innerPayload.length + 4 /*crc*/ + 4 /*suffix*/;

  const header = Buffer.alloc(16);
  header.writeUInt32BE(PREFIX,           0);
  header.writeUInt32BE(seqN,             4);
  header.writeUInt32BE(CMD_SET,          8);
  header.writeUInt32BE(totalPayloadLen, 12);

  // CRC wird ueber header + innerPayload berechnet
  const crcBase = Buffer.concat([header, innerPayload]);
  const crcVal  = crc32(crcBase);
  const crcBuf  = Buffer.alloc(4);
  crcBuf.writeUInt32BE(crcVal, 0);

  const suffixBuf = Buffer.alloc(4);
  suffixBuf.writeUInt32BE(SUFFIX, 0);

  return Buffer.concat([header, innerPayload, crcBuf, suffixBuf]);
}

/**
 * Schickt ein SET-Paket per TCP an das Geraet, wartet auf Response oder Timeout.
 *
 * @param {object} opts
 * @param {string} opts.ip
 * @param {string} opts.localKey
 * @param {string} opts.deviceId
 * @param {object} opts.dpsMap          { dpId-String: value }
 * @param {string} [opts.version]       nur '3.3' supported in v0.3.0
 * @param {number} [opts.port]          default 6668
 * @param {number} [opts.timeoutMs]     default 2500
 * @returns {Promise<{ok: boolean, reason: string}>}
 */
function sendCommand(opts) {
  return new Promise((resolve) => {
    const ip       = String(opts.ip || '').trim();
    const key      = String(opts.localKey || '').trim();
    const version  = String(opts.version || '3.3').trim();
    const port     = Number(opts.port || DEFAULT_PORT);
    const timeout  = Number(opts.timeoutMs || DEFAULT_TIMEOUT_MS);

    if (!ip || !key) {
      return resolve({ ok: false, reason: 'missing ip or localKey' });
    }
    if (!isPrivateIp(ip)) {
      return resolve({ ok: false, reason: 'ip not private/local' });
    }
    if (version !== '3.3') {
      return resolve({ ok: false, reason: 'protocol ' + version + ' not supported (only 3.3)' });
    }
    if (!opts.dpsMap || typeof opts.dpsMap !== 'object' || !Object.keys(opts.dpsMap).length) {
      return resolve({ ok: false, reason: 'empty dpsMap' });
    }

    const packet = pack33(opts.deviceId, key, opts.dpsMap);

    let settled = false;
    const socket = new net.Socket();
    const done = (ok, reason) => {
      if (settled) return;
      settled = true;
      try { socket.destroy(); } catch (e) {}
      resolve({ ok: !!ok, reason: reason });
    };

    try {
      socket.setTimeout(timeout);
      socket.connect(port, ip, () => {
        try { socket.write(packet); }
        catch (e) { done(false, 'write error: ' + e.message); }
      });
      socket.on('data', () => { done(true, 'ok'); });
      socket.on('timeout', () => { done(false, 'timeout'); });
      socket.on('error', (e) => { done(false, 'error: ' + e.message); });
      socket.on('close', () => { if (!settled) done(true, 'closed'); });
    } catch (e) {
      done(false, 'exception: ' + e.message);
    }
  });
}

/**
 * Holt den DP-Status eines Geraets lokal via TCP DP_QUERY (cmd 0x0a).
 * Liefert {ok, dps, reason} - dps = { '1': false, '2': 20, ... } bei Erfolg.
 *
 * @param {object} opts
 * @param {string} opts.ip
 * @param {string} opts.localKey
 * @param {string} opts.deviceId
 * @param {string} [opts.version]     default '3.3'
 * @param {number} [opts.port]        default 6668
 * @param {number} [opts.timeoutMs]   default 2500
 * @returns {Promise<{ok: boolean, dps?: object, reason?: string}>}
 */
function queryStatus(opts) {
  return new Promise((resolve) => {
    const ip       = String(opts.ip || '').trim();
    const key      = String(opts.localKey || '').trim();
    const version  = String(opts.version || '3.3').trim();
    const port     = Number(opts.port || DEFAULT_PORT);
    const timeout  = Number(opts.timeoutMs || DEFAULT_TIMEOUT_MS);

    if (!ip || !key) return resolve({ ok: false, reason: 'missing ip or localKey' });
    if (!isPrivateIp(ip)) return resolve({ ok: false, reason: 'ip not private/local' });
    if (version !== '3.3') return resolve({ ok: false, reason: 'protocol ' + version + ' not supported' });

    const packet = packQuery33(opts.deviceId, key);
    let settled = false;
    let recvBuf = Buffer.alloc(0);

    const socket = new net.Socket();
    const done = (result) => {
      if (settled) return;
      settled = true;
      try { socket.destroy(); } catch (e) {}
      resolve(result);
    };

    try {
      socket.setTimeout(timeout);
      socket.connect(port, ip, () => {
        try { socket.write(packet); }
        catch (e) { done({ ok: false, reason: 'write: ' + e.message }); }
      });
      socket.on('data', (chunk) => {
        recvBuf = Buffer.concat([recvBuf, chunk]);
        // Versuche zu parsen sobald wir mind 20 Bytes haben
        if (recvBuf.length >= 20) {
          const parsed = parseResponse33(recvBuf, key);
          if (parsed.ok) done({ ok: true, dps: parsed.dps });
          else if (parsed.reason && parsed.reason !== 'truncated') {
            done({ ok: false, reason: parsed.reason });
          }
          // sonst: warten auf mehr daten
        }
      });
      socket.on('timeout', () => done({ ok: false, reason: 'timeout' }));
      socket.on('error', (e) => done({ ok: false, reason: 'error: ' + e.message }));
      socket.on('close', () => {
        if (!settled) {
          // Connection closed - versuche zu parsen was wir haben
          if (recvBuf.length >= 20) {
            const parsed = parseResponse33(recvBuf, key);
            if (parsed.ok) return done({ ok: true, dps: parsed.dps });
            return done({ ok: false, reason: parsed.reason || 'closed' });
          }
          done({ ok: false, reason: 'closed without data' });
        }
      });
    } catch (e) {
      done({ ok: false, reason: 'exception: ' + e.message });
    }
  });
}

module.exports = {
  DEFAULT_PORT,
  DEFAULT_TIMEOUT_MS,
  isPrivateIp,
  pack33,
  packQuery33,
  parseResponse33,
  sendCommand,
  queryStatus,
  crc32,
  ensureAesKey16
};
