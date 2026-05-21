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
 *   - AES-128-ECB Encryption mit Geraete-localKey
 *   - Private-IP Check
 *
 * Was (noch) nicht:
 *   - Protokoll 3.1 (anderes Padding) und 3.4 (HMAC-SHA256 statt CRC32)
 *   - Persistente TCP-Verbindung
 *   - Status-Read via Local (cmd 10) - das laeuft weiter ueber Cloud-Poll
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

module.exports = {
  DEFAULT_PORT,
  DEFAULT_TIMEOUT_MS,
  isPrivateIp,
  pack33,
  sendCommand,
  crc32,           // exportiert fuer Tests
  ensureAesKey16   // exportiert fuer Tests
};
