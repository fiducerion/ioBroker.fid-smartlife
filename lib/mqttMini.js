/* lib/mqttMini.js
 *
 * Minimaler MQTT 3.1.1 Client ohne externe Dependencies.
 * Implementiert nur was wir fuer Tuya brauchen:
 *   - TLS-Connection (port 8883)
 *   - CONNECT + CONNACK
 *   - SUBSCRIBE + SUBACK
 *   - PUBLISH (rx)  -> on('message') Callback
 *   - PINGREQ + PINGRESP (Keepalive)
 *   - DISCONNECT
 *
 * KEINE Implementation von: QoS 1/2 ack out, RETAIN, WILL, MQTT 5.0 properties.
 *
 * Tuya-MQTT braucht QoS 0/1 inbound, deshalb minimal-Scope.
 *
 * Usage:
 *   const mc = new MqttMiniClient({
 *     host: 'm1.tuyaeu.com',
 *     port: 8883,
 *     clientId: '...',
 *     username: '...',
 *     password: '...',
 *     keepAliveSec: 60,
 *     logger: (level, msg) => ...
 *   });
 *   mc.on('connect', () => mc.subscribe(topic));
 *   mc.on('message', (topic, payload) => ...);
 *   mc.on('error', (err) => ...);
 *   mc.on('close', () => ...);
 *   mc.connect();
 */
'use strict';

const tls = require('tls');
const EventEmitter = require('events');

// Packet-Typen
const TYPE_CONNECT     = 0x10;
const TYPE_CONNACK     = 0x20;
const TYPE_PUBLISH     = 0x30;
const TYPE_PUBACK      = 0x40;
const TYPE_SUBSCRIBE   = 0x80;
const TYPE_SUBACK      = 0x90;
const TYPE_UNSUBSCRIBE = 0xA0;
const TYPE_UNSUBACK    = 0xB0;
const TYPE_PINGREQ     = 0xC0;
const TYPE_PINGRESP    = 0xD0;
const TYPE_DISCONNECT  = 0xE0;

function encodeRemainingLength(len) {
  // Variable length encoding (1-4 bytes)
  const out = [];
  do {
    let digit = len % 128;
    len = Math.floor(len / 128);
    if (len > 0) digit |= 0x80;
    out.push(digit);
  } while (len > 0);
  return Buffer.from(out);
}

function decodeRemainingLength(buf, offset) {
  let multiplier = 1;
  let value = 0;
  let i = offset;
  let digit;
  do {
    if (i >= buf.length) return null;   // brauche mehr Daten
    digit = buf[i++];
    value += (digit & 0x7f) * multiplier;
    if (multiplier > 128 * 128 * 128) return { error: 'malformed length' };
    multiplier *= 128;
  } while ((digit & 0x80) !== 0);
  return { value: value, offset: i };
}

function encodeString(str) {
  const b = Buffer.from(str, 'utf8');
  const out = Buffer.alloc(2 + b.length);
  out.writeUInt16BE(b.length, 0);
  b.copy(out, 2);
  return out;
}

class MqttMiniClient extends EventEmitter {
  constructor(opts) {
    super();
    this.host = opts.host;
    this.port = opts.port || 8883;
    this.clientId = opts.clientId;
    this.username = opts.username;
    this.password = opts.password;
    this.keepAliveSec = Math.max(10, Number(opts.keepAliveSec) || 60);
    this.logger = opts.logger || (() => {});
    this.socket = null;
    this.connected = false;
    this.rxBuf = Buffer.alloc(0);
    this.pingTimer = null;
    this.connectTimeout = null;
    this.lastPingSent = 0;
    this._nextPacketId = 1;
    this._closing = false;
  }

  connect() {
    this._closing = false;
    this.logger('debug', 'MqttMini: connecting to ' + this.host + ':' + this.port);
    this.socket = tls.connect({
      host: this.host,
      port: this.port,
      // SNI brauchen wir auf Tuya-Brokers
      servername: this.host,
      // Tuya nutzt offizielle TLS-Certs - normalerweise valid
      rejectUnauthorized: true
    });
    this.socket.setTimeout(0);

    this.socket.on('secureConnect', () => {
      this.logger('debug', 'MqttMini: TLS established, sending CONNECT');
      this._sendConnect();
    });
    this.socket.on('data', (data) => this._onData(data));
    this.socket.on('error', (err) => {
      this.logger('warn', 'MqttMini: socket error: ' + (err.message || err));
      this.emit('error', err);
    });
    this.socket.on('close', () => {
      this.logger('debug', 'MqttMini: socket closed');
      this._cleanup();
      this.emit('close');
    });

    // Connect-Timeout: wenn nach 20s kein CONNACK, abbrechen
    this.connectTimeout = setTimeout(() => {
      if (!this.connected) {
        this.logger('warn', 'MqttMini: CONNECT timeout');
        this.socket.destroy();
      }
    }, 20000);
  }

  _sendConnect() {
    // Variable Header: Protocol Name "MQTT" (4 bytes), Protocol Level 4 (MQTT 3.1.1),
    //                  Connect Flags, KeepAlive
    const protoName = encodeString('MQTT');
    const protoLevel = Buffer.from([0x04]);
    // Flags: username=1, password=1, clean session=1
    let flags = 0x00;
    flags |= 0x02;   // clean session
    if (this.username) flags |= 0x80;
    if (this.password) flags |= 0x40;
    const flagsBuf = Buffer.from([flags]);
    const keepAlive = Buffer.alloc(2);
    keepAlive.writeUInt16BE(this.keepAliveSec, 0);

    // Payload: ClientId, [Username, Password]
    const clientIdBuf = encodeString(this.clientId);
    const userBuf = this.username ? encodeString(this.username) : Buffer.alloc(0);
    const passBuf = this.password ? encodeString(this.password) : Buffer.alloc(0);

    const variableHeader = Buffer.concat([protoName, protoLevel, flagsBuf, keepAlive]);
    const payload = Buffer.concat([clientIdBuf, userBuf, passBuf]);
    const remaining = Buffer.concat([variableHeader, payload]);

    const fixedHeader = Buffer.concat([
      Buffer.from([TYPE_CONNECT]),
      encodeRemainingLength(remaining.length)
    ]);

    this.socket.write(Buffer.concat([fixedHeader, remaining]));
  }

  _startKeepAlive() {
    if (this.pingTimer) clearInterval(this.pingTimer);
    // Send PINGREQ alle (keepAlive - 5) Sekunden
    const intervalMs = Math.max(5000, (this.keepAliveSec - 5) * 1000);
    this.pingTimer = setInterval(() => {
      if (!this.connected || !this.socket) return;
      try {
        this.socket.write(Buffer.from([TYPE_PINGREQ, 0x00]));
        this.lastPingSent = Date.now();
        this.logger('debug', 'MqttMini: PINGREQ sent');
      } catch (e) {
        this.logger('warn', 'MqttMini: PINGREQ write failed: ' + e.message);
      }
    }, intervalMs);
  }

  _onData(data) {
    this.rxBuf = Buffer.concat([this.rxBuf, data]);
    // Process all complete packets in buffer
    while (this.rxBuf.length >= 2) {
      const type = this.rxBuf[0] & 0xf0;
      const lenInfo = decodeRemainingLength(this.rxBuf, 1);
      if (!lenInfo) return;   // need more
      if (lenInfo.error) {
        this.logger('warn', 'MqttMini: malformed packet length');
        this.socket.destroy();
        return;
      }
      const totalLen = lenInfo.offset + lenInfo.value;
      if (this.rxBuf.length < totalLen) return;   // need more

      const pkt = this.rxBuf.slice(0, totalLen);
      this.rxBuf = this.rxBuf.slice(totalLen);
      this._handlePacket(type, pkt[0], pkt.slice(lenInfo.offset));
    }
  }

  _handlePacket(type, firstByte, body) {
    switch (type) {
      case TYPE_CONNACK: {
        if (this.connectTimeout) {
          clearTimeout(this.connectTimeout);
          this.connectTimeout = null;
        }
        if (body.length < 2) {
          this.logger('warn', 'MqttMini: short CONNACK');
          this.socket.destroy();
          return;
        }
        const returnCode = body[1];
        if (returnCode === 0) {
          this.connected = true;
          this.logger('info', 'MqttMini: CONNACK ok - MQTT connected');
          this._startKeepAlive();
          this.emit('connect');
        } else {
          const reasons = {
            1: 'Unacceptable protocol version',
            2: 'Identifier rejected',
            3: 'Server unavailable',
            4: 'Bad username or password',
            5: 'Not authorized'
          };
          const msg = 'CONNACK rejected: ' + (reasons[returnCode] || ('code ' + returnCode));
          this.logger('warn', 'MqttMini: ' + msg);
          this.emit('error', new Error(msg));
          this.socket.destroy();
        }
        break;
      }
      case TYPE_PUBLISH: {
        const qos = (firstByte >> 1) & 0x03;
        // Topic
        if (body.length < 2) return;
        const topicLen = body.readUInt16BE(0);
        if (body.length < 2 + topicLen) return;
        const topic = body.slice(2, 2 + topicLen).toString('utf8');
        let payloadStart = 2 + topicLen;
        let packetId = 0;
        if (qos > 0) {
          if (body.length < payloadStart + 2) return;
          packetId = body.readUInt16BE(payloadStart);
          payloadStart += 2;
        }
        const payload = body.slice(payloadStart);
        this.emit('message', topic, payload);
        // PUBACK fuer QoS 1
        if (qos === 1) {
          const ack = Buffer.alloc(4);
          ack[0] = TYPE_PUBACK;
          ack[1] = 0x02;
          ack.writeUInt16BE(packetId, 2);
          try { this.socket.write(ack); } catch (e) { /* ignore */ }
        }
        break;
      }
      case TYPE_SUBACK: {
        if (body.length < 3) return;
        const pid = body.readUInt16BE(0);
        const returnCode = body[2];
        if (returnCode >= 0x80) {
          this.logger('warn', 'MqttMini: SUBACK REJECTED pid=' + pid + ' rc=0x' + returnCode.toString(16));
          this.emit('error', new Error('Subscribe rejected with code 0x' + returnCode.toString(16)));
        } else {
          this.logger('info', 'MqttMini: SUBACK ok pid=' + pid + ' rc=' + returnCode);
        }
        break;
      }
      case TYPE_PINGRESP: {
        this.logger('debug', 'MqttMini: PINGRESP received');
        break;
      }
      case TYPE_UNSUBACK: {
        this.logger('debug', 'MqttMini: UNSUBACK');
        break;
      }
      default: {
        this.logger('debug', 'MqttMini: unknown packet type 0x' + type.toString(16));
      }
    }
  }

  subscribe(topic, qos) {
    if (!this.connected) {
      this.logger('warn', 'MqttMini: subscribe called but not connected');
      return false;
    }
    qos = (typeof qos === 'number') ? qos : 1;
    const pid = this._nextPacketId++;
    if (this._nextPacketId > 0xffff) this._nextPacketId = 1;

    const topicBuf = encodeString(topic);
    const qosBuf = Buffer.from([qos & 0x03]);
    const variableHeader = Buffer.alloc(2);
    variableHeader.writeUInt16BE(pid, 0);
    const payload = Buffer.concat([topicBuf, qosBuf]);
    const remaining = Buffer.concat([variableHeader, payload]);

    const fixedHeader = Buffer.concat([
      Buffer.from([TYPE_SUBSCRIBE | 0x02]),   // Reserved bits = 0010 fuer SUBSCRIBE
      encodeRemainingLength(remaining.length)
    ]);
    try {
      this.socket.write(Buffer.concat([fixedHeader, remaining]));
      this.logger('info', 'MqttMini: SUBSCRIBE sent for topic=' + topic + ' (qos=' + qos + ', pid=' + pid + ')');
      return true;
    } catch (e) {
      this.logger('warn', 'MqttMini: subscribe write failed: ' + e.message);
      return false;
    }
  }

  disconnect() {
    this._closing = true;
    if (this.socket && this.connected) {
      try {
        this.socket.write(Buffer.from([TYPE_DISCONNECT, 0x00]));
      } catch (e) { /* ignore */ }
    }
    if (this.socket) {
      try { this.socket.end(); } catch (e) { /* ignore */ }
    }
    this._cleanup();
  }

  _cleanup() {
    this.connected = false;
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
    if (this.connectTimeout) {
      clearTimeout(this.connectTimeout);
      this.connectTimeout = null;
    }
  }
}

module.exports = { MqttMiniClient: MqttMiniClient };
