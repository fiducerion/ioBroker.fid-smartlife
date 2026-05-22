/* lib/enhanced.js
 *
 * Post-Processors fuer spezielle DPS-Codes die in zusaetzlichen,
 * lesbaren States gespiegelt werden sollen. Vorbild: tuya-Adapter
 * enhanced_logic.js.
 *
 *  - DP 5 (colour_data / colour)  -> <dev>.5-rgb  als #rrggbb
 *  - DP 24 (colour_data)          -> <dev>.24-rgb als #rrggbb
 *  - phase_a / phase_b / phase_c  -> <code>-voltage, -current, -power, -frequency
 *
 * Read-Pfad (Tuya schickt Status):
 *   processIncoming({ code, dpId, value }, dev) -> [{statePath, value, common}]
 *
 * Write-Pfad (User schreibt auf den derived State):
 *   processWrite(derivedStateKey, value) -> { primaryDpsId: <encoded value> } | null
 */
'use strict';

const colorTools = require('./colorTools.js');

function pad(num, maxLength) {
  if (!maxLength) maxLength = 2;
  let s = num.toString(16);
  while (s.length < maxLength) s = '0' + s;
  return s;
}

/* --------------------------------------------------------------------------
 * Color-Format-Handler
 * Format A (DP 5 alte Lampen): rrggbb0hhhssvv   (14 hex Zeichen)
 *   rgb 6 hex + h(3 hex 0-360) + s(2 hex 0-255) + v(2 hex 0-255)
 * Format B (DP 24 neue Lampen): hhhhssssvvvv    (12 hex Zeichen)
 *   h(4 hex 0-360) + s(4 hex 0-1000) + v(4 hex 0-1000)
 * Format C (Group):  240;248;255  (semicolon-separierte RGB-Decimal)
 * -------------------------------------------------------------------------- */

function colorAFromDp(dpValue) {
  if (typeof dpValue !== 'string') return null;
  if (dpValue.includes(';')) {
    const parts = dpValue.split(';');
    if (parts.length === 3) {
      return '#' + pad(parseInt(parts[0], 10), 2) + pad(parseInt(parts[1], 10), 2) + pad(parseInt(parts[2], 10), 2);
    }
  }
  if (dpValue.length === 14) {
    return '#' + dpValue.substring(0, 6);
  }
  return null;
}

function colorAToDp(rgbHex) {
  if (typeof rgbHex !== 'string' || rgbHex.length !== 7 || rgbHex[0] !== '#') return null;
  const r = parseInt(rgbHex.substring(1, 3), 16);
  const g = parseInt(rgbHex.substring(3, 5), 16);
  const b = parseInt(rgbHex.substring(5, 7), 16);
  const [h, s, v] = colorTools.rgbToHsv(r, g, b);
  let res = pad(r, 2) + pad(g, 2) + pad(b, 2);
  res += pad(Math.round(h * 360), 4);
  res += pad(Math.round(s * 255), 2);
  res += pad(Math.round(v * 255), 2);
  return res;
}

function colorBFromDp(dpValue) {
  if (typeof dpValue !== 'string') return null;
  if (dpValue.includes(';')) {
    const parts = dpValue.split(';');
    if (parts.length === 3) {
      return '#' + pad(parseInt(parts[0], 10), 2) + pad(parseInt(parts[1], 10), 2) + pad(parseInt(parts[2], 10), 2);
    }
  }
  if (dpValue.length === 12) {
    const h = parseInt(dpValue.substring(0, 4), 16) / 360;
    const s = parseInt(dpValue.substring(4, 8), 16) / 1000;
    const v = parseInt(dpValue.substring(8, 12), 16) / 1000;
    const [r, g, b] = colorTools.hsvToRgb(h, s, v);
    return '#' + pad(r, 2) + pad(g, 2) + pad(b, 2);
  }
  return null;
}

function colorBToDp(rgbHex) {
  if (typeof rgbHex !== 'string' || rgbHex.length !== 7 || rgbHex[0] !== '#') return null;
  const r = parseInt(rgbHex.substring(1, 3), 16);
  const g = parseInt(rgbHex.substring(3, 5), 16);
  const b = parseInt(rgbHex.substring(5, 7), 16);
  const [h, s, v] = colorTools.rgbToHsv(r, g, b);
  let res = pad(Math.round(h * 360), 4);
  res += pad(Math.round(s * 1000), 4);
  res += pad(Math.round(v * 1000), 4);
  return res;
}

/* --------------------------------------------------------------------------
 * Phase-Power-Sensor (base64 encoded, manche Energy-Plugs)
 *   bytes[0..1] = Voltage * 10        -> V
 *   bytes[3..4] = Current * 1000      -> A
 *   bytes[6..7] = Power               -> W
 *   bytes[8..9] = Frequency * 10      -> Hz   (nur wenn length>9)
 * Manche neuere Geraete schicken laengere Buffer mit Layout:
 *   bytes[11..12] = Current/1000
 *   bytes[13..14] = Voltage/10
 * -------------------------------------------------------------------------- */

function _phaseDecode(dpValue, kind) {
  if (typeof dpValue !== 'string') return null;
  let buf;
  try { buf = Buffer.from(dpValue, 'base64'); }
  catch (e) { return null; }
  if (!buf || buf.length < 6) return null;

  if (kind === 'voltage') {
    if (buf.length > 14) return buf.readUInt16BE(13) / 10;
    if (buf.length > 1 && buf.length <= 10) return buf.readUInt16BE(0) / 10;
  } else if (kind === 'current') {
    if (buf.length > 14) return buf.readUInt16BE(11) / 1000;
    if (buf.length > 4 && buf.length <= 10) return buf.readUInt16BE(3) / 1000;
  } else if (kind === 'power') {
    if (buf.length > 7 && buf.length <= 10) return buf.readUInt16BE(6);
  } else if (kind === 'frequency') {
    if (buf.length > 9 && buf.length <= 10) return buf.readUInt16BE(8) / 10;
  }
  return null;
}

/* --------------------------------------------------------------------------
 * Enhanced-DP-Definitionen
 *   Pro DP eine Liste von "derived states" (postfix + common + converter)
 * -------------------------------------------------------------------------- */

// Set of canonical code names die als Color-DPs zaehlen.
// Wir wenden Color-Postprocessing NUR an wenn der Code-Name passt UND die DP-ID
// in den bekannten Slots liegt. Sonst wird z.B. bei einem AC mit DP 5 = windspeed
// faelschlich ein 5-rgb angelegt.
const COLOR_CODE_NAMES = new Set([
  'colour_data', 'colour', 'color_data', 'color',
  'colour_data_v2', 'colour_data_hsv', 'color_data_v2'
]);

const ENHANCED_BY_DPID = {
  // DP 5: Color-Format A (rrggbb + hsv) - NUR wenn code ein Color-Name ist
  '5': [{
    postfix: '-rgb',
    common: { name: 'RGB Color', type: 'string', role: 'level.color.rgb', read: true, write: true },
    fromDp: colorAFromDp,
    toDp: colorAToDp,
    primaryDpId: '5',
    requireCode: COLOR_CODE_NAMES
  }],
  // DP 24: Color-Format B (hsv-Hex)
  '24': [{
    postfix: '-rgb',
    common: { name: 'RGB Color', type: 'string', role: 'level.color.rgb', read: true, write: true },
    fromDp: colorBFromDp,
    toDp: colorBToDp,
    primaryDpId: '24',
    requireCode: COLOR_CODE_NAMES
  }]
};

const ENHANCED_BY_CODE = {
  // colour_data wird oft per Code referenziert ohne DPS-ID-Wissen.
  // Wir mappen nach DPS-ID je nachdem welche da ist (Heuristik).
  // Wenn DPS-ID nicht in {5,24} bekannt -> kein Postprocessing.
  'phase_a': _phasePostprocessors('Phase A'),
  'phase_b': _phasePostprocessors('Phase B'),
  'phase_c': _phasePostprocessors('Phase C')
};

function _phasePostprocessors(label) {
  return [
    {
      postfix: '-voltage',
      common: { name: label + ' Voltage', type: 'number', role: 'value.voltage', unit: 'V', read: true, write: false },
      fromDp: (v) => _phaseDecode(v, 'voltage'),
      readOnly: true
    },
    {
      postfix: '-current',
      common: { name: label + ' Current', type: 'number', role: 'value.current', unit: 'A', read: true, write: false },
      fromDp: (v) => _phaseDecode(v, 'current'),
      readOnly: true
    },
    {
      postfix: '-power',
      common: { name: label + ' Power', type: 'number', role: 'value.power', unit: 'W', read: true, write: false },
      fromDp: (v) => _phaseDecode(v, 'power'),
      readOnly: true
    },
    {
      postfix: '-frequency',
      common: { name: label + ' Frequency', type: 'number', role: 'value.frequency', unit: 'Hz', read: true, write: false },
      fromDp: (v) => _phaseDecode(v, 'frequency'),
      readOnly: true
    }
  ];
}

/**
 * Gibt die Liste der Postprocessor-Definitionen fuer einen DP zurueck.
 * Lookup primaer nach DPS-ID, sekundaer nach Code-Name.
 *
 * @param {string} dpId        z.B. '5' oder '24'
 * @param {string} codeCanon   z.B. 'colour_data', 'phase_a'
 * @return {Array|null}
 */
function getEnhanced(dpId, codeCanon) {
  if (dpId && ENHANCED_BY_DPID[String(dpId)]) {
    const defs = ENHANCED_BY_DPID[String(dpId)];
    // Filter: nur Definitionen ohne requireCode oder mit passendem code-Name
    const matching = defs.filter(d => !d.requireCode || (codeCanon && d.requireCode.has(codeCanon)));
    return matching.length ? matching : null;
  }
  if (codeCanon && ENHANCED_BY_CODE[codeCanon]) {
    return ENHANCED_BY_CODE[codeCanon];
  }
  return null;
}

/**
 * Findet die postprocessor-Definition fuer einen abgeleiteten State.
 * @param {string} derivedKey  z.B. '5-rgb'
 * @return {Object|null}
 */
function findEnhancedForDerived(derivedKey) {
  if (typeof derivedKey !== 'string') return null;
  for (const dpId of Object.keys(ENHANCED_BY_DPID)) {
    for (const def of ENHANCED_BY_DPID[dpId]) {
      if (derivedKey === dpId + def.postfix) return { def, dpId };
    }
  }
  // Fuer phase_x via Code
  // (kein toDp, daher write-mode nicht relevant)
  return null;
}

module.exports = {
  getEnhanced,
  findEnhancedForDerived,
  ENHANCED_BY_DPID,
  ENHANCED_BY_CODE
};
