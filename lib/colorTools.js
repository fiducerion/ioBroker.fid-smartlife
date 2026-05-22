/* lib/colorTools.js
 *
 * Konvertierung von Tuya-Color-Strings <-> #RRGGBB.
 *
 * Tuya hat zwei Color-DP-Formate:
 *  - DPS 5  (colour, 14 chars): rrggbb0hhhssvv
 *  - DPS 24 (colour_data, 12 chars): hhhhssssvvvv (hex, h=0..360, s=0..1000, v=0..1000)
 *
 * Implementierung uebernommen aus iobroker.tuya.
 */
'use strict';

function rgbToHsv(r, g, b) {
  r = r / 255; g = g / 255; b = b / 255;
  const max = Math.max(r, g, b), min = Math.min(r, g, b);
  let h, s, v = max;
  const d = max - min;
  s = max === 0 ? 0 : d / max;
  if (max === min) { h = 0; }
  else {
    switch (max) {
      case r: h = (g - b) / d + (g < b ? 6 : 0); break;
      case g: h = (b - r) / d + 2; break;
      case b: h = (r - g) / d + 4; break;
    }
    h /= 6;
  }
  return [Math.max(0, Math.min(1, h)), Math.max(0, Math.min(1, s)), Math.max(0, Math.min(1, v))];
}

function hsvToRgb(h, s, v) {
  let r, g, b;
  const i = Math.floor(h * 6);
  const f = h * 6 - i;
  const p = v * (1 - s);
  const q = v * (1 - f * s);
  const t = v * (1 - (1 - f) * s);
  switch (i % 6) {
    case 0: r = v; g = t; b = p; break;
    case 1: r = q; g = v; b = p; break;
    case 2: r = p; g = v; b = t; break;
    case 3: r = p; g = q; b = v; break;
    case 4: r = t; g = p; b = v; break;
    case 5: r = v; g = p; b = q; break;
  }
  return [Math.round(r * 255), Math.round(g * 255), Math.round(b * 255)];
}

function padding(n, len) {
  let s = (n < 0 ? '-' : '') + Math.abs(n).toString(16);
  while (s.length < len) s = '0' + s;
  return s;
}

function tuyaColorToHex(dpsId, dpValue) {
  if (typeof dpValue !== 'string') return null;
  if (String(dpsId) === '5') {
    if (dpValue.length !== 14) return null;
    return '#' + dpValue.substring(0, 6).toLowerCase();
  }
  if (String(dpsId) === '24') {
    if (dpValue.length !== 12) return null;
    const h = parseInt(dpValue.substring(0, 4),  16) / 360;
    const s = parseInt(dpValue.substring(4, 8),  16) / 1000;
    const v = parseInt(dpValue.substring(8, 12), 16) / 1000;
    if (isNaN(h) || isNaN(s) || isNaN(v)) return null;
    const [r, g, b] = hsvToRgb(h, s, v);
    return '#' + padding(r, 2) + padding(g, 2) + padding(b, 2);
  }
  return null;
}

function hexToTuyaColor(dpsId, hex) {
  if (typeof hex !== 'string' || hex.length !== 7 || hex[0] !== '#') return null;
  const r = parseInt(hex.substring(1, 3), 16);
  const g = parseInt(hex.substring(3, 5), 16);
  const b = parseInt(hex.substring(5, 7), 16);
  if (isNaN(r) || isNaN(g) || isNaN(b)) return null;
  const [h, s, v] = rgbToHsv(r, g, b);
  if (String(dpsId) === '5') {
    return padding(r, 2) + padding(g, 2) + padding(b, 2) + padding(Math.round(h * 360), 4) + padding(Math.round(s * 255), 2) + padding(Math.round(v * 255), 2);
  }
  if (String(dpsId) === '24') {
    return padding(Math.round(h * 360), 4) + padding(Math.round(s * 1000), 4) + padding(Math.round(v * 1000), 4);
  }
  return null;
}

function isColorDp(dpsId, codeCanon) {
  const id = String(dpsId);
  if (id !== '5' && id !== '24') return false;
  const c = String(codeCanon || '').toLowerCase();
  return c === 'colour' || c === 'colour_data' || c === 'color' || c === 'color_data';
}

module.exports = { rgbToHsv, hsvToRgb, tuyaColorToHex, hexToTuyaColor, isColorDp };
