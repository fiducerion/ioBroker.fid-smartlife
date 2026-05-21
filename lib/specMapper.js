/* lib/specMapper.js
 *
 * Wandelt die Tuya-Specification (functions + status) in eine flache Liste
 * von Definitionen um, aus denen wir States anlegen koennen.
 *
 * 1:1 aus dem TuyaCloudReplace v2.5.2-Skript portiert.
 */
'use strict';

const ALIAS_NAMES = new Set([
  'on', 'brightness', 'color_temp_k', 'color_rgb',
  'mode', 'temperature', 'humidity', 'battery'
]);

function canon(code) {
  return String(code == null ? '' : code).trim().toLowerCase();
}

function parseValues(v) {
  if (!v) return {};
  if (typeof v === 'string') {
    try { return JSON.parse(v) || {}; } catch (e) { return {}; }
  }
  if (typeof v === 'object') return v;
  return {};
}

function specToDefs(spec) {
  if (!spec) return [];
  const fn = spec.functions || spec.standard_functions || [];
  const st = spec.status    || spec.standard_status    || [];
  const by = new Map();

  for (const x of st) {
    const rawCode = x.code || x.id;
    if (!rawCode) continue;
    const c = canon(rawCode);
    by.set(c, Object.assign({}, x, { codeReal: String(rawCode), canon: c }));
  }
  for (const x of fn) {
    const rawCode = x.code || x.id;
    if (!rawCode) continue;
    const c = canon(rawCode);
    const prev = by.get(c) || {};
    by.set(c, Object.assign({}, prev, x, {
      codeReal: prev.codeReal || String(rawCode),
      canon: c,
      _wFromFunctions: true
    }));
  }
  return Array.from(by.values());
}

function extractMeta(dpDef) {
  const p = (dpDef && dpDef.property) || parseValues(dpDef && dpDef.values) || {};
  const subType = String((dpDef && dpDef.type) || p.type || 'string').toLowerCase();

  let outType = 'string';
  if (subType === 'bool' || subType === 'boolean') outType = 'boolean';
  else if (/value|number|int|integer|float/.test(subType)) outType = 'number';

  const mode = String((dpDef && dpDef.mode) || '').toLowerCase();
  const enums = Array.isArray(p.enum)  ? p.enum
              : Array.isArray(p.range) ? p.range
              : undefined;

  return {
    type: outType,
    writable: mode.includes('w') || (dpDef && dpDef._wFromFunctions === true),
    unit:   p.unit,
    min:    typeof p.min === 'number' ? p.min : undefined,
    max:    typeof p.max === 'number' ? p.max : undefined,
    scale:  typeof p.scale === 'number' ? p.scale : undefined,
    enums:  enums,
    friendly: (dpDef && (dpDef.name || dpDef.codeReal || dpDef.canon)) || 'dp',
    dpId:   (dpDef && typeof dpDef.id !== 'undefined') ? String(dpDef.id) : undefined
  };
}

function roleFor(code, type) {
  const c = canon(code);
  if (type === 'boolean') return 'switch';
  if (/bright/i.test(c)) return 'level.dimmer';
  if (/temp_value|cct|color_temp|colourtemp/.test(c)) return 'level.color.temperature';
  if (/colour_data|color_data/.test(c)) return 'level.color.rgb';
  if (/humidity/.test(c)) return 'value.humidity';
  if (/temp|temperature/.test(c)) return 'value.temperature';
  if (/battery/.test(c)) return 'value.battery';
  return 'state';
}

function scaleOut(meta, value) {
  if (typeof value !== 'number') return value;
  if (meta && typeof meta.scale === 'number' && meta.scale > 0) {
    return value / Math.pow(10, meta.scale);
  }
  return value;
}
function scaleIn(meta, value) {
  if (typeof value !== 'number') return value;
  if (meta && typeof meta.scale === 'number' && meta.scale > 0) {
    return Math.round(value * Math.pow(10, meta.scale));
  }
  return value;
}

/**
 * Versucht aus den vorhandenen DP-Codes einen "canonicalen" zu finden.
 * Pruefen jetzt zusaetzlich den Typ des Source-DPs, damit kein boolean-Alias
 * auf einen number-DP zeigt (oder umgekehrt).
 *
 * @param {Set<string>} defCanonSet
 * @param {string[]} candidates
 * @param {object} dpMeta   meta-Map (canon -> {type, ...})
 * @param {string} expectedType  'boolean' | 'number' | 'string' oder null fuer egal
 */
function resolveAlias(defCanonSet, candidates, dpMeta, expectedType) {
  for (const c of candidates) {
    const cc = canon(c);
    if (!defCanonSet.has(cc)) continue;
    if (expectedType && dpMeta) {
      const meta = dpMeta[cc];
      if (meta && meta.type && meta.type !== expectedType) continue;  // Typ mismatch -> skip
    }
    return cc;
  }
  return null;
}

function computeAliases(defCanonSet, dpMeta) {
  const alias = {};
  const power   = resolveAlias(defCanonSet, ['switch_led', 'switch', 'switch_1', 'master_switch', 'power', 'relay_status'], dpMeta, 'boolean');
  const bri     = resolveAlias(defCanonSet, ['bright_value', 'bright_value_v2', 'brightness', 'bright'], dpMeta, 'number');
  const cct     = resolveAlias(defCanonSet, ['temp_value', 'cct', 'color_temp', 'colourtemp'], dpMeta, 'number');
  const rgb     = resolveAlias(defCanonSet, ['colour_data', 'color_data', 'colour_data_v2'], dpMeta, 'string');
  const mode    = resolveAlias(defCanonSet, ['work_mode', 'mode', 'light_mode'], dpMeta, null);  // mode kann string ODER number sein
  const temp    = resolveAlias(defCanonSet, ['temp_current', 'temperature', 'va_temperature'], dpMeta, 'number');
  const hum     = resolveAlias(defCanonSet, ['humidity_current', 'humidity_indoor', 'humidity'], dpMeta, 'number');
  const battery = resolveAlias(defCanonSet, ['battery_percentage', 'battery', 'battery_state'], dpMeta, 'number');

  if (power)   alias.on            = power;
  if (bri)     alias.brightness    = bri;
  if (cct)     alias.color_temp_k  = cct;
  if (rgb)     alias.color_rgb     = rgb;
  if (mode)    alias.mode          = mode;
  if (temp)    alias.temperature   = temp;
  if (hum)     alias.humidity      = hum;
  if (battery) alias.battery       = battery;

  // Mode-Alias: wenn der gewaehlte DP eine Zahl ist, dann ist der Alias auch
  // number. Wir kennzeichnen das im resultierenden alias-Objekt damit der
  // Caller den korrekten State-Typ anlegen kann.
  if (mode && dpMeta && dpMeta[mode] && dpMeta[mode].type === 'number') {
    alias._modeType = 'number';
  } else {
    alias._modeType = 'string';
  }
  return alias;
}

module.exports = {
  ALIAS_NAMES,
  canon,
  specToDefs,
  extractMeta,
  roleFor,
  scaleOut,
  scaleIn,
  computeAliases
};
