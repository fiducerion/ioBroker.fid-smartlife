/* lib/specMapper.js
 *
 * Wandelt verschiedene Tuya-Schema-Quellen in eine flache Liste
 * von Definitionen um, aus denen wir States anlegen koennen.
 *
 * Drei Quellen, hierarchisch ausgewertet:
 *   1. Cloud-Spec (/v1.0/iot-03/devices/<id>/specification) - functions+status
 *   2. Raw-Schema (device.schema aus listDevices) - vollstaendiges Array
 *   3. Local schema.json - Lookup per productKey, vollstaendiges Fallback
 *
 * Quelle 2 ist meist am vollstaendigsten (kommt direkt aus dem User-Tuya-Account).
 * Quelle 1 fehlen oft viele DPs (Klima: 9 statt 30).
 * Quelle 3 dient als Fallback wenn productKey bekannt aber raw-Schema fehlt.
 */
'use strict';

const fs = require('fs');
const path = require('path');

let _schemaDb = null;       // 31 MB - lazy laden
let _schemaDbLoaded = false;

function _loadSchemaDb(logger) {
  if (_schemaDbLoaded) return _schemaDb;
  _schemaDbLoaded = true;
  try {
    const dbPath = path.join(__dirname, 'schema.json');
    _schemaDb = JSON.parse(fs.readFileSync(dbPath, 'utf-8'));
    if (logger) logger('info', 'Schema-DB geladen: ' + Object.keys(_schemaDb).length + ' Eintraege');
  } catch (e) {
    _schemaDb = null;
    if (logger) logger('warn', 'Schema-DB konnte nicht geladen werden: ' + e.message);
  }
  return _schemaDb;
}

/**
 * Lookup im lokalen schema.json per productKey.
 * @return {Array|null} schema-Array (wie raw.schema in listDevices) oder null
 */
function loadProductSchema(productKey, logger) {
  if (!productKey) return null;
  const db = _loadSchemaDb(logger);
  if (!db || !db[productKey]) return null;
  try {
    const entry = db[productKey];
    if (entry && typeof entry.schema === 'string') {
      return JSON.parse(entry.schema);
    }
    if (entry && Array.isArray(entry.schema)) {
      return entry.schema;
    }
  } catch (e) {
    if (logger) logger('debug', 'parse schemaDB ' + productKey + ': ' + e.message);
  }
  return null;
}

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

/**
 * Normalisiert Raw-Schema (Array von DP-Definitionen wie von Tuya-Cloud
 * geliefert) auf gemeinsames Format.
 *
 *   { code, codeReal, canon, id, mode, property: {type, range, min, max, scale, unit, ...} }
 */
function defsFromRawSchema(rawSchema) {
  if (!Array.isArray(rawSchema)) return [];
  const out = [];
  for (const item of rawSchema) {
    if (!item || (item.code === undefined && item.id === undefined)) continue;
    const rawCode = item.code || ('dp' + item.id);
    const c = canon(rawCode);
    out.push(Object.assign({}, item, {
      codeReal: String(rawCode),
      canon: c,
      _fromRawSchema: true
    }));
  }
  return out;
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

/**
 * Merge der drei Quellen. Reihenfolge: RawSchema > Spec > SchemaDB.
 * Das heisst:
 *   - alles aus RawSchema landet drin
 *   - wenn ein DP nur in Spec ist, kommt der noch dazu
 *   - wenn productKey-Lookup zusaetzliche DPs findet die nirgendwo sonst sind,
 *     kommen die auch dazu (mit niedrigster Prioritaet)
 *
 * @param {Object} spec        Cloud-Spec ({functions, status}) oder null
 * @param {Array}  rawSchema   Raw schema array oder null
 * @param {string} productKey  productKey fuer DB-Lookup oder null
 * @param {Function} logger    optional
 * @return {Array} normalisierte def-Liste
 */
function mergeSchemaSources(spec, rawSchema, productKey, logger) {
  const byCanon = new Map();
  const byId = new Map();

  // 1. Niedrigste Prio: SchemaDB
  if (productKey) {
    const dbSchema = loadProductSchema(productKey, logger);
    if (dbSchema) {
      for (const d of defsFromRawSchema(dbSchema)) {
        byCanon.set(d.canon, Object.assign({}, d, { _source: 'db' }));
        if (d.id !== undefined) byId.set(String(d.id), d.canon);
      }
    }
  }

  // 2. Spec (kann Spec-only-Felder beisteuern)
  const specDefs = specToDefs(spec);
  for (const d of specDefs) {
    const prev = byCanon.get(d.canon) || {};
    byCanon.set(d.canon, Object.assign({}, prev, d, { _source: 'spec' }));
    if (d.id !== undefined) byId.set(String(d.id), d.canon);
  }

  // 3. Hoechste Prio: RawSchema (am vollstaendigsten)
  if (Array.isArray(rawSchema)) {
    for (const d of defsFromRawSchema(rawSchema)) {
      const prev = byCanon.get(d.canon) || {};
      byCanon.set(d.canon, Object.assign({}, prev, d, { _source: 'raw' }));
      if (d.id !== undefined) byId.set(String(d.id), d.canon);
    }
  }

  return Array.from(byCanon.values());
}

function extractMeta(dpDef) {
  // Akzeptiere drei Formate:
  //  - Cloud-Spec: { type:'obj', property:{type:'bool'|'value'|...} }
  //  - Raw-Schema (device.schema): identisch zu Cloud-Spec
  //  - Schema-DB-flat:  { type:'boolean'|'value'|... } direkt am def
  const p = (dpDef && dpDef.property) || parseValues(dpDef && dpDef.values) || {};
  const flatType = String((dpDef && dpDef.type) || '').toLowerCase();
  const subType  = String(p.type || flatType || 'string').toLowerCase();

  let outType = 'string';
  let encoding;          // 'base64' fuer raw, bitmap-Decoder spezial
  let isBitmap = false;

  if (subType === 'bool' || subType === 'boolean') {
    outType = 'boolean';
  } else if (/value|number|int|integer|float/.test(subType)) {
    outType = 'number';
  } else if (subType === 'bitmap') {
    outType = 'number';
    isBitmap = true;
  } else if (subType === 'raw') {
    outType = 'string';
    encoding = 'base64';
  } else if (subType === 'enum') {
    // enum -> number mit common.states (siehe stateForDef)
    outType = 'number';
  }

  const mode = String((dpDef && dpDef.mode) || '').toLowerCase();
  const enums = Array.isArray(p.range) ? p.range
              : Array.isArray(p.enum)  ? p.enum
              : Array.isArray(dpDef && dpDef.range) ? dpDef.range
              : undefined;
  // bitmap labels (z.B. fault-Codes)
  const bitmapLabels = Array.isArray(p.label) ? p.label : undefined;

  const minVal   = (typeof p.min === 'number') ? p.min : (typeof (dpDef && dpDef.min) === 'number' ? dpDef.min : undefined);
  const maxVal   = (typeof p.max === 'number') ? p.max : (typeof (dpDef && dpDef.max) === 'number' ? dpDef.max : undefined);
  const scaleVal = (typeof p.scale === 'number') ? p.scale : (typeof (dpDef && dpDef.scale) === 'number' ? dpDef.scale : undefined);
  const unit     = p.unit || (dpDef && dpDef.unit);

  return {
    type: outType,
    subType: subType,
    isBitmap: isBitmap,
    bitmapLabels: bitmapLabels,
    encoding: encoding,
    writable: mode.includes('w') || (dpDef && dpDef._wFromFunctions === true),
    unit:   unit,
    min:    minVal,
    max:    maxVal,
    scale:  scaleVal,
    enums:  enums,
    friendly: (dpDef && (dpDef.name || dpDef.codeReal || dpDef.canon || dpDef.code)) || 'dp',
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
  if (/voltage/.test(c)) return 'value.voltage';
  if (/current/.test(c)) return 'value.current';
  if (/power|watt/.test(c)) return 'value.power';
  if (/energy|kwh/.test(c)) return 'value.power.consumption';
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
  const mode    = resolveAlias(defCanonSet, ['work_mode', 'mode', 'light_mode'], dpMeta, null);
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
  defsFromRawSchema,
  mergeSchemaSources,
  loadProductSchema,
  extractMeta,
  roleFor,
  scaleOut,
  scaleIn,
  computeAliases
};
