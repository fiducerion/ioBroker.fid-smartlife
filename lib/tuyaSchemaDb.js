/* lib/tuyaSchemaDb.js
 *
 * Lokale Schema-Datenbank fuer Tuya-Geraete - extrahiert aus dem
 * iobroker.tuya-Adapter (lib/schema.json). Enthaelt nur Schemas fuer die
 * productKeys die in deiner Installation tatsaechlich vorkommen. Aktualisiert
 * werden kann es per Skript scripts/update-tuya-schemas.sh
 *
 * Schema-Item-Format:
 *   {
 *     id:        Number,         // DPS-ID (z.B. 1, 24, 133)
 *     code:      String,         // Code-Name (z.B. 'switch_led', 'colour_data')
 *     name:      String,         // Friendly Name (chinesisch oder englisch)
 *     mode:      'ro' | 'rw' | 'wo',
 *     type:      'obj' | 'raw',
 *     property:  { type: 'bool' | 'value' | 'enum' | 'string' | 'bitmap', ... }
 *   }
 *
 * Wir konvertieren Schema-Items in die `defs`-Struktur die der specMapper sonst
 * aus der Cloud-Spec macht, damit der Rest des Codes unveraendert bleibt.
 */
'use strict';

const fs   = require('fs');
const path = require('path');

let _loaded = null;

function loadDb() {
  if (_loaded) return _loaded;
  try {
    const raw = fs.readFileSync(path.join(__dirname, 'tuyaSchemas.json'), 'utf-8');
    _loaded = JSON.parse(raw);
  } catch (e) {
    _loaded = {};
  }
  return _loaded;
}

/**
 * Holt das Schema fuer einen productKey aus der lokalen DB.
 * Returns: { schema: [<dp items>], schemaExt: [...] } oder null.
 */
function getSchema(productKey) {
  if (!productKey) return null;
  const db = loadDb();
  const entry = db[productKey];
  if (!entry) return null;
  let schema, schemaExt;
  try { schema    = typeof entry.schema    === 'string' ? JSON.parse(entry.schema)    : entry.schema; }
  catch (e) { schema = []; }
  try { schemaExt = typeof entry.schemaExt === 'string' ? JSON.parse(entry.schemaExt) : entry.schemaExt; }
  catch (e) { schemaExt = []; }
  return { schema: schema || [], schemaExt: schemaExt || [] };
}

/**
 * Wandelt ein Schema-Item in die `defs`-Struktur die der spec-Mapper auch aus
 * der Cloud-Spec liefert. So muss der Rest des Codes nicht angefasst werden.
 *
 * Input  (Schema):
 *   { id:24, code:'colour_data', mode:'rw', type:'obj',
 *     property:{ type:'string', maxlen:128 } }
 *
 * Output (defs-Item kompatibel zu specMapper.specToDefs):
 *   { id:24, code:'colour_data', codeReal:'colour_data',
 *     mode:'rw', type:'string', maxlen:128 }
 */
function schemaItemToDef(item) {
  if (!item || !item.code || item.id == null) return null;
  const def = {
    id:       item.id,
    code:     String(item.code),
    codeReal: String(item.code),
    mode:     item.mode || 'ro',
    name:     item.name || item.code
  };
  const prop = item.property || {};
  // Type-Mapping Schema -> specMapper-Erwartung
  switch (prop.type) {
    case 'bool':
      def.type = 'boolean';
      break;
    case 'value':
      def.type = 'value';
      if (prop.min !== undefined)   def.min   = Number(prop.min);
      if (prop.max !== undefined)   def.max   = Number(prop.max);
      if (prop.step !== undefined)  def.step  = Number(prop.step);
      if (prop.scale !== undefined) def.scale = Number(prop.scale);
      if (prop.unit)                def.unit  = prop.unit;
      break;
    case 'enum':
      def.type = 'enum';
      if (Array.isArray(prop.range)) def.range = prop.range.map(String);
      break;
    case 'string':
      def.type = 'string';
      if (prop.maxlen !== undefined) def.maxlen = prop.maxlen;
      break;
    case 'bitmap':
      def.type = 'bitmap';
      if (Array.isArray(prop.label)) def.label = prop.label;
      if (prop.maxlen !== undefined) def.maxlen = prop.maxlen;
      break;
    case 'raw':
      def.type = 'raw';
      break;
    default:
      def.type = 'string';
  }
  return def;
}

/**
 * Holt eine Liste von defs-Items fuer einen productKey, fertig zur Uebergabe
 * an den specMapper-Workflow.
 *
 * Gibt [] zurueck wenn productKey unbekannt.
 */
function getDefs(productKey) {
  const sch = getSchema(productKey);
  if (!sch) return [];
  const defs = [];
  for (const item of (sch.schema || [])) {
    const d = schemaItemToDef(item);
    if (d) defs.push(d);
  }
  return defs;
}

module.exports = {
  loadDb,
  getSchema,
  getDefs,
  schemaItemToDef
};
