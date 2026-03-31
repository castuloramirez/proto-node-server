'use strict';

/**
 * server.js — Node.js replacement for the Javalin protobuf backend.
 *
 * Run:  node server.js
 *       node server.js --port 8080
 *
 * Exposes exactly the same REST surface the React app expects:
 *   GET  /api/types
 *   POST /api/compile
 *   POST /api/decode
 *   POST /api/encode
 *   POST /api/validateJson
 *
 * Default schema:  place protos/all.desc (binary FileDescriptorSet from protoc)
 *                  next to this file and it will be loaded on startup —
 *                  exactly like the Java version loaded it from the classpath.
 */

const express = require('express');
const path    = require('path');
const fs      = require('fs');
const os      = require('os');
const { execFile } = require('child_process');
const { promisify } = require('util');
const protobuf = require('protobufjs');
const descriptor = require('protobufjs/ext/descriptor');

const execFileAsync  = promisify(execFile);
const mkdtempAsync   = promisify(fs.mkdtemp);
const writeFileAsync = promisify(fs.writeFile);
const readFileAsync  = promisify(fs.readFile);
const rmAsync        = promisify(fs.rm);

/*  
   CLI args
  */
let port = 7070;
const args = process.argv.slice(2);
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--port' && args[i + 1]) port = parseInt(args[++i], 10);
}

/*  
   Express setup
  */
const app = express();
app.use(express.json({ limit: '10mb' }));

/*  
   ProtoRegistry helpers
   (mirrors ProtoRegistry.java)
  */

/**
 * Parse a binary FileDescriptorSet (produced by protoc --descriptor_set_out)
 * and return a Map<fullName, protobuf.Type>.
 *
 * @param {Buffer} bytes
 * @returns {Map<string, protobuf.Type>}
 */
function registryFromBytes(bytes) {
  const FileDescriptorSet = descriptor.FileDescriptorSet;
  const fds  = FileDescriptorSet.decode(bytes);
  const root = protobuf.Root.fromDescriptor(fds);
  root.resolveAll();

  const map = new Map();
  collectTypes(root, map);
  return map;
}

function collectTypes(ns, map) {
  for (const name of Object.keys(ns.nested || {})) {
    const child = ns.nested[name];
    if (child instanceof protobuf.Type) {
      // strip leading dot that protobufjs sometimes adds
      map.set(child.fullName.replace(/^\./, ''), child);
      collectTypes(child, map);          // nested messages
    } else if (child instanceof protobuf.Namespace) {
      collectTypes(child, map);
    }
  }
}

function listMessageTypes(map) {
  return Array.from(map.keys()).sort();
}

function getType(map, fullName) {
  const t = map.get(fullName);
  if (!t) throw new BadRequest(`Unknown message type: ${fullName}`);
  return t;
}

/*  
   SchemaCache  (mirrors SchemaCache.java)
   TTL: 20 min, UUID keys
  */
class SchemaCache {
  constructor(ttlSeconds = 20 * 60) {
    this._ttl   = ttlSeconds;
    this._cache = new Map();
  }

  put(descBytes) {
    this._cleanup();
    const id = crypto.randomUUID();
    this._cache.set(id, { desc: descBytes, createdAt: Date.now() });
    return id;
  }

  get(id) {
    this._cleanup();
    const e = this._cache.get(id);
    if (!e) throw new BadRequest("Unknown/expired schemaId. Click 'Validate Proto' again.");
    return e.desc;
  }

  _cleanup() {
    const now = Date.now();
    for (const [k, v] of this._cache) {
      if ((now - v.createdAt) / 1000 > this._ttl) this._cache.delete(k);
    }
  }
}

const schemaCache = new SchemaCache();

/*  
   ProtoCompiler  (mirrors ProtoCompiler.java)
   Shells out to system protoc.
  */
async function compileSingleFile(protoContent, includePaths = []) {
  const dir     = await mkdtempAsync(path.join(os.tmpdir(), 'proto-ui-'));
  const proto   = path.join(dir, 'unnamed.proto');
  const outDesc = path.join(dir, 'all.desc');

  try {
    await writeFileAsync(proto, protoContent, 'utf8');

    const cliArgs = [
      '--include_imports',
      `--descriptor_set_out=${outDesc}`,
      '-I', dir,
      ...includePaths.flatMap(p => p ? ['-I', p.trim()] : []),
      proto,
    ];

    try {
      await execFileAsync('protoc', cliArgs);
    } catch (err) {
      const msg = (err.stderr || '') + (err.stdout || '') || err.message;
      throw new BadRequest(`protoc failed:\n${msg}`);
    }

    const descBytes    = await readFileAsync(outDesc);
    const registry     = registryFromBytes(descBytes);
    const messageTypes = listMessageTypes(registry);
    return { descriptorSetBytes: descBytes, messageTypes };
  } finally {
    rmAsync(dir, { recursive: true, force: true }).catch(() => {});
  }
}

/*  
   Decoder  (mirrors Decoder.java)
  */

function stripGrpcFrame(buf) {
  if (buf.length < 5) throw new Error('Buffer too short for gRPC frame.');
  if (buf[0] !== 0)   throw new Error('Compressed gRPC frames are not supported.');
  const len = buf.readUInt32BE(1);
  if (buf.length < 5 + len) throw new Error('gRPC frame payload truncated.');
  return buf.slice(5, 5 + len);
}

function stripVarintPrefix(buf) {
  let result = 0, shift = 0, pos = 0;
  while (pos < buf.length) {
    const b = buf[pos++];
    result |= (b & 0x7f) << shift;
    if ((b & 0x80) === 0) break;
    shift += 7;
    if (shift >= 35) throw new Error('Varint prefix overlong.');
  }
  const len = result >>> 0;
  if (buf.length < pos + len) throw new Error('Varint-delimited payload truncated.');
  return buf.slice(pos, pos + len);
}

function decodeBytes(type, msgBytes, modeLabel, format) {
  const msg = type.decode(msgBytes);
  const obj = type.toObject(msg, {
    longs: String, enums: String, bytes: String,
    defaults: true, arrays: true, objects: true, oneofs: true,
  });
  return {
    decoded: JSON.stringify(obj, null, 2),
    modeUsed: modeLabel,
    messageByteLength: msgBytes.length,
  };
}

function decodeBase64(type, base64, mode, format) {
  const raw = Buffer.from(base64, 'base64');

  if (mode === 'AUTO') {
    for (const m of ['RAW', 'GRPC', 'DELIMITED_VARINT']) {
      try { return decodeWithMode(type, raw, m, format); } catch (_) {}
    }
    throw new BadRequest('AUTO decode failed for RAW, GRPC, and DELIMITED_VARINT.');
  }
  return decodeWithMode(type, raw, mode, format);
}

function decodeWithMode(type, raw, mode, format) {
  let msgBytes;
  switch (mode) {
    case 'RAW':              msgBytes = raw; break;
    case 'GRPC':             msgBytes = stripGrpcFrame(raw); break;
    case 'DELIMITED_VARINT': msgBytes = stripVarintPrefix(raw); break;
    default: throw new BadRequest(`Unknown decode mode: ${mode}`);
  }
  return decodeBytes(type, msgBytes, mode, format);
}

function encodeJson(type, jsonString) {
  const obj = JSON.parse(jsonString);
  const msg = type.fromObject(obj);
  const err = type.verify(msg);
  if (err) throw new BadRequest(`Encode verify failed: ${err}`);
  const bytes = type.encode(msg).finish();
  return {
    base64:     Buffer.from(bytes).toString('base64'),
    hex:        Buffer.from(bytes).toString('hex'),
    byteLength: bytes.length,
  };
}

function validateJson(type, jsonString) {
  let obj;
  try { obj = JSON.parse(jsonString); }
  catch (e) { throw new BadRequest('Invalid JSON: ' + e.message); }
  const err = type.verify(type.fromObject(obj));
  if (err) throw new BadRequest('Validation failed: ' + err);
}

/*  
   Default registry  (mirrors Java classpath load)
   Place protos/all.desc next to server.js.
  */
let defaultRegistry = null;
const defaultDescPath = path.join(__dirname, 'protos', 'all.desc');
if (fs.existsSync(defaultDescPath)) {
  try {
    defaultRegistry = registryFromBytes(fs.readFileSync(defaultDescPath));
    console.log(`Loaded default schema: ${listMessageTypes(defaultRegistry).length} types.`);
  } catch (e) {
    console.warn('Could not load protos/all.desc:', e.message);
  }
}

/*  
   Error class
 */
class BadRequest extends Error { constructor(m) { super(m); } }

function fail(res, status, message) {
  res.status(status).json({ message: String(message) });
}

/* Routes */

// GET /api/types
app.get('/api/types', (_req, res) => {
  console.log("Hola debugging")
  if (!defaultRegistry) return res.json([]);
  res.json(listMessageTypes(defaultRegistry));
});

// POST /api/compile
app.post('/api/compile', async (req, res) => {
  try {
    const { proto, includePaths: rawPaths = '' } = req.body;
    if (!proto) return fail(res, 400, 'Missing field: proto');
    const includePaths = String(rawPaths).split(/\r?\n/).map(s => s.trim()).filter(Boolean);
    const { descriptorSetBytes, messageTypes } = await compileSingleFile(proto, includePaths);
    const schemaId = schemaCache.put(descriptorSetBytes);
    res.json({ schemaId, types: messageTypes });
  } catch (e) {
    fail(res, e instanceof BadRequest ? 400 : 500, e.message);
  }
});

// POST /api/decode
app.post('/api/decode', (req, res) => {
  try {
    const { type: typeName, base64: b64, schemaId,
            mode = 'AUTO', format = 'JSON' } = req.body;
    if (!typeName) return fail(res, 400, 'Missing field: type');
    if (!b64)      return fail(res, 400, 'Missing field: base64');

    let registry;
    if (schemaId) {
      registry = registryFromBytes(schemaCache.get(schemaId));
    } else {
      if (!defaultRegistry) return fail(res, 400,
        'No schemaId provided and no default protos/all.desc loaded. Use /api/compile first.');
      registry = defaultRegistry;
    }

    const result = decodeBase64(
      getType(registry, typeName), b64,
      String(mode).toUpperCase(), String(format).toUpperCase()
    );
    res.json(result);
  } catch (e) {
    fail(res, e instanceof BadRequest ? 400 : 500, e.message);
  }
});

// POST /api/encode
app.post('/api/encode', (req, res) => {
  try {
    const { schemaId, type: typeName, json } = req.body;
    if (!schemaId) return fail(res, 400, 'Missing field: schemaId');
    if (!typeName) return fail(res, 400, 'Missing field: type');
    if (!json)     return fail(res, 400, 'Missing field: json');
    const registry = registryFromBytes(schemaCache.get(schemaId));
    res.json(encodeJson(getType(registry, typeName), json));
  } catch (e) {
    fail(res, e instanceof BadRequest ? 400 : 500, e.message);
  }
});

// POST /api/validateJson
app.post('/api/validateJson', (req, res) => {
  try {
    const { schemaId, type: typeName, json } = req.body;
    if (!typeName) return fail(res, 400, 'Missing field: type');
    if (!json)     return fail(res, 400, 'Missing field: json');

    // schemaId is optional — fall back to default registry (same as /api/decode)
    let registry;
    if (schemaId) {
      registry = registryFromBytes(schemaCache.get(schemaId));
    } else {
      if (!defaultRegistry) return fail(res, 400,
        'No schemaId and no default protos/all.desc loaded.');
      registry = defaultRegistry;
    }

    validateJson(getType(registry, typeName), json);
    res.json({ ok: true });
  } catch (e) {
    fail(res, e instanceof BadRequest ? 400 : 500, e.message);
  }
});

/*  
   Start
  */
app.listen(port, () => console.log(`Open http://localhost:${port}`));
