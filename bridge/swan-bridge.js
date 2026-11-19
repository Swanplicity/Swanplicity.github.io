#!/usr/bin/env node
'use strict';
/*
 * SWAN BRIDGE — PC copy. By Swanplicity.
 *
 * One job: sit between Casey's miners and the mining pool and pass
 * messages both ways, unchanged.
 *
 *   miners  --WebSocket (Stratum JSON)-->  SWAN BRIDGE  --raw TCP-->  pool
 *
 * The bridge is a relay ONLY. It holds no wallet, no payout address of
 * its own, no keys, and no money. The pool pays Casey's Bitcoin address
 * directly; the address travels inside the miners' own authorize
 * messages, straight through.
 *
 * Zero dependencies: Node.js built-ins only. Start it with
 * Start-Bridge.bat (double-click) or:  node swan-bridge.js
 *
 * Settings (environment, all optional):
 *   SWAN_BRIDGE_PORT   port miners connect to        (default 3333)
 *   SWAN_POOL_HOST     main pool host                 (default stratum.ckpool.org)
 *   SWAN_POOL_PORT     main pool port                 (default 3333)
 *   SWAN_BACKUP_HOST   backup pool host               (default solo.stratum.braiins.com)
 *   SWAN_BACKUP_PORT   backup pool port               (default 3333)
 *   SWAN_PROBE_MS      main-pool recovery probe pace  (default 60000)
 */

const net = require('net');
const crypto = require('crypto');

/* ---------------- configuration ---------------- */

const PORT = intOr(process.env.SWAN_BRIDGE_PORT, 3333);
const POOLS = {
  main:   { name: 'main',   host: process.env.SWAN_POOL_HOST   || 'stratum.ckpool.org',      port: intOr(process.env.SWAN_POOL_PORT, 3333) },
  backup: { name: 'backup', host: process.env.SWAN_BACKUP_HOST || 'solo.stratum.braiins.com', port: intOr(process.env.SWAN_BACKUP_PORT, 3333) },
};
const PROBE_MS = intOr(process.env.SWAN_PROBE_MS, 60000);
const CONNECT_TIMEOUT_MS = 10000;

function intOr(value, fallback) {
  const n = parseInt(value, 10);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

function stamp() {
  return new Date().toISOString().replace('T', ' ').replace(/\..+/, '');
}
function log(line) {
  console.log('[' + stamp() + '] ' + line);
}

/* ---------------- pool manager ----------------
 * One "current pool" for the whole bridge. Every miner session holds
 * its own TCP line to the current pool. If the current pool fails, the
 * manager flips to the other pool and every miner is closed with code
 * 1012 (service restart) so it reconnects at once onto the new pool
 * and re-runs its subscribe/authorize there — Stratum session state
 * (extranonce) lives on the pool line, so a fresh miner handshake is
 * the only clean way across a pool switch.
 * While on the backup, a probe checks the main pool every PROBE_MS;
 * the moment it answers, the bridge flips back and logs the line
 * Casey asked for: "Back on the main pool."
 */

const poolManager = {
  current: 'main',
  probeTimer: null,
  probing: false,

  pool() { return POOLS[this.current]; },
  other() { return this.current === 'main' ? 'backup' : 'main'; },

  // A pool line failed. Flip to the other pool and bounce all miners.
  poolFailed(which) {
    if (which !== this.current) return; // stale report from an old line
    const next = this.other();
    log('Pool ' + which + ' (' + POOLS[which].host + ':' + POOLS[which].port + ') failed. Switching to ' + next + ' pool (' + POOLS[next].host + ':' + POOLS[next].port + ').');
    this.current = next;
    sessions.bounceAll('pool switch');
    this.armProbe();
  },

  armProbe() {
    if (this.probeTimer) { clearInterval(this.probeTimer); this.probeTimer = null; }
    if (this.current !== 'backup') return;
    this.probeTimer = setInterval(() => this.probeMain(), PROBE_MS);
    if (this.probeTimer.unref) this.probeTimer.unref();
  },

  probeMain() {
    if (this.current !== 'backup' || this.probing) return;
    this.probing = true;
    const main = POOLS.main;
    const probe = net.connect(main.port, main.host);
    let settled = false;
    const done = (ok) => {
      if (settled) return;
      settled = true;
      probe.destroy();
      this.probing = false;
      if (ok && this.current === 'backup') {
        this.current = 'main';
        log('Back on the main pool.');
        sessions.bounceAll('back on main pool');
        this.armProbe();
      }
    };
    probe.setTimeout(CONNECT_TIMEOUT_MS);
    probe.once('connect', () => done(true));
    probe.once('timeout', () => done(false));
    probe.once('error', () => done(false));
  },
};

/* ---------------- sessions ----------------
 * One session per miner: one WebSocket + one TCP line to the pool.
 * Messages pass through byte-faithful: whatever the miner sends goes
 * to the pool exactly as sent (a trailing newline is added only if
 * the miner left it off — Stratum is newline-delimited, so the line
 * must end somewhere); every complete line the pool sends becomes one
 * WebSocket message to that miner, and only that miner.
 */

const sessions = {
  set: new Set(),

  count() { return this.set.size; },

  add(session) {
    this.set.add(session);
    log('Miner connected from ' + session.remote + '. Miners connected: ' + this.count());
  },

  remove(session) {
    if (this.set.delete(session)) {
      log('Miner gone (' + session.remote + '). Miners connected: ' + this.count());
    }
  },

  bounceAll(reason) {
    for (const session of Array.from(this.set)) session.closeMiner(1012, reason);
  },
};

class Session {
  constructor(minerSocket, remote) {
    this.miner = minerSocket;
    this.remote = remote;
    this.poolSocket = null;
    this.poolName = null;
    this.poolBuffer = '';
    this.dead = false;

    minerSocket.onMessage((text) => this.toPool(text));
    minerSocket.onClose(() => this.teardown());
    sessions.add(this);
    this.connectPool();
  }

  connectPool() {
    const pool = poolManager.pool();
    this.poolName = pool.name;
    const socket = net.connect(pool.port, pool.host);
    this.poolSocket = socket;
    socket.setTimeout(0);

    const failTimer = setTimeout(() => {
      if (this.poolSocket === socket && !socket._swanReady) {
        log('Pool ' + pool.name + ' did not answer in time for ' + this.remote + '.');
        socket.destroy();
      }
    }, CONNECT_TIMEOUT_MS);
    if (failTimer.unref) failTimer.unref();

    socket.once('connect', () => {
      socket._swanReady = true;
      clearTimeout(failTimer);
    });
    socket.on('data', (chunk) => this.toMiner(chunk));
    socket.once('error', () => this.poolDied(socket));
    socket.once('close', () => this.poolDied(socket));
  }

  poolDied(socket) {
    if (this.dead || this.poolSocket !== socket) return;
    this.poolSocket = null;
    // The pool line is gone; this miner's Stratum session died with
    // it. Tell the pool manager (it may flip pools) and close the
    // miner kindly so it reconnects at once onto a live pool line.
    poolManager.poolFailed(this.poolName);
    this.closeMiner(1012, 'pool line lost');
  }

  toPool(text) {
    const socket = this.poolSocket;
    if (!socket || socket.destroyed || !socket._swanReady) {
      // Pool line not up yet (or mid-switch): the miner's messages
      // have nowhere true to go. Drop them rather than fake a queue —
      // the miner's own retry re-sends its handshake on reconnect.
      return;
    }
    socket.write(text.endsWith('\n') ? text : text + '\n');
  }

  toMiner(chunk) {
    this.poolBuffer += chunk.toString('utf8');
    let idx;
    while ((idx = this.poolBuffer.indexOf('\n')) >= 0) {
      const line = this.poolBuffer.slice(0, idx).replace(/\r$/, '');
      this.poolBuffer = this.poolBuffer.slice(idx + 1);
      if (line.length > 0) this.miner.sendText(line);
    }
  }

  closeMiner(code, reason) {
    if (this.dead) return;
    this.dead = true;
    try { this.miner.close(code, reason); } catch (e) { /* already gone */ }
    this.teardownPool();
    sessions.remove(this);
  }

  teardown() {
    if (this.dead) { this.teardownPool(); sessions.remove(this); return; }
    this.dead = true;
    this.teardownPool();
    sessions.remove(this);
  }

  teardownPool() {
    if (this.poolSocket) {
      try { this.poolSocket.destroy(); } catch (e) { /* ignore */ }
      this.poolSocket = null;
    }
  }
}

/* ---------------- WebSocket side ----------------
 * The small slice of RFC 6455 the miners use: an HTTP upgrade
 * handshake, then text frames. Client frames arrive masked; server
 * frames go out unmasked. Ping gets a pong, close gets a close, and
 * fragmented messages are reassembled before they are relayed.
 */

const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

class MinerSocket {
  constructor(socket) {
    this.socket = socket;
    this.buffer = Buffer.alloc(0);
    this.handshaken = false;
    this.fragments = [];
    this.closed = false;
    this.messageHandler = null;
    this.closeHandler = null;

    socket.on('data', (chunk) => this.onData(chunk));
    socket.on('error', () => this.fireClose());
    socket.on('close', () => this.fireClose());
  }

  onMessage(fn) { this.messageHandler = fn; }
  onClose(fn) { this.closeHandler = fn; }

  onData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    if (!this.handshaken) {
      const end = this.buffer.indexOf('\r\n\r\n');
      if (end < 0) {
        if (this.buffer.length > 16384) this.socket.destroy();
        return;
      }
      const head = this.buffer.slice(0, end).toString('latin1');
      this.buffer = this.buffer.slice(end + 4);
      if (!this.doHandshake(head)) { this.socket.destroy(); return; }
      this.handshaken = true;
    }
    this.drainFrames();
  }

  doHandshake(head) {
    const keyMatch = /sec-websocket-key:\s*(\S+)/i.exec(head);
    if (!keyMatch || !/^GET\s/i.test(head)) return false;
    const accept = crypto.createHash('sha1')
      .update(keyMatch[1] + WS_GUID)
      .digest('base64');
    this.socket.write(
      'HTTP/1.1 101 Switching Protocols\r\n' +
      'Upgrade: websocket\r\n' +
      'Connection: Upgrade\r\n' +
      'Sec-WebSocket-Accept: ' + accept + '\r\n' +
      '\r\n'
    );
    return true;
  }

  drainFrames() {
    for (;;) {
      const frame = this.readFrame();
      if (!frame) return;
      const { fin, opcode, payload } = frame;
      if (opcode === 0x8) { // close
        let code = 1000;
        if (payload.length >= 2) code = payload.readUInt16BE(0);
        this.sendFrame(0x8, payload.slice(0, 2));
        this.socket.end();
        this.fireClose(code);
        return;
      }
      if (opcode === 0x9) { this.sendFrame(0xA, payload); continue; } // ping -> pong
      if (opcode === 0xA) continue; // pong: nothing to do
      if (opcode === 0x1 || opcode === 0x2 || opcode === 0x0) {
        if (opcode !== 0x0) this.fragments = [payload];
        else this.fragments.push(payload);
        if (fin) {
          const whole = Buffer.concat(this.fragments).toString('utf8');
          this.fragments = [];
          if (this.messageHandler) this.messageHandler(whole);
        }
        continue;
      }
      // Unknown opcode: protocol error.
      this.close(1002, 'bad opcode');
      return;
    }
  }

  readFrame() {
    const buf = this.buffer;
    if (buf.length < 2) return null;
    const fin = (buf[0] & 0x80) !== 0;
    const opcode = buf[0] & 0x0f;
    const masked = (buf[1] & 0x80) !== 0;
    let len = buf[1] & 0x7f;
    let offset = 2;
    if (len === 126) {
      if (buf.length < offset + 2) return null;
      len = buf.readUInt16BE(offset); offset += 2;
    } else if (len === 127) {
      if (buf.length < offset + 8) return null;
      const big = buf.readBigUInt64BE(offset); offset += 8;
      if (big > BigInt(16 * 1024 * 1024)) { this.socket.destroy(); return null; }
      len = Number(big);
    }
    let mask = null;
    if (masked) {
      if (buf.length < offset + 4) return null;
      mask = buf.slice(offset, offset + 4); offset += 4;
    }
    if (buf.length < offset + len) return null;
    let payload = buf.slice(offset, offset + len);
    if (mask) {
      payload = Buffer.from(payload);
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
    }
    this.buffer = buf.slice(offset + len);
    return { fin, opcode, payload };
  }

  sendFrame(opcode, payload) {
    if (this.closed || this.socket.destroyed) return;
    const data = Buffer.isBuffer(payload) ? payload : Buffer.from(String(payload), 'utf8');
    let head;
    if (data.length < 126) {
      head = Buffer.from([0x80 | opcode, data.length]);
    } else if (data.length < 65536) {
      head = Buffer.alloc(4);
      head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(data.length, 2);
    } else {
      head = Buffer.alloc(10);
      head[0] = 0x80 | opcode; head[1] = 127; head.writeBigUInt64BE(BigInt(data.length), 2);
    }
    this.socket.write(Buffer.concat([head, data]));
  }

  sendText(text) { this.sendFrame(0x1, text); }

  close(code, reason) {
    if (this.closed) return;
    const payload = Buffer.alloc(2);
    payload.writeUInt16BE(code || 1000, 0);
    this.sendFrame(0x8, payload); // send BEFORE the closed flag: sendFrame refuses once closed
    this.closed = true;
    this.socket.end();
    this.fireClose(code);
  }

  fireClose(code) {
    if (this._closeFired) return;
    this._closeFired = true;
    this.closed = true;
    if (this.closeHandler) this.closeHandler(code);
  }
}

/* ---------------- main ---------------- */

const server = net.createServer((rawSocket) => {
  rawSocket.setNoDelay(true);
  const remote = rawSocket.remoteAddress + ':' + rawSocket.remotePort;
  const miner = new MinerSocket(rawSocket);
  // A session is born the moment the miner's socket opens; it holds
  // its pool line ready so the miner's first message goes straight
  // through — no delay on the first attempt.
  new Session(miner, remote);
});

server.on('error', (err) => {
  log('Bridge server error: ' + err.message);
});

server.listen(PORT, () => {
  const main = POOLS.main, backup = POOLS.backup;
  log('SWAN BRIDGE IS RUNNING');
  log('Miners on this device / network: ws://127.0.0.1:' + PORT + ' (binds every interface, so the Wi-Fi address works too)');
  log('Pool: main ' + main.host + ':' + main.port + ' - backup ' + backup.host + ':' + backup.port);
  log('Relay only: this bridge holds no wallet, no keys, no money.');
});

process.on('SIGINT', () => { log('Swan Bridge stopping.'); process.exit(0); });
process.on('SIGTERM', () => { log('Swan Bridge stopping.'); process.exit(0); });
