// ==================== 大富翁辅助计钱工具 服务器 v2 ====================
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const os = require('os');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  maxHttpBufferSize: 1e5, // 100KB 防止大payload攻击
  pingInterval: 25000,
  pingTimeout: 20000,
});
app.use(express.static('public'));
app.use(express.json());

// ==================== 全局异常处理 ====================
process.on('uncaughtException', (err) => {
  console.error('未捕获异常:', err.message);
});

// ==================== 辅助函数 ====================
function genToken() { return crypto.randomBytes(8).toString('hex'); }
function genId()    { return crypto.randomBytes(4).toString('hex'); }
function genCode()  {
  let c, tries = 0;
  do {
    c = String(Math.floor(1000 + Math.random() * 9000));
    if (++tries > 1000) throw new Error('无法生成房间号');
  } while (rooms.has(c));
  return c;
}

function parseAmount(v, fallback = 0) {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return NaN;
  return Math.min(99999999, Math.floor(n));
}

function sanitize(s, maxLen) {
  return String(s || '').slice(0, maxLen).replace(/[<>&"'\n\r\\]/g, '').trim();
}

// ==================== 房间系统 ====================
const rooms = new Map();
const DEFAULT_MONEY = 1500;
const MAX_LOG_SIZE = 200;
const ROOM_MAX_AGE = 2 * 60 * 60 * 1000; // 2小时
const DISCONNECT_GRACE = 30000; // 30秒离线容忍

function findRoom(socketId) {
  for (const [, room] of rooms) {
    if (room.players.some(p => p.id === socketId)) return room;
  }
  return null;
}

function getPublic(p) {
  return {
    id: p.id, name: p.name, token: p.token,
    isOnline: p.isOnline !== false,
    isHost: p.isHost,
    money: p.money,
  };
}

function broadcastRoom(room, event, data) {
  io.to(room.code).emit(event, data);
}

function pushLog(room, entry) {
  room.dealLog.push(entry);
  if (room.dealLog.length > MAX_LOG_SIZE) {
    room.dealLog.splice(0, room.dealLog.length - MAX_LOG_SIZE);
  }
}

// ==================== Socket.IO ====================
io.on('connection', (socket) => {
  console.log(`连接: ${socket.id}`);

  // ---- 创建房间 ----
  socket.on('create-room', ({ name, initialMoney }) => {
    try {
      const safeName = sanitize(name, 8) || '玩家';
      const initMoney = parseAmount(initialMoney, DEFAULT_MONEY) || DEFAULT_MONEY;
      const room = {
        code: genCode(),
        players: [],
        hostId: socket.id,
        createdAt: Date.now(),
        dealLog: [],
        events: [],
        config: { initialMoney: initMoney },
      };
      const player = {
        id: socket.id, name: safeName,
        token: genToken(), isOnline: true, isHost: true,
        money: initMoney,
      };
      room.players.push(player);
      socket.join(room.code);
      rooms.set(room.code, room);

      console.log(`🏦 房间 ${room.code} 创建，房主: ${safeName}，初始: ¥${initMoney}`);
      socket.emit('room-created', {
        code: room.code, token: player.token,
        players: room.players.map(getPublic),
        dealLog: room.dealLog, events: room.events,
        config: room.config,
      });
    } catch (err) {
      console.error('创建房间失败:', err.message);
      socket.emit('error', { message: '创建房间失败，请重试' });
    }
  });

  // ---- 加入房间 ----
  socket.on('join-room', ({ code, name, token }) => {
    if (!/^\d{4}$/.test(String(code || ''))) {
      socket.emit('error', { message: '房间号格式错误（4位数字）' }); return;
    }
    const safeName = sanitize(name, 8);
    const room = rooms.get(code);
    if (!room) { socket.emit('error', { message: '房间不存在' }); return; }

    // Token 重连
    if (token) {
      const rp = room.players.find(p => p.token === token);
      if (rp) {
        rp.id = socket.id; rp.isOnline = true;
        if (safeName) rp.name = safeName;
        socket.join(code);
        socket.emit('reconnected', {
          token: rp.token, code,
          players: room.players.map(getPublic),
          dealLog: room.dealLog, events: room.events,
          config: room.config,
        });
        broadcastRoom(room, 'players-updated', { players: room.players.map(getPublic) });
        console.log(`${rp.name} 重连房间 ${code}`);
        return;
      }
    }

    if (room.players.length >= 8) {
      socket.emit('error', { message: '房间已满（8人）' }); return;
    }

    const player = {
      id: socket.id,
      name: safeName || '玩家' + (room.players.length + 1),
      token: genToken(), isOnline: true, isHost: false,
      money: room.config.initialMoney,
    };
    room.players.push(player);
    socket.join(code);

    socket.emit('room-joined', {
      code, token: player.token,
      players: room.players.map(getPublic),
      dealLog: room.dealLog, events: room.events,
      config: room.config,
    });
    socket.to(code).emit('player-joined', { players: room.players.map(getPublic) });
    console.log(`${player.name} 加入房间 ${code}`);
  });

  // ---- 金钱操作（统一处理）----
  function applyMoneyOp(type, amount, label, toPlayerId) {
    const room = findRoom(socket.id);
    if (!room) return;
    const p = room.players.find(pl => pl.id === socket.id);
    if (!p) return;

    const safeLabel = sanitize(label, 30);

    if (type === 'add') {
      p.money += amount;
    } else if (type === 'subtract') {
      p.money -= amount;
    } else if (type === 'transfer') {
      const to = room.players.find(pl => pl.id === toPlayerId);
      if (!to || to.id === p.id) { socket.emit('error', { message: '转账目标无效' }); return; }
      p.money -= amount;
      to.money += amount;
    }

    const entry = {
      ts: Date.now(),
      fromId: type === 'add' ? null : p.id,
      fromName: type === 'add' ? '🏦' : p.name,
      toId: type === 'subtract' ? null
        : (type === 'transfer' ? toPlayerId : p.id),
      toName: type === 'subtract' ? '🏦'
        : (type === 'transfer'
          ? (room.players.find(pl => pl.id === toPlayerId)?.name || '?') : p.name),
      amount, label: safeLabel, type,
    };
    pushLog(room, entry);

    broadcastRoom(room, 'money-changed', {
      players: room.players.map(getPublic),
      newLog: entry,
    });

    console.log(`${p.name} ${type === 'add' ? '+' : '-'}¥${amount}${safeLabel ? ' (' + safeLabel + ')' : ''} → 余额: ¥${p.money}`);
  }

  socket.on('add-money', ({ amount, label }) => {
    const amt = parseAmount(amount);
    if (!amt) { socket.emit('error', { message: '金额无效' }); return; }
    applyMoneyOp('add', amt, label);
  });

  socket.on('subtract-money', ({ amount, label }) => {
    const amt = parseAmount(amount);
    if (!amt) { socket.emit('error', { message: '金额无效' }); return; }
    applyMoneyOp('subtract', amt, label);
  });

  socket.on('transfer-money', ({ toPlayerId, amount, label }) => {
    if (!toPlayerId || typeof toPlayerId !== 'string') {
      socket.emit('error', { message: '请选择转账目标' }); return;
    }
    const amt = parseAmount(amount);
    if (!amt) { socket.emit('error', { message: '金额无效' }); return; }
    applyMoneyOp('transfer', amt, label, toPlayerId);
  });

  // ---- 快捷事件触发 ----
  socket.on('trigger-event', ({ eventId }) => {
    if (!eventId || typeof eventId !== 'string') return;
    const room = findRoom(socket.id);
    if (!room) return;
    const p = room.players.find(pl => pl.id === socket.id);
    if (!p) return;
    const ev = room.events.find(e => e.id === eventId);
    if (!ev) return;

    if (ev.category === 'income') {
      p.money += ev.amount;
    } else if (ev.category === 'expense') {
      p.money -= ev.amount;
    } else return;

    const entry = {
      ts: Date.now(),
      fromId: ev.category === 'income' ? null : p.id,
      fromName: ev.category === 'income' ? '🏦' : p.name,
      toId: ev.category === 'expense' ? null : p.id,
      toName: ev.category === 'expense' ? '🏦' : p.name,
      amount: ev.amount, label: ev.label, type: ev.category,
    };
    pushLog(room, entry);

    broadcastRoom(room, 'money-changed', {
      players: room.players.map(getPublic),
      newLog: entry,
    });
    console.log(`${p.name} 触发事件: ${ev.label} ${ev.category === 'income' ? '+' : '-'}¥${ev.amount}`);
  });

  // ---- 事件管理 ----
  socket.on('add-event', ({ label, amount, category }) => {
    const safeLabel = sanitize(label, 12);
    if (!safeLabel) { socket.emit('error', { message: '事件名称不能为空' }); return; }
    const amt = parseAmount(amount, 100);
    if (!amt) { socket.emit('error', { message: '金额无效' }); return; }
    if (!['income', 'expense'].includes(category)) {
      socket.emit('error', { message: '类型无效' }); return;
    }

    const room = findRoom(socket.id);
    if (!room) return;
    const p = room.players.find(pl => pl.id === socket.id);
    if (!p) return;

    // 防止单个房间事件过多
    if (room.events.length >= 50) {
      socket.emit('error', { message: '事件数量已达上限（50个）' }); return;
    }

    const ev = {
      id: genId(), label: safeLabel, amount: amt, category,
      creatorId: p.id, creatorName: p.name,
    };
    room.events.push(ev);

    broadcastRoom(room, 'events-updated', { events: room.events });
    console.log(`${p.name} 添加事件: ${safeLabel} ${category === 'income' ? '+' : '-'}${amt}`);
  });

  socket.on('remove-event', ({ eventId }) => {
    if (!eventId || typeof eventId !== 'string') return;
    const room = findRoom(socket.id);
    if (!room) return;
    const idx = room.events.findIndex(e => e.id === eventId);
    if (idx === -1) return;
    // 仅创建者可删除（非严格：允许任何在线玩家删除过期事件）
    room.events.splice(idx, 1);
    broadcastRoom(room, 'events-updated', { events: room.events });
  });

  // ---- 重置 ----
  socket.on('reset-game', () => {
    const room = findRoom(socket.id);
    if (!room) return;
    for (const p of room.players) p.money = room.config.initialMoney;
    room.dealLog = [];
    broadcastRoom(room, 'game-reset', {
      players: room.players.map(getPublic),
      dealLog: [],
    });
    console.log(`房间 ${room.code} 已重置`);
  });

  // ---- 改名 ----
  socket.on('rename', ({ name }) => {
    const safeName = sanitize(name, 8);
    if (!safeName) return;
    const room = findRoom(socket.id);
    if (!room) return;
    const player = room.players.find(p => p.id === socket.id);
    if (!player) return;
    player.name = safeName;
    broadcastRoom(room, 'players-updated', { players: room.players.map(getPublic) });
  });

  // ---- 断开连接 ----
  socket.on('disconnect', () => {
    const room = findRoom(socket.id);
    if (!room) return;
    const player = room.players.find(p => p.id === socket.id);
    if (!player) return;
    player.isOnline = false;
    broadcastRoom(room, 'players-updated', { players: room.players.map(getPublic) });

    // 宽限期后移除
    const code = room.code;
    setTimeout(() => {
      const r = rooms.get(code);
      if (!r) return;
      const p = r.players.find(pl => pl.id === socket.id);
      if (p && !p.isOnline) {
        const idx = r.players.indexOf(p);
        if (idx === -1) return;
        r.players.splice(idx, 1);
        if (r.players.length === 0) {
          rooms.delete(code);
          console.log(`房间 ${code} 已删除（无人）`);
        } else {
          if (r.hostId === p.id) {
            r.hostId = r.players[0].id;
            r.players[0].isHost = true;
          }
          broadcastRoom(r, 'player-left', { players: r.players.map(getPublic) });
        }
      }
    }, DISCONNECT_GRACE);
  });
});

// ==================== HTTP API ====================
app.get('/api/rooms', (req, res) => {
  const list = [];
  for (const [code, room] of rooms) {
    list.push({
      code, playerCount: room.players.length,
      players: room.players.map(p => ({ name: p.name, money: p.money, isOnline: p.isOnline !== false })),
      createdAt: room.createdAt,
      config: room.config,
    });
  }
  res.json(list);
});

// ==================== 启动 ====================
const PORT = 3000;

function getBestLocalIP() {
  const ifaces = os.networkInterfaces();
  const candidates = [];
  for (const [name, addrs] of Object.entries(ifaces)) {
    for (const iface of addrs) {
      if (iface.family === 'IPv4' && !iface.internal && /^(192\.168\.|10\.|172\.(1[6-9]|2\d|3[01])\.)/.test(iface.address)) {
        // 优先 WLAN/WiFi/以太网
        const priority = /wlan|wi-?fi|无线|ether|以太/i.test(name) ? 0 : 1;
        candidates.push({ ip: iface.address, priority });
      }
    }
  }
  candidates.sort((a, b) => a.priority - b.priority);
  return candidates.length > 0 ? candidates[0].ip : 'localhost';
}

server.listen(PORT, '0.0.0.0', () => {
  const ip = getBestLocalIP();
  console.log('🏦 大富翁计钱工具 v2');
  console.log(`  局域网: http://${ip}:${PORT}`);
  console.log(`  本机:   http://localhost:${PORT}`);
  console.log(`  初始资金 ¥${DEFAULT_MONEY} | 2-8人 | 日志上限${MAX_LOG_SIZE}条`);

  // 定期清理过期房间
  setInterval(() => {
    const now = Date.now();
    for (const [code, room] of rooms) {
      if (now - room.createdAt > ROOM_MAX_AGE) {
        broadcastRoom(room, 'room-closed', { reason: '房间超时自动关闭' });
        rooms.delete(code);
        console.log(`房间 ${code} 已清理（超时）`);
      }
    }
  }, 10 * 60 * 1000);
});
