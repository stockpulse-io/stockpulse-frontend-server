require('dotenv').config();
const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');
const { createClient } = require('redis');

const server = http.createServer();
const io = new Server(server, { cors: { origin: "*" } });

const pool = new Pool({
  host: process.env.PG_HOST || 'localhost',
  user: process.env.PG_USER || 'stockpulse_user',
  password: process.env.PG_PASSWORD || 'abcd1234',
  database: process.env.PG_DB || 'stockpulse',
  port: process.env.PG_PORT || 5432,
});

const redisSubscriber = createClient({ url: `redis://${process.env.REDIS_HOST || 'localhost'}:${process.env.REDIS_PORT || 6379}` });
const redisClient = createClient({ url: `redis://${process.env.REDIS_HOST || 'localhost'}:${process.env.REDIS_PORT || 6379}` });

// Buffer for throttling Market Watch updates
const tickBuffer = new Map();

(async () => {
  redisSubscriber.on('error', (err) => console.error('Redis subscriber error', err));
  redisClient.on('error', (err) => console.error('Redis client error', err));

  await redisSubscriber.connect();
  await redisClient.connect();
  console.log('✅ Connected to Redis');

  // Subscribe to Redis: keep handler minimal and use volatile emit
  redisSubscriber.subscribe('live_ticks', (message) => {
    try {
      const tick = JSON.parse(message);
      // keep latest tick per symbol in buffer for market watch
      tickBuffer.set(tick.symbol, tick);

      // emit per-tick to stock room with volatile (no retransmit/backpressure)
      io.to(tick.symbol).volatile.emit('tick', {
        symbol: tick.symbol,
        price: tick.price,
        event_time: tick.event_time,
        percent_price_change: tick.percent_price_change // if producer includes it
      });
    } catch (e) {
      console.error("Error parsing tick:", e);
    }
  });
})();

// Broadcast Market Watch updates every 1 second (Throttling)
setInterval(() => {
  if (tickBuffer.size > 0) {
    const updates = Array.from(tickBuffer.values()).map(t => {
      // Parse price safely
      const price = (t.price !== undefined && t.price !== null) ? parseFloat(t.price) : 0;

      // Prefer a percent field if producer already added it.
      // Some producers attach `percent_price_change` or similarly named field.
      const percentPriceChange = (() => {
        if (t.percent_price_change !== undefined && t.percent_price_change !== null) return Number(t.percent_price_change);
        if (t.percentChange !== undefined && t.percentChange !== null) return Number(t.percentChange);
        // leave undefined if not present
        return undefined;
      })();

      // Optional: include open1m if tick contains it (useful for client to compute if percent absent)
      const open1m = (t.open1m !== undefined && t.open1m !== null) ? Number(t.open1m) : undefined;

      return {
        symbol: t.symbol,
        price,
        event_time: t.event_time,
        // include change info if present (client will prefer this)
        percent_price_change: percentPriceChange,
        // include open1m if available
        open1m
      };
    });


    io.to('market_watch').emit('market_update', updates);
    tickBuffer.clear();
  }
}, 1000);

io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);

  socket.on('request_market_data', async (callback) => {
    try {
      // Fetch latest candle to establish the "Open" price of the current minute
      const query = `
        SELECT DISTINCT ON (symbol) symbol, open, open_time 
        FROM candles 
        ORDER BY symbol, open_time DESC;
      `;
      const result = await pool.query(query);
      
      const stocks = await Promise.all(result.rows.map(async (row) => {
        const tickStr = await redisClient.get(`live:${row.symbol}`);
        const tick = tickStr ? JSON.parse(tickStr) : null;
        
        const currentPrice = tick ? parseFloat(tick.price) : 0;
        const openPrice = parseFloat(row.open);
        
        // Calculate initial 1m change
        let change1m = 0;
        if (openPrice > 0 && currentPrice > 0) {
          change1m = ((currentPrice - openPrice) / openPrice) * 100;
        }

        return {
          symbol: row.symbol,
          price: currentPrice,
          change: change1m,
          open1m: openPrice,
          openTime: parseInt(row.open_time) // Send timestamp of the open candle
        };
      }));
      
      callback({ status: 'ok', data: stocks });
    } catch (err) {
      console.error(err);
      callback({ status: 'error', message: 'Failed to fetch data' });
    }
  });

  socket.on('request_history', async (symbol, callback) => {
    try {
      const result = await pool.query(`
        SELECT * FROM candles WHERE symbol = $1 ORDER BY open_time ASC LIMIT 1000
      `, [symbol]);
      callback({ status: 'ok', data: result.rows });
    } catch (err) {
      console.error(err);
      callback({ status: 'error', message: 'Failed to fetch history' });
    }
  });

  socket.on('join_market_watch', () => socket.join('market_watch'));
  socket.on('leave_market_watch', () => socket.leave('market_watch'));
  socket.on('join_stock', (symbol) => socket.join(symbol));
  socket.on('leave_stock', (symbol) => socket.leave(symbol));
});

server.listen(4000, () => {
  console.log('🚀 Backend Service running on port 4000');
});