// Launch sniper for the pump.fun bonding-curve replay (agent-fun/replay/v2).
//
// Idea: most new pump.fun coins bleed out within minutes, so the bot only ever
// buys a small, fixed stake (0.02 SOL) in a launch that has already survived its
// first seconds without looking like a dump, and it gets out fast. A launch
// qualifies only when, 20-150 slots after its CreateEvent:
//   - its curve holds 0.3-30 SOL of real SOL (too small = no market, too big = the
//     move already happened),
//   - the creator's own buys total at most 2.5 SOL and the creator has not sold,
//   - at most 3 distinct non-creator wallets have bought (a launch bundled by many
//     wallets is a dump waiting to happen),
//   - net SOL flow over the last 10 slots is at least +0.05 SOL (buyers are still
//     stepping in, not leaving).
// Exits: sell half at +10% (bank the move), sell the rest at +25%, cut the whole
// position at -6%, exit on the creator selling, and never hold past 50 slots.
// One entry per coin, at most 5 open positions, no shorts, no leverage.
//
// Pure function of the batches the harness sends: no clock, no randomness, no
// I/O, integer (bigint) money math, and iteration only over insertion-ordered
// maps fed by the batch stream, so the two graded executions produce
// byte-identical action logs.

import type { Batch, BotState, OrderInput, Pubkey } from './vendor/makefun-bot.ts';

const LAMPORTS = 1_000_000_000n;
const BPS = 10_000n;
const CURVE_FEE_BPS = 125n; // protocol 95 + creator 30 bps charged on every trade.

const DEFAULTS = {
  stake: 20_000_000n, // 0.02 SOL per coin.
  minBuy: 10_000_000n, // 0.01 SOL: every buy must clear the guardrail's round-trip bar.
  minAge: 20, // slots after CreateEvent before a buy may be decided.
  maxAge: 150, // slots after CreateEvent after which a first buy is too late.
  minRealSol: 300_000_000n, // 0.3 SOL of real reserves at decision time.
  maxRealSol: 30_000_000_000n, // 30 SOL.
  maxDevBuy: 2_500_000_000n, // 2.5 SOL of creator buys, at most.
  maxBuyers: 3, // distinct non-creator buyers, at most.
  flowWindow: 10, // slots of recent tape the entry flow is measured over.
  minFlow: 50_000_000n, // +0.05 SOL net inflow over the flow window.
  buySlippageBps: 200, // tight: refuse a buy whose price jumped in the latency.
  takeProfitBps: 2500, // sell the rest at +25% over cost.
  stopLossBps: 600, // sell everything at -6% under cost.
  partialBps: 1000, // sell half the position at +10% over cost.
  partialShareBps: 5000, // ...that half is 50% of the tokens.
  maxHold: 50, // slots; a position that never moves gets closed.
};

interface Tick {
  slot: number;
  buy: boolean;
  sol: bigint;
  user: Pubkey;
}

interface Launch {
  createSlot: number;
  creator: Pubkey;
  devBuy: bigint;
  devSold: boolean;
  buyers: Set<Pubkey>;
  bought: bigint;
  soldByOthers: bigint;
  recent: Tick[];
  done: boolean;
}

interface Held {
  openedSlot: number;
  tookPartial: boolean;
  bestValue: bigint;
}

function sellValue(tokens: bigint, vq: bigint, vt: bigint): bigint {
  if (tokens <= 0n || vt <= 0n) return 0n;
  const q = (tokens * vq) / (vt + tokens);
  return q - (q * CURVE_FEE_BPS + BPS - 1n) / BPS;
}

export function createBot(overrides: Partial<typeof DEFAULTS> = {}) {
  const P = { ...DEFAULTS, ...overrides };
  const STAKE = P.stake;
  const MIN_BUY = P.minBuy;
  const MIN_AGE = P.minAge;
  const MAX_AGE = P.maxAge;
  const MIN_REAL_SOL = P.minRealSol;
  const MAX_REAL_SOL = P.maxRealSol;
  const MAX_DEV_BUY = P.maxDevBuy;
  const MAX_BUYERS = P.maxBuyers;
  const FLOW_WINDOW = P.flowWindow;
  const MIN_FLOW = P.minFlow;
  const BUY_SLIPPAGE_BPS = P.buySlippageBps;
  const TAKE_PROFIT_BPS = P.takeProfitBps;
  const STOP_LOSS_BPS = P.stopLossBps;
  const PARTIAL_BPS = P.partialBps;
  const PARTIAL_SHARE_BPS = P.partialShareBps;
  const MAX_HOLD = P.maxHold;
  const launches = new Map<Pubkey, Launch>();
  const held = new Map<Pubkey, Held>();
  const selling = new Set<Pubkey>();

  const launchOf = (mint: Pubkey): Launch => {
    let l = launches.get(mint);
    if (l === undefined) {
      l = {
        createSlot: -1,
        creator: '',
        devBuy: 0n,
        devSold: false,
        buyers: new Set(),
        bought: 0n,
        soldByOthers: 0n,
        recent: [],
        done: false,
      };
      launches.set(mint, l);
    }
    return l;
  };

  const recentFlow = (l: Launch, now: number): bigint => {
    // Drop ticks older than the flow window before summing (lazy pruning, so
    // no pass over every launch is needed on every batch).
    let cut = 0;
    while (cut < l.recent.length && l.recent[cut]!.slot <= now - FLOW_WINDOW) cut++;
    if (cut > 0) l.recent.splice(0, cut);
    let net = 0n;
    for (const t of l.recent) net += t.buy ? t.sol : -t.sol;
    return net;
  };

  return {
    onBatch(state: BotState, batch: Batch): OrderInput[] {
      const now = batch.slot;
      const tx = state.txFeeLamports();
      const cap = state.hello.caps.max_orders_per_batch;
      const orders: OrderInput[] = [];

      // Fold the batch's events into the launch tapes.
      for (const e of batch.events) {
        if (e.event === 'CreateEvent') {
          const l = launchOf(e.mint);
          l.createSlot = e.slot;
          l.creator = e.creator;
        } else if (e.event === 'TradeEvent') {
          const l = launchOf(e.mint);
          const sol = BigInt(e.sol_amount);
          l.recent.push({ slot: e.slot, buy: e.is_buy, sol, user: e.user });
          if (e.is_buy) {
            l.bought += sol;
            // Only the creator's buys in the create slot count as the dev buy:
            // a creator topping up later is a different signal.
            if (e.user === l.creator && e.slot === l.createSlot) l.devBuy += sol;
            else if (e.user !== l.creator) l.buyers.add(e.user);
          } else if (e.user === l.creator) {
            l.devSold = true;
          } else {
            l.soldByOthers += sol;
          }
        } else {
          // CompleteEvent: the curve is done; never buy it again.
          launchOf(e.mint).done = true;
        }
      }
      // Fills: a sell in flight has landed, so the mint may be bought again later
      // (it cannot: done is set on exit) and partial-sale bookkeeping can clear.
      for (const f of batch.fills) {
        if (f.status === 'filled' && f.side === 'sell') selling.delete(f.mint);
      }

      // Latest reserves per mint, preferring the batch's marks.
      const marks = new Map(batch.marks.map((m) => [m.mint, m] as const));
      const reservesOf = (mint: Pubkey): { vq: bigint; vt: bigint; rq: bigint } | null => {
        const m = marks.get(mint);
        if (m !== undefined) {
          return { vq: BigInt(m.virtual_quote_reserves), vt: BigInt(m.virtual_token_reserves), rq: BigInt(m.real_quote_reserves) };
        }
        const c = state.curves.get(mint);
        if (c === undefined) return null;
        return { vq: BigInt(c.virtual_quote_reserves), vt: BigInt(c.virtual_token_reserves), rq: BigInt(c.real_quote_reserves) };
      };

      // Exits first: every held position is re-priced and sold whole on the first
      // of take-profit, stop-loss, creator sell, or the hold limit. At +10% half
      // the tokens are sold and the rest rides to the full take-profit.
      for (const [mint, h] of held) {
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        if (pos === undefined) {
          // Do not drop the entry while its buy is still in flight: the
          // position only appears in the account once the buy fills, two
          // slots after the decision. Dropping it here would orphan the
          // position (it would never be exited) and pin all 5 slots.
          let buyPending = false;
          for (const p of state.pending.values()) {
            if (p.order.mint === mint && p.order.side === 'buy') buyPending = true;
          }
          if (!buyPending && !selling.has(mint)) {
            held.delete(mint);
            launchOf(mint).done = true;
          }
          continue;
        }
        if (curve === undefined || curve.complete || selling.has(mint)) continue;
        const r = reservesOf(mint);
        if (r === null) continue;
        const tokens = BigInt(pos.token_amount);
        const cost = BigInt(pos.cost_lamports) + tx; // basis includes the buy's tx fee.
        const value = sellValue(tokens, r.vq, r.vt) - tx;
        if (value > h.bestValue) h.bestValue = value;
        const l = launchOf(mint);
        let exitAll = false;
        if (value * BPS >= cost * (BPS + BigInt(TAKE_PROFIT_BPS))) exitAll = true;
        else if (value * BPS <= cost * (BPS - BigInt(STOP_LOSS_BPS))) exitAll = true;
        else if (l.devSold) exitAll = true;
        else if (now - h.openedSlot > MAX_HOLD) exitAll = true;
        if (exitAll) {
          if (orders.length >= cap) break;
          orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
          selling.add(mint);
          continue;
        }
        if (!h.tookPartial && value * BPS >= cost * (BPS + BigInt(PARTIAL_BPS))) {
          const half = (tokens * BigInt(PARTIAL_SHARE_BPS)) / BPS;
          if (half > 0n && orders.length < cap) {
            orders.push({ side: 'sell', mint, token_amount: half.toString(), max_slippage_bps: 10_000 });
            selling.add(mint);
            h.tookPartial = true;
          }
        }
      }

      // Entries: one small buy per qualifying launch. Every tracked launch is
      // re-checked on every batch in first-seen order (a launch may sit quiet
      // for batches at a time; only its age window matters).
      let free = state.freeCashLamports();
      let slotsLeft = state.hello.caps.max_open_positions - state.committedPositionCount();
      const pendingMints = new Set<Pubkey>();
      for (const p of state.pending.values()) pendingMints.add(p.order.mint);
      for (const [mint, l] of launches) {
        if (orders.length >= cap || slotsLeft <= 0) break;
        if (l.done || l.createSlot < 0) continue;
        if (held.has(mint) || pendingMints.has(mint) || selling.has(mint)) continue;
        const age = now - l.createSlot;
        if (age < MIN_AGE) continue;
        // Past the entry window for good: drop the launch so the per-batch
        // scan stays over live launches only (the final window holds ~16x the
        // dev window's coins; scanning finished launches every batch would
        // blow the wall budget).
        if (age > MAX_AGE) {
          l.done = true;
          continue;
        }
        if (l.devSold) continue;
        if (l.devBuy > MAX_DEV_BUY) continue;
        if (l.buyers.size > MAX_BUYERS) continue;
        if (l.bought > 0n && l.soldByOthers > l.bought) continue;
        const curve = state.curves.get(mint);
        if (curve === undefined || curve.complete) continue;
        const r = reservesOf(mint);
        if (r === null) continue;
        if (r.rq < MIN_REAL_SOL || r.rq > MAX_REAL_SOL) continue;
        if (recentFlow(l, now) < MIN_FLOW) continue;
        const sizeCap = state.maxBuyLamports(mint);
        let amount = STAKE < sizeCap ? STAKE : sizeCap;
        if (amount < MIN_BUY) continue;
        if (free < amount + tx) continue;
        orders.push({ side: 'buy', mint, quote_lamports: amount, max_slippage_bps: BUY_SLIPPAGE_BPS });
        free -= amount + tx;
        slotsLeft -= 1;
        pendingMints.add(mint);
        held.set(mint, { openedSlot: now, tookPartial: false, bestValue: 0n });
        l.done = true;
      }
      return orders;
    },
  };
}
