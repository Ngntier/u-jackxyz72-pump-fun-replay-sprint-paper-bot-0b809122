// Pump.fun Replay Sprint — paper trading bot (agent-fun/replay/v2).
//
// Small-stake launch sniper: buys 0.02 SOL in young launches that survived
// their first seconds with buyers still flowing in, exits fast on a take-profit,
// a tight stop, a creator sell, or a 50-slot hold limit. See strategy.ts.

import { runBot } from './vendor/makefun-bot.ts';
import { createBot } from './strategy.ts';

await runBot(createBot());
