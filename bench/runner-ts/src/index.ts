import {
  actor,
  type RivetMessageEvent,
  setup,
  type UniversalWebSocket,
} from "rivetkit";

type CounterVars = { count: number };

const persistMode = process.env.BENCH_PERSIST_MODE ?? "persist";
if (persistMode !== "persist" && persistMode !== "no-persist") {
  throw new Error(`unsupported BENCH_PERSIST_MODE ${JSON.stringify(persistMode)}`);
}

const counter = actor({
  state: { count: 0 },
  createVars: (): CounterVars => ({ count: 0 }),
  actions: {
    increment: async (c, amount: number): Promise<number> => {
      if (!Number.isSafeInteger(amount)) {
        throw new Error("amount must be a safe integer");
      }
      if (persistMode === "persist") {
        c.state.count += amount;
        // Proxy mutations request a deferred save automatically. Immediate
        // save makes durability part of the measured action response, matching
        // Go's successful-action adapter and the Rust benchmark action.
        await c.saveState({ immediate: true });
        return c.state.count;
      }
      c.vars.count += amount;
      return c.vars.count;
    },
    get: (c): number =>
      persistMode === "persist" ? c.state.count : c.vars.count,
  },
});

const echo = actor({
  state: {},
  onWebSocket: async (_c, websocket: UniversalWebSocket): Promise<void> => {
    websocket.addEventListener("message", (event: RivetMessageEvent) => {
      websocket.send(event.data);
    });
  },
});

const registry = setup({
  use: { counter, echo },
  endpoint: process.env.RIVET_ENDPOINT ?? "http://127.0.0.1:6420",
  token: "dev",
  namespace: "default",
  noWelcome: true,
  logging: { level: "error" },
  envoy: {
    poolName: process.env.BENCH_RUNNER_NAME ?? `bench-ts-${persistMode}`,
    totalSlots: 100_000,
    version: 1,
  },
});

registry.start();
