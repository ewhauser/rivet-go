import {
  actor,
  type RivetMessageEvent,
  setup,
  type UniversalWebSocket,
} from "rivetkit";
import { db } from "rivetkit/db";

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

type TodoRequest = {
  kind: "select" | "insert" | "transaction";
  title: string;
};

const todo = actor({
  state: {},
  db: db({
    warnOnManualTransactions: false,
    onMigrate: async (database) => {
      await database.execute(
        "CREATE TABLE IF NOT EXISTS todos (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL UNIQUE, done INTEGER NOT NULL DEFAULT 0)",
      );
      await database.execute(
        "INSERT OR IGNORE INTO todos(id, title, done) VALUES (1, 'seed', 0)",
      );
    },
  }),
  onRequest: async (c, request): Promise<Response> => {
    const path = new URL(request.url).pathname;
    if (path === "/count") {
      const rows = await c.db.execute<{ count: number }>(
        "SELECT COUNT(*) AS count FROM todos",
      );
      return Response.json({ count: Number(rows[0]?.count ?? -1) });
    }
    if (path !== "/op") {
      return Response.json({ error: `unknown todo path ${path}` }, { status: 404 });
    }
    const input = (await request.json()) as TodoRequest;
    switch (input.kind) {
      case "select": {
        const rows = await c.db.execute<{ id: number }>(
          "SELECT id, title, done FROM todos WHERE id = ?",
          1,
        );
        if (rows.length !== 1) {
          throw new Error(`point SELECT returned ${rows.length} rows`);
        }
        break;
      }
      case "insert":
        await c.db.execute(
          "INSERT INTO todos(title, done) VALUES (?, ?)",
          input.title,
          0,
        );
        break;
      case "transaction":
        await c.db.transaction(async (tx) => {
          await tx.execute(
            "INSERT INTO todos(title, done) VALUES (?, ?)",
            input.title,
            0,
          );
          await tx.execute(
            "UPDATE todos SET done = 1 WHERE title = ?",
            input.title,
          );
          const rows = await tx.execute<{ done: number }>(
            "SELECT done FROM todos WHERE title = ?",
            input.title,
          );
          if (rows.length !== 1 || Number(rows[0]?.done) !== 1) {
            throw new Error("transaction SELECT did not observe its UPDATE");
          }
        });
        break;
      default:
        throw new Error(`unknown todo operation ${JSON.stringify(input.kind)}`);
    }
    return Response.json({ count: 1 });
  },
});

const registry = setup({
  use: { counter, echo, todo },
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
