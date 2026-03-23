import "dotenv/config";

import { IGDBClient } from "./igdb/client.js";
import { IGDBGame } from "./igdb/types.js";

async function main() {
  // Validate environment
  const { TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET } = process.env;

  if (!TWITCH_CLIENT_ID || !TWITCH_CLIENT_SECRET) {
    throw new Error("Missing TWITCH_CLIENT_ID or TWITCH_CLIENT_SECRET in .env");
  }

  // Initialize clients
  const igdb = new IGDBClient(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET);
  await igdb.authenticate();

  // Register graceful shutdown — save state on SIGINT/SIGTERM
  const shutdown = () => {
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  // const queryResult = await igdb.query<IGDBGame>("games", `fields release_dates; where id = 1020;`);

  const queryResult = await igdb.query<IGDBGame>(
    "platforms",
    `fields *; where id = 48;`,
  );

  console.log(queryResult);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
