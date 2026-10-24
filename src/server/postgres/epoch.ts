import { Effect } from "effect"
import { SqlClient } from "effect/sql/SqlClient"
import { Protocol } from "echo-sync"

/** Restore is offline: rotate before reopening traffic, never during live requests (SPEC §11). */
export const checkEpoch = Effect.fnUntraced(function* (presented: string | null | undefined) {
  const sql = yield* SqlClient
  const rows = yield* sql<{ epoch: string; require_epoch: boolean }>`
    SELECT epoch, require_epoch FROM echo_sync.engine_state`
  const state = rows[0]
  if (state === undefined) return yield* Effect.die("echo_sync.engine_state is empty; run migrate")
  if (presented === undefined) {
    if (state.require_epoch)
      return yield* new Protocol.ClientOutdated({ message: "engine epoch required; update client" })
  } else if (presented !== state.epoch) {
    return yield* new Protocol.ResnapshotRequired({ reason: "unknownCursor", epoch: state.epoch })
  }
  return state.epoch
})
