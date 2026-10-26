import { Effect, Option, Stream } from "effect"
import type { Protocol } from "../../../core/index.ts"

/** Adapt scripted unary pages without eagerly requesting the next continuation or repeating Done. */
export const paginate =
  <E, R>(page: (request: Protocol.PullRequest) => Effect.Effect<Protocol.PullResponse, E, R>) =>
  (request: Protocol.PullRequest): Stream.Stream<Protocol.PullResponse, E, R> =>
    Stream.paginate(request, (next) =>
      Effect.map(
        Effect.suspend(() => page(next)),
        (response) => [
          [response],
          response._tag === "PullPage"
            ? Option.some({
                ...next,
                ...(response.epoch === undefined ? {} : { epoch: response.epoch }),
                continuation: Option.some(response.continuation)
              })
            : Option.none()
        ]
      )
    )

/** Unary assertions must fail, rather than silently succeed, when a stream has no response. */
export const last = <A, E, R>(stream: Stream.Stream<A, E, R>) =>
  Stream.runLast(stream).pipe(
    Effect.flatMap(
      Option.match({ onNone: () => Effect.die("pull stream ended without a response"), onSome: Effect.succeed })
    )
  )
