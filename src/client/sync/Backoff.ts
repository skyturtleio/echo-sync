import { Duration, Effect, Schedule } from "effect"

/** Exponential retry delays in milliseconds, capped after the first wait. */
export const cappedExponential = (base: number, cap: number) =>
  Schedule.exponential(base).pipe(
    // The first delay is uncapped. Keep zero zero even when exponential's power overflows.
    Schedule.modifyDelay(({ attempt, duration }) =>
      Effect.succeed(attempt === 1 || base === 0 ? base : Math.min(Duration.toMillis(duration), cap))
    )
  )
