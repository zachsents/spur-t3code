// T3 Code's threads on this machine: a turn finishing, or a thread waiting on the user, as events.
// T3 pushes nothing to an outside client, so its MCP endpoint is polled: each project's threads
// with a run under way, compared with the last look. What was under way is kept in a file, so a
// turn that ends while the spur is down is still told.
import { mkdirSync, readFileSync, writeFileSync } from "node:fs"
import path from "node:path"
import { Context, Effect, Layer, Option, Ref, Schema } from "effect"
import { AppEvents, defineApp, defineEvent, tool } from "spur-kit"
import type { Draft } from "spur-kit"

import { makeT3 } from "./t3.ts"
import type { T3 } from "./t3.ts"

const POLL = "4 seconds"
const PROJECTS_EVERY_MS = 60_000
const UNDER_WAY = ["preparing", "queued", "starting", "running", "waiting"] as const
const ENDED = ["completed", "failed", "interrupted", "cancelled", "rolled_back"] as const
const REPLY_CHARS = 2000
const RECENT = 20

const link = (thread: string) => `t3-thread://v1/${thread}`

// What the spur reads of T3's answers; the rest is let through.
const ProjectList = Schema.Struct({
  projects: Schema.Array(Schema.Struct({ id: Schema.String, title: Schema.String })),
  nextCursor: Schema.optionalKey(Schema.NullOr(Schema.Unknown)),
})
const ThreadList = Schema.Struct({
  threads: Schema.Array(
    Schema.Struct({
      threadId: Schema.String,
      title: Schema.String,
      status: Schema.String,
      latestRunId: Schema.NullOr(Schema.String),
      relationshipToParent: Schema.optionalKey(Schema.NullOr(Schema.String)),
      updatedAt: Schema.String,
    }),
  ),
})
const Run = Schema.Struct({
  runId: Schema.String,
  status: Schema.String,
  startedAt: Schema.NullOr(Schema.String),
  completedAt: Schema.NullOr(Schema.String),
})
const ThreadRead = Schema.Struct({
  thread: Schema.Struct({
    threadId: Schema.String,
    projectId: Schema.String,
    title: Schema.String,
    status: Schema.String,
    itemCount: Schema.Number,
    pendingRequestCount: Schema.optionalKey(Schema.Number),
  }),
  recentRuns: Schema.Array(Run),
  items: Schema.Array(
    Schema.Struct({
      type: Schema.String,
      runId: Schema.optionalKey(Schema.NullOr(Schema.String)),
      text: Schema.optionalKey(Schema.NullOr(Schema.String)),
    }),
  ),
})
const PendingList = Schema.Struct({ requestIds: Schema.Array(Schema.String) })
const PendingRead = Schema.Struct({
  questions: Schema.Array(
    Schema.Struct({
      header: Schema.String,
      question: Schema.String,
      options: Schema.Array(Schema.Struct({ label: Schema.String })),
    }),
  ),
})

const ended = Schema.Literals(ENDED)
const question = Schema.Struct({
  header: Schema.String,
  question: Schema.String,
  options: Schema.Array(Schema.String),
})
const where = {
  thread: Schema.String,
  title: Schema.String,
  project: Schema.String,
  /** Opens the thread in T3 Code. */
  link: Schema.String,
}

/** An agent's turn in a thread ended: done, failed or stopped, with its last reply. */
export const TurnFinished = defineEvent(
  "t3code.turn_finished",
  1,
  Schema.Struct({
    ...where,
    status: ended,
    startedAt: Schema.NullOr(Schema.String),
    completedAt: Schema.NullOr(Schema.String),
    reply: Schema.String,
  }),
)

/** A thread stopped for the user: a question to answer, or an approval. */
export const NeedsInput = defineEvent(
  "t3code.needs_input",
  1,
  Schema.Struct({
    ...where,
    kind: Schema.Literals(["question", "approval"]),
    questions: Schema.Array(question),
  }),
)

const tracked = Schema.Struct({
  thread: Schema.String,
  title: Schema.String,
  project: Schema.String,
  status: Schema.String,
  run: Schema.NullOr(Schema.String),
  since: Schema.String,
})
type Tracked = typeof tracked.Type
const finished = Schema.Struct({
  thread: Schema.String,
  title: Schema.String,
  project: Schema.String,
  status: Schema.String,
  completedAt: Schema.NullOr(Schema.String),
})
type Finished = typeof finished.Type
const state = Schema.Struct({
  underWay: Schema.Array(tracked),
  recent: Schema.Array(finished),
})
type State = typeof state.Type
const decodeState = Schema.decodeUnknownSync(Schema.fromJsonString(state))

const decode =
  <A>(schema: Schema.Codec<A, unknown>) =>
  (raw: unknown) =>
    Schema.decodeUnknownEffect(schema)(raw).pipe(Effect.mapError((e) => e.message))

const isReply = (i: { readonly type: string; readonly text?: string | null }) =>
  i.type === "assistant_message" && typeof i.text === "string" && i.text !== ""

const clip = (text: string, max: number) => (text.length > max ? `${text.slice(0, max)}…` : text)

export class Watch extends Context.Service<
  Watch,
  {
    readonly t3: T3
    readonly read: Effect.Effect<State>
    readonly projectName: (id: string) => string
  }
>()("switch/t3code/Watch") {
  static readonly layer = (file: string) =>
    Layer.effect(
      Watch,
      Effect.gen(function* making() {
        const events = yield* AppEvents
        const t3 = makeT3(path.join(path.dirname(file), "t3code-session.json"))
        const loaded = yield* Effect.sync(() => {
          try {
            return Option.some(decodeState(readFileSync(file, "utf-8")))
          } catch {
            return Option.none<State>()
          }
        })
        const ref = yield* Ref.make<State>(
          Option.getOrElse(loaded, () => ({ underWay: [], recent: [] })),
        )
        // Nothing to compare the first look with: it only seeds.
        let seeded = Option.isSome(loaded)
        const projects = new Map<string, string>()
        let projectsAt = 0

        const save = (s: State) =>
          Effect.sync(() => {
            mkdirSync(path.dirname(file), { recursive: true })
            writeFileSync(file, JSON.stringify(s))
          })
        const emit = (draft: Draft) => Effect.ignore(events.emit(draft))
        const projectName = (id: string) => projects.get(id) ?? id

        const loadProjects = Effect.gen(function* listing() {
          if (projects.size > 0 && Date.now() - projectsAt < PROJECTS_EVERY_MS) {
            return
          }
          const list = yield* t3
            .call("t3_project_list", {})
            .pipe(Effect.flatMap(decode(ProjectList)))
          projects.clear()
          for (const p of list.projects) {
            projects.set(p.id, p.title)
          }
          projectsAt = Date.now()
        })

        const underWay = Effect.gen(function* looking() {
          yield* loadProjects
          const seen: Tracked[] = []
          for (const [id, title] of projects) {
            const list = yield* t3
              .call("t3_thread_list", { projectId: id, statuses: UNDER_WAY, limit: 100 })
              .pipe(Effect.flatMap(decode(ThreadList)))
            for (const t of list.threads) {
              // A provider's own subagents run inside their parent's turn, which is told instead.
              if (t.relationshipToParent !== "subagent") {
                seen.push({
                  thread: t.threadId,
                  title: t.title,
                  project: title,
                  status: t.status,
                  run: t.latestRunId,
                  since: t.updatedAt,
                })
              }
            }
          }
          return seen
        })

        const readThread = (thread: string, args: object = {}) =>
          t3
            .call("t3_thread_read", { threadId: thread, ...args })
            .pipe(Effect.flatMap(decode(ThreadRead)))

        /** The thread's last reply in `run`, else its last reply at all. */
        const lastReply = (head: typeof ThreadRead.Type, run: string | null) =>
          Effect.gen(function* replying() {
            const tail = yield* readThread(head.thread.threadId, {
              view: "messages",
              afterPosition: Math.max(0, head.thread.itemCount - 12),
              limit: 12,
              maxCharsPerItem: REPLY_CHARS,
            })
            const replies = tail.items.filter(isReply)
            const reply = replies.findLast((i) => i.runId === run) ?? replies.at(-1)
            return clip(reply?.text ?? "", REPLY_CHARS)
          })

        const tellFinished = (was: Tracked) =>
          Effect.gen(function* telling() {
            const head = yield* readThread(was.thread, { limit: 1, runLimit: 5 })
            const run = head.recentRuns.find((r) => r.runId === was.run) ?? head.recentRuns[0]
            const status = Option.getOrUndefined(Schema.decodeUnknownOption(ended)(run?.status))
            if (run === undefined || status === undefined) {
              return Option.none<Finished>()
            }
            const reply = yield* lastReply(head, run.runId)
            yield* emit(
              TurnFinished.make({
                thread: was.thread,
                title: head.thread.title,
                project: was.project,
                link: link(was.thread),
                status,
                startedAt: run.startedAt,
                completedAt: run.completedAt,
                reply,
              }),
            )
            return Option.some<Finished>({
              thread: was.thread,
              title: head.thread.title,
              project: was.project,
              status,
              completedAt: run.completedAt,
            })
          })

        const tellWaiting = (now: Tracked) =>
          Effect.gen(function* asking() {
            const pending = yield* t3
              .call("t3_pending_request_list", { threadId: now.thread })
              .pipe(Effect.flatMap(decode(PendingList)))
            const questions: (typeof question.Type)[] = []
            for (const requestId of pending.requestIds) {
              const read = yield* t3
                .call("t3_pending_request_read", { threadId: now.thread, requestId })
                .pipe(Effect.flatMap(decode(PendingRead)))
              for (const q of read.questions) {
                questions.push({
                  header: q.header,
                  question: clip(q.question, REPLY_CHARS),
                  options: q.options.map((o) => o.label),
                })
              }
            }
            yield* emit(
              NeedsInput.make({
                thread: now.thread,
                title: now.title,
                project: now.project,
                link: link(now.thread),
                kind: questions.length > 0 ? "question" : "approval",
                questions,
              }),
            )
          })

        const look = Effect.gen(function* comparing() {
          const before = yield* Ref.get(ref)
          const seen = yield* underWay
          const was = new Map(before.underWay.map((t) => [t.thread, t]))
          const now = new Map(seen.map((t) => [t.thread, t]))
          const done: Finished[] = []
          if (seeded) {
            for (const [id, w] of was) {
              const n = now.get(id)
              if (n === undefined || n.run !== w.run) {
                const told = yield* tellFinished(w).pipe(
                  Effect.catch((error) =>
                    Effect.as(Effect.logWarning(error), Option.none<Finished>()),
                  ),
                )
                if (Option.isSome(told)) {
                  done.push(told.value)
                }
              }
            }
            for (const [id, n] of now) {
              const w = was.get(id)
              if (n.status === "waiting" && (w?.status !== "waiting" || w.run !== n.run)) {
                yield* tellWaiting(n).pipe(Effect.catch((error) => Effect.logWarning(error)))
              }
            }
          }
          seeded = true
          const next: State = {
            // A thread's start under way is when it was first seen so.
            underWay: seen.map((t) => {
              const w = was.get(t.thread)
              return w && w.run === t.run ? { ...t, since: w.since } : t
            }),
            recent: [...done.toReversed(), ...before.recent].slice(0, RECENT),
          }
          yield* Ref.set(ref, next)
          yield* save(next)
        })

        yield* Effect.forkScoped(
          Effect.forever(
            look.pipe(
              Effect.catch(() => Effect.void),
              Effect.andThen(Effect.sleep(POLL)),
            ),
          ),
        )

        return { t3, read: Ref.get(ref), projectName }
      }),
    )
}

const threads = tool(
  "threads",
  {
    description:
      "T3 Code threads with a turn under way (running, or waiting on the user), and turns that ended lately, newest first.",
    readOnly: true,
  },
  () =>
    Effect.gen(function* listing() {
      const s = yield* (yield* Watch).read
      return {
        underWay: s.underWay.map((t) => ({ ...t, link: link(t.thread) })),
        ended: s.recent.map((t) => ({ ...t, link: link(t.thread) })),
      }
    }),
)

const thread = tool(
  "thread",
  {
    description: "One T3 Code thread: its status, latest runs and last reply.",
    readOnly: true,
    input: Schema.Struct({ thread: Schema.String }),
  },
  (input) =>
    Effect.gen(function* reading() {
      const watch = yield* Watch
      const head = yield* watch.t3
        .call("t3_thread_read", { threadId: input.thread, limit: 1, runLimit: 3 })
        .pipe(
          Effect.mapError((e) => e.reason),
          Effect.flatMap(decode(ThreadRead)),
        )
      const tail = yield* watch.t3
        .call("t3_thread_read", {
          threadId: input.thread,
          view: "messages",
          afterPosition: Math.max(0, head.thread.itemCount - 12),
          limit: 12,
          maxCharsPerItem: 4000,
        })
        .pipe(
          Effect.mapError((e) => e.reason),
          Effect.flatMap(decode(ThreadRead)),
        )
      const reply = tail.items.findLast(isReply)
      return {
        thread: head.thread.threadId,
        title: head.thread.title,
        project: watch.projectName(head.thread.projectId),
        status: head.thread.status,
        pendingQuestions: head.thread.pendingRequestCount ?? 0,
        runs: head.recentRuns,
        reply: reply?.text ?? "",
        link: link(head.thread.threadId),
      }
    }),
)

export const t3codeApp = defineApp<Watch>({
  name: "t3code",
  version: "1",
  instructions:
    "T3 Code, the coding agent app on this computer: its threads (threads, thread), and an event when an agent's turn ends (t3code.turn_finished) or a thread waits on the user (t3code.needs_input). To tell the user when one finishes, expect its turn_finished.",
  tools: [threads, thread],
  events: [TurnFinished, NeedsInput],
})
