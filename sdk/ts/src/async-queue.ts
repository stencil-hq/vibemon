/** A promise resolver triple compatible with Promise.withResolvers. */
export type Resolver<T> = {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(reason: unknown): void;
};
/** Create a deferred promise. */
export function deferred<T>(): Resolver<T> {
  let resolve!: Resolver<T>["resolve"];
  let reject!: Resolver<T>["reject"];
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

/** An unbounded async producer/consumer queue with terminal end/fail states. */
export class AsyncQueue<T> implements AsyncIterable<T> {
  readonly #values: T[] = [];
  readonly #waiters: Array<Resolver<IteratorResult<T>>> = [];
  #error: unknown;
  #done = false;
  /** Deliver one value to the oldest waiter or enqueue it. */
  push(value: T): void {
    const waiter = this.#waiters.shift();
    if (waiter) waiter.resolve({ value, done: false });
    else this.#values.push(value);
  }
  /** End the queue and resolve every pending waiter. */
  end(): void {
    this.#done = true;
    for (const waiter of this.#waiters.splice(0)) waiter.resolve({ value: undefined, done: true });
  }
  /** Fail the queue and reject every pending waiter. */
  fail(error: unknown): void {
    this.#error = error;
    this.#done = true;
    for (const waiter of this.#waiters.splice(0)) waiter.reject(error);
  }
  /** Consume queued and future values until the queue ends. */
  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: async () => {
        const value = this.#values.shift();
        if (value !== undefined) return { value, done: false };
        if (this.#error) throw this.#error;
        if (this.#done) return { value: undefined, done: true };
        const waiter = deferred<IteratorResult<T>>();
        this.#waiters.push(waiter);
        return waiter.promise;
      },
      // Connect's iterable pipeline requires throw/return to propagate
      // downstream errors and cancellation into the request iterable.
      throw: async (error: unknown) => {
        this.fail(error);
        return { value: undefined, done: true };
      },
      return: async () => {
        this.end();
        return { value: undefined, done: true };
      },
    };
  }
}
