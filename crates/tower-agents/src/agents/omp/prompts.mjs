function createObserver(notify) {
  const approvals = new Set(), reviews = new Set();
  let dialogs = 0, ctx, session, epoch = 0;
  const tracked = new WeakSet();
  const isChild = value => safely(() => value?.agent?.kind === 'sub') === true;
  const snapshot = () => approvals.size ? 'approval' : dialogs || reviews.size ? 'decision' : 'none';
  function wrap(ui, name) {
    const adapter = safely(() => ui[name]);
    const descriptor = safely(() => Object.getOwnPropertyDescriptor(ui, name));
    // OMP's proxy adapters call the target property. Calling the adapter after
    // replacing that property would recurse, so use the target's own function.
    const original = typeof descriptor?.value === 'function' ? descriptor.value : adapter;
    if (typeof original !== 'function' || tracked.has(original)) return;
    const wrapped = function (...args) {
      const opened = epoch;
      dialogs++;
      safely(() => notify(ctx));
      let closed = false;
      const close = () => {
        if (closed) return;
        closed = true;
        if (opened !== epoch) return;
        dialogs--;
        safely(() => notify(ctx));
      };
      try {
        const result = original.apply(this, args);
        if (result && typeof result.then === 'function') {
          return result.then(value => { close(); return value; }, error => { close(); throw error; });
        }
        close();
        return result;
      } catch (error) { close(); throw error; }
    };
    tracked.add(wrapped);
    ui[name] = wrapped;
  }
  return {
    capture(name, event, context) {
      const id = safely(() => context?.sessionManager?.getSessionId?.());
      if ((name === 'session_start' || name === 'session_switch') && id !== session) {
        session = id; epoch++; dialogs = 0; approvals.clear(); reviews.clear();
      }
      const tool = safely(() => event?.toolCallId) ?? '?';
      const review = safely(() => event?.reviewId) ?? '?';
      if (name === 'tool_approval_requested') approvals.add(tool);
      if (name === 'tool_approval_resolved') approvals.delete(tool);
      if (name === 'plan_review_requested') reviews.add(review);
      if (name === 'plan_review_resolved') reviews.delete(review);
      return {session_is_child: isChild(context) || undefined, blocking_prompt: snapshot()};
    },
    install(name, context) {
      if (name !== 'session_start' && name !== 'session_switch') return;
      if (isChild(context)) return;
      ctx = context;
      const ui = safely(() => ctx?.ui);
      if (!ui) return;
      for (const method of ['select', 'confirm', 'input', 'editor', 'custom', 'askDialog', 'approve']) {
        safely(() => wrap(ui, method));
      }
    }
  };
}
