const route = {
  handle(request) {
    const requested = Number.parseInt(request.query.limit ?? "20", 10);
    const limit = Number.isNaN(requested) ? 20 : Math.max(1, Math.min(100, requested));

    const raw = Deno.core.ops.op_store_range_lookup("by_created_at", "", "", limit);
    const posts = raw ? JSON.parse(raw) : [];
    posts.sort((a, b) => String(b.created_at).localeCompare(String(a.created_at)));

    return {
      status: 200,
      headers: {},
      body: {
        count: posts.length,
        posts,
      },
    };
  },
};
