function parseLimit(rawLimit) {
  const requested = Number.parseInt(String(rawLimit ?? "50"), 10);
  if (Number.isNaN(requested)) {
    return 50;
  }
  return Math.max(1, Math.min(200, requested));
}

const route = {
  handle(request) {
    const userId = String(request.params.id ?? request.query.user_id ?? "");
    if (!userId) {
      return {
        status: 400,
        headers: {},
        body: {
          error: "missing user id",
        },
      };
    }

    const limit = parseLimit(request.query.limit);
    const raw = Deno.core.ops.op_collection_scan("user_timeline", userId, limit);
    const posts = raw ? JSON.parse(raw) : [];

    return {
      status: 200,
      headers: {},
      body: {
        user_id: userId,
        count: posts.length,
        posts,
      },
    };
  },
};
