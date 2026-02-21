function parseLimit(rawLimit) {
  const requested = Number.parseInt(String(rawLimit ?? "50"), 10);
  if (Number.isNaN(requested)) {
    return 50;
  }
  return Math.max(1, Math.min(200, requested));
}

function parseFollowing(query) {
  const followingCsv = String(query.following ?? "");
  if (!followingCsv) {
    return [];
  }
  return followingCsv
    .split(",")
    .map((id) => id.trim())
    .filter((id) => id.length > 0);
}

const route = {
  handle(request) {
    const userId = String(request.query.user_id ?? "");
    const followingIds = parseFollowing(request.query);
    if (userId && !followingIds.includes(userId)) {
      followingIds.push(userId);
    }

    const params = {
      user_id: userId,
      user_ids: followingIds,
      limit: parseLimit(request.query.limit),
    };

    let posts = [];
    try {
      const raw = Deno.core.ops.op_execute_query("get_feed", JSON.stringify(params));
      posts = raw ? JSON.parse(raw) : [];
    } catch (_err) {
      const fallbackRaw = Deno.core.ops.op_collection_multi_scan(
        "user_timeline",
        JSON.stringify(followingIds),
        params.limit,
      );
      const fallbackPosts = fallbackRaw ? JSON.parse(fallbackRaw) : [];
      posts = Array.isArray(fallbackPosts) ? fallbackPosts.slice(0, params.limit) : [];
    }

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
