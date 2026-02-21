function toTimestamp(value) {
  const parsed = Number.parseInt(String(value ?? "0"), 10);
  return Number.isNaN(parsed) ? 0 : parsed;
}

function on_query(params) {
  const userIds = Array.isArray(params?.user_ids)
    ? params.user_ids.map((id) => String(id)).filter((id) => id.length > 0)
    : [];

  const requestedLimit = Number.parseInt(String(params?.limit ?? "50"), 10);
  const limit = Number.isNaN(requestedLimit) ? 50 : Math.max(1, Math.min(200, requestedLimit));
  const perUserLimit = Math.max(1, Math.min(500, limit));

  const raw = Deno.core.ops.op_collection_multi_scan(
    "user_timeline",
    JSON.stringify(userIds),
    perUserLimit,
  );
  const posts = raw ? JSON.parse(raw) : [];

  const dedupedById = new Map();
  for (const post of posts) {
    const postId = String(post?.id ?? "");
    if (!postId || dedupedById.has(postId)) {
      continue;
    }
    dedupedById.set(postId, post);
  }

  const uniquePosts = [...dedupedById.values()];
  uniquePosts.sort((a, b) => toTimestamp(b?.created_at) - toTimestamp(a?.created_at));
  return uniquePosts.slice(0, limit);
}
