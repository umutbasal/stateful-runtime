function normalizePost(data) {
  return {
    id: String(data.id ?? data.post_id ?? ""),
    author_id: String(data.author_id ?? data.user_id ?? ""),
    body: String(data.body ?? ""),
    created_at: String(data.created_at ?? Date.now()),
    conversation_id: String(data.conversation_id ?? ""),
    is_retweet: Boolean(data.is_retweet ?? false),
    is_reply: Boolean(data.is_reply ?? false),
    source_user_id: String(data.source_user_id ?? ""),
    source_post_id: String(data.source_post_id ?? ""),
  };
}

function on_ingest(eventType, data, context) {
  if (eventType !== "tweet_event") {
    return [];
  }

  const post = normalizePost(data);
  if (!post.id || !post.author_id) {
    return [];
  }

  const event = String(data.event ?? "create");
  if (event === "delete") {
    return [
      {
        op: "delete",
        entity_type: "post",
        key: post.id,
      },
      {
        op: "remove_item",
        entity_type: "user_timeline",
        key: post.author_id,
        item_id: post.id,
      },
    ];
  }

  return [
    {
      op: "upsert",
      entity_type: "post",
      key: post.id,
      value: post,
    },
    {
      op: "push",
      entity_type: "user_timeline",
      key: post.author_id,
      item_id: post.id,
      value: post,
    },
  ];
}
