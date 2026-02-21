function on_ingest(eventType, data, context) {
  if (eventType !== "post_event") {
    return [];
  }

  const id = String(data.id ?? "");
  if (!id) {
    return [];
  }

  const event = String(data.event ?? "create");
  if (event === "delete") {
    return [
      {
        op: "delete",
        entity_type: "post",
        key: id,
      },
    ];
  }

  const post = {
    id,
    author_id: String(data.author_id ?? "unknown"),
    body: String(data.body ?? ""),
    created_at: String(data.created_at ?? new Date().toISOString()),
    conversation_id: String(data.conversation_id ?? ""),
  };

  return [
    {
      op: "upsert",
      entity_type: "post",
      key: id,
      value: post,
    },
  ];
}
