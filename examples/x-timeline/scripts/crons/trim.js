function on_cron(name) {
  if (name !== "trim_user_timeline") {
    return [];
  }

  Deno.core.ops.op_collection_trim("user_timeline", 172800);
  return [];
}
