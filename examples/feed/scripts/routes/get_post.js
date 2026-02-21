const route = {
  handle(request) {
    const id = String(request.params.id ?? "");
    if (!id) {
      return {
        status: 400,
        headers: {},
        body: { error: "missing post id" },
      };
    }

    const raw = Deno.core.ops.op_store_get("post", id);
    const post = raw ? JSON.parse(raw) : null;
    if (!post || post === null) {
      return {
        status: 404,
        headers: {},
        body: { error: "post not found" },
      };
    }

    return {
      status: 200,
      headers: {},
      body: post,
    };
  },
};
