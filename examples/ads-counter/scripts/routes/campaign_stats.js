const route = {
  handle(request) {
    const campaignId = request.params.id;
    if (!campaignId) {
      return {
        status: 400,
        headers: {},
        body: { error: "missing campaign id" },
      };
    }

    const raw = Deno.core.ops.op_store_get("campaign_stats", String(campaignId));
    const stats = raw ? JSON.parse(raw) : null;

    if (!stats || stats === null) {
      return {
        status: 404,
        headers: {},
        body: { error: "campaign not found" },
      };
    }

    return {
      status: 200,
      headers: {},
      body: stats,
    };
  },
};
