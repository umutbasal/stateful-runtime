function on_ingest(eventType, data, context) {
  if (eventType !== "campaign_event") {
    return [];
  }

  const campaignId = String(data.campaign_id ?? "");
  if (!campaignId) {
    return [];
  }

  const eventName = String(data.event ?? "impression");
  const previous = Deno.core.ops.op_store_get("campaign_stats", campaignId);
  const current = previous ? JSON.parse(previous) : null;

  const next = {
    campaign_id: campaignId,
    impressions: current?.impressions ?? 0,
    clicks: current?.clicks ?? 0,
    conversions: current?.conversions ?? 0,
    updated_at: new Date().toISOString(),
  };

  if (eventName === "impression") {
    next.impressions += 1;
  } else if (eventName === "click") {
    next.clicks += 1;
  } else if (eventName === "conversion") {
    next.conversions += 1;
  }

  return [
    {
      op: "upsert",
      entity_type: "campaign_stats",
      key: campaignId,
      value: next,
    },
  ];
}
