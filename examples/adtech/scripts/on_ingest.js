function on_ingest(eventType, data, context) {
  if (eventType !== "campaign_update") {
    return [];
  }

  const campaignId = String(data.id ?? "");
  if (!campaignId) {
    return [];
  }

  // Demographics must be normalized to "any" if not provided for effective index lookups
  const targetGender = data.target_gender ? String(data.target_gender) : "any";
  const targetLocation = data.target_location ? String(data.target_location) : "any";

  return [
    {
      op: "upsert",
      entity_type: "campaign",
      key: campaignId,
      value: {
        id: campaignId,
        bid: Number(data.bid ?? 0),
        target_age_min: data.target_age_min ? Number(data.target_age_min) : null,
        target_age_max: data.target_age_max ? Number(data.target_age_max) : null,
        target_gender: targetGender,
        target_location: targetLocation,
        status: String(data.status ?? "active"),
        updated_at: new Date().toISOString(),
      },
    },
  ];
}

globalThis.on_ingest = on_ingest;
