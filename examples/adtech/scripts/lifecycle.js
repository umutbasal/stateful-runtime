function on_init() {
    // Seed some initial data for demonstration purposes.
    // In a production scenario, state would typically be derived from replaying Kafka topics.
    const campaigns = [
        {
            id: "c1",
            bid: 1.5,
            target_age_min: 18,
            target_age_max: 35,
            target_gender: "male",
            target_location: "USA",
            status: "active"
        },
        {
            id: "c2",
            bid: 2.5,
            target_age_min: 18,
            target_age_max: 35,
            target_gender: "male",
            target_location: "USA",
            status: "active"
        },
        {
            id: "c3",
            bid: 3.5,
            target_age_min: 18,
            target_age_max: 35,
            target_gender: "female",
            target_location: "USA",
            status: "active"
        },
        {
            id: "c_broad",
            bid: 0.5,
            target_age_min: null,
            target_age_max: null,
            target_gender: "any",
            target_location: "any",
            status: "active"
        }
    ];

    const ops = campaigns.map(c => {
        // Demographic fields must be normalized (e.g. "any") for effective index lookups.
        const normalized = {
            ...c,
            target_gender: c.target_gender || "any",
            target_location: c.target_location || "any",
            updated_at: new Date().toISOString(),
        };
        return {
            op: "upsert",
            entity_type: "campaign",
            key: normalized.id,
            value: normalized
        };
    });

    return ops;
}

globalThis.on_init = on_init;
