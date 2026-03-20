const route = {
  handle(request) {
    const user = request.body || {};
    const age = user.age ? Number(user.age) : null;
    const gender = user.gender ? String(user.gender) : null;
    const location = user.location ? String(user.location) : "any";

    // Optimization for millions of campaigns:
    // Instead of fetching all "active" campaigns (which could be millions),
    // we start with the most selective demographic index (location).

    const fetchItems = (indexName, value) => {
        // op_store_index_lookup returns up to 10,000 entities in the current MVP.
        const raw = Deno.core.ops.op_store_index_lookup(indexName, value);
        return raw ? JSON.parse(raw) : [];
    };

    // 1. Fetch campaigns for the user's location + broad ("any") location
    const matchedByLocation = fetchItems("by_location", location);
    const broadLocation = location !== "any" ? fetchItems("by_location", "any") : [];

    const candidates = matchedByLocation.concat(broadLocation);

    // 2. Fetch IDs for the user's gender + broad ("any") gender to use for set-based filtering
    const userGender = gender || "any";
    const matchedByGenderIds = new Set(fetchItems("by_gender", userGender).map(c => c.id));
    if (userGender !== "any") {
        fetchItems("by_gender", "any").forEach(c => matchedByGenderIds.add(c.id));
    }

    // 3. Filter the reduced set
    const eligibleCampaigns = candidates.filter(campaign => {
      // Must be active
      if (campaign.status !== "active") return false;

      // Must match gender set (targeted or "any")
      if (!matchedByGenderIds.has(campaign.id)) return false;

      // Age check
      if (age !== null) {
        if (campaign.target_age_min !== null && age < campaign.target_age_min) return false;
        if (campaign.target_age_max !== null && age > campaign.target_age_max) return false;
      }

      return true;
    });

    if (eligibleCampaigns.length === 0) {
      return {
        status: 204,
        headers: {},
        body: {},
      };
    }

    // Auction: Sort by bid price descending
    eligibleCampaigns.sort((a, b) => b.bid - a.bid);
    const winner = eligibleCampaigns[0];

    return {
      status: 200,
      headers: {
        "x-auction-count": eligibleCampaigns.length.toString()
      },
      body: {
        campaign_id: winner.id,
        bid: winner.bid,
        metadata: {
          auction_participants: eligibleCampaigns.length
        }
      },
    };
  },
};

globalThis.route = route;
