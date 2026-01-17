package main

import "fmt"

// ===========================================
// STRUCT: Like a Python dataclass or Kafka message schema
// ===========================================
type AdEvent struct {
	CampaignID  string
	Platform    string  // "google", "meta", "tiktok"
	Impressions int
	Clicks      int
	Spend       float64
}

func runP3() {
	// ===========================================
	// PART 1: Arrays vs Slices
	// ===========================================
	// Array = fixed partition count (rigid, like Kafka topic partitions)
	// Slice = dynamic list (flexible, like Python list)

	// Array: size is part of the type! [3]int != [4]int
	var fixedMetrics [3]float64 = [3]float64{0.05, 0.12, 0.08}
	fmt.Println("=== Arrays (Fixed Size) ===")
	fmt.Printf("CTR metrics (fixed array): %v\n", fixedMetrics)

	// Slice: dynamic, the workhorse of Go
	// Like a Python list but with explicit capacity control
	platforms := []string{"google", "meta", "tiktok"}
	fmt.Println("\n=== Slices (Dynamic) ===")
	fmt.Printf("Platforms: %v\n", platforms)

	// ===========================================
	// PART 2: Slice Length vs Capacity
	// ===========================================
	// len() = current elements (like len() in Python)
	// cap() = allocated space (like knowing your Kafka batch buffer size)

	events := make([]AdEvent, 0, 100) // len=0, cap=100
	fmt.Println("\n=== Length vs Capacity ===")
	fmt.Printf("Before append - len: %d, cap: %d\n", len(events), cap(events))

	// Append grows length, capacity stays (until exceeded)
	events = append(events, AdEvent{"camp1", "google", 10000, 250, 150.00})
	events = append(events, AdEvent{"camp2", "meta", 25000, 500, 200.00})
	fmt.Printf("After 2 appends - len: %d, cap: %d\n", len(events), cap(events))

	// ===========================================
	// PART 3: Memory Sharing (The Gotcha!)
	// ===========================================
	// Slicing creates a VIEW, not a copy
	// Like creating a Spark DataFrame view - changes reflect back!

	original := []int{100, 200, 300, 400, 500}
	subset := original[1:4] // [200, 300, 400] - shares memory!

	fmt.Println("\n=== Memory Sharing (Dangerous!) ===")
	fmt.Printf("Original: %v\n", original)
	fmt.Printf("Subset [1:4]: %v\n", subset)

	// Modify subset - original changes too!
	subset[0] = 9999
	fmt.Printf("After subset[0] = 9999:\n")
	fmt.Printf("  Original: %v  <- Also changed!\n", original)
	fmt.Printf("  Subset: %v\n", subset)

	// ===========================================
	// PART 4: Safe Copy
	// ===========================================
	// Use copy() when you need independent data
	// Like materializing a Spark view to avoid recomputation

	source := []int{10, 20, 30, 40}
	safeCopy := make([]int, len(source)) // Allocate new memory
	copy(safeCopy, source)               // Copy data over

	fmt.Println("\n=== Safe Copy ===")
	safeCopy[0] = 999
	fmt.Printf("Source (unchanged): %v\n", source)
	fmt.Printf("SafeCopy (modified): %v\n", safeCopy)

	// ===========================================
	// PART 5: Maps (Like Python dict)
	// ===========================================
	// Hash map: O(1) lookup, great for aggregations

	// Campaign spend by platform
	spendByPlatform := map[string]float64{
		"google": 15000.50,
		"meta":   22000.75,
		"tiktok": 8500.25,
	}

	fmt.Println("\n=== Maps ===")
	fmt.Printf("Spend map: %v\n", spendByPlatform)
	fmt.Printf("Google spend: $%.2f\n", spendByPlatform["google"])

	// ===========================================
	// PART 6: Comma-Ok Idiom (Critical!)
	// ===========================================
	// Distinguish "key missing" from "key exists with zero value"
	// Like handling NULL vs 0 in your analytics queries

	clicksByAd := map[string]int{
		"ad_001": 500,
		"ad_002": 0, // Zero clicks, but exists!
	}

	fmt.Println("\n=== Comma-Ok Idiom ===")

	// Without comma-ok: ambiguous!
	clicks := clicksByAd["ad_003"]
	fmt.Printf("ad_003 clicks (no comma-ok): %d <- Is it 0 or missing?\n", clicks)

	// With comma-ok: crystal clear
	clicks, exists := clicksByAd["ad_002"]
	fmt.Printf("ad_002: clicks=%d, exists=%t\n", clicks, exists)

	clicks, exists = clicksByAd["ad_003"]
	fmt.Printf("ad_003: clicks=%d, exists=%t <- Now we know!\n", clicks, exists)

	// ===========================================
	// PART 7: Structs in Action
	// ===========================================
	fmt.Println("\n=== Structs ===")

	// Named struct
	event := AdEvent{
		CampaignID:  "campaign_123",
		Platform:    "google",
		Impressions: 50000,
		Clicks:      1250,
		Spend:       500.00,
	}
	ctr := float64(event.Clicks) / float64(event.Impressions) * 100
	fmt.Printf("Campaign %s: %.2f%% CTR\n", event.CampaignID, ctr)

	// Anonymous struct - great for JSON parsing!
	// Like a one-off Pydantic model for an API response
	apiResponse := struct {
		Status string
		Count  int
	}{
		Status: "success",
		Count:  42,
	}
	fmt.Printf("API Response: %+v\n", apiResponse)

	// ===========================================
	// EXERCISE: Build a Campaign Aggregator
	// ===========================================
	fmt.Println("\n=== Exercise: Campaign Aggregator ===")

	rawEvents := []AdEvent{
		{"c1", "google", 10000, 200, 100.0},
		{"c1", "google", 15000, 350, 150.0},
		{"c2", "meta", 20000, 400, 200.0},
		{"c1", "meta", 5000, 100, 50.0},
	}

	// Aggregate spend by campaign
	spendByCampaign := make(map[string]float64)
	for _, e := range rawEvents {
		spendByCampaign[e.CampaignID] += e.Spend
	}

	for campaign, total := range spendByCampaign {
		fmt.Printf("Campaign %s: $%.2f total spend\n", campaign, total)
	}
}