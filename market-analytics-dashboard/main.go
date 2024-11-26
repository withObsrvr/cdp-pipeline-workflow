package main

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

func main() {
	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	defer client.Close()

	// Create query interface
	query := NewMarketAnalyticsQuery(client, "stellar:asset:")
	ctx := context.Background()

	// Get top 20 assets
	assets, err := query.GetTopAssets(ctx, 20)
	if err != nil {
		log.Fatal(err)
	}

	// Print header
	fmt.Println("\n╔══════════════════════════════════════════╗")
	fmt.Println("║           Stellar Asset Rankings          ║")
	fmt.Println("╚══════════════════════════════════════════╝")

	// Calculate total market stats
	var totalMarketCap, totalVolume float64
	for _, asset := range assets {
		totalMarketCap += asset.MarketCap
		totalVolume += asset.Volume24h
	}

	// Print market overview
	fmt.Println("\n📊 Market Overview")
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Printf("Total Market Cap:    $%12.2f\n", totalMarketCap)
	fmt.Printf("24h Volume:          $%12.2f\n", totalVolume)
	fmt.Printf("Listed Assets:       %14d\n", len(assets))

	// Print top assets
	fmt.Println("\n📈 Top 20 Assets by Market Cap")
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

	for i, asset := range assets {
		fmt.Printf("\n%d. %s\n", i+1, asset.Code)
		fmt.Printf("   Issuer: %s\n", asset.Issuer)
		fmt.Printf("   %-15s $%-12.2f\n", "Price:", asset.Price)
		fmt.Printf("   %-15s $%-12.2f\n", "Market Cap:", asset.MarketCap)
		fmt.Printf("   %-15s $%-12.2f\n", "Volume 24h:", asset.Volume24h)
		fmt.Printf("   %-15s %d\n", "Holders:", asset.NumHolders)

		fmt.Printf("   ┌─────────────────┐ ┌─────────────────┐\n")
		fmt.Printf("   │ High   $%-7.2f │ │ Low    $%-7.2f │\n",
			asset.High24h, asset.Low24h)
		fmt.Printf("   │ Supply %-8.0f │ │ Change  %-7.2f │\n",
			asset.CirculatingSupply, asset.PriceChange24h)
		fmt.Printf("   └─────────────────┘ └─────────────────┘\n")
	}
}
