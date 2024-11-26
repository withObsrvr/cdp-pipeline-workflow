package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// MarketAnalyticsQuery provides methods to query market analytics data from Redis
type MarketAnalyticsQuery struct {
	client    *redis.Client
	keyPrefix string
}

// TimeSeriesPoint represents a single point in time series data
type TimeSeriesPoint struct {
	Timestamp time.Time
	Value     float64
}

// MarketStatsResponse contains the latest market statistics
type MarketStatsResponse struct {
	TradePair      string    `json:"trade_pair"`
	OpenPrice      float64   `json:"open_price"`
	ClosePrice     float64   `json:"close_price"`
	HighPrice      float64   `json:"high_price"`
	LowPrice       float64   `json:"low_price"`
	Volume         float64   `json:"volume"`
	TradeCount     int64     `json:"trade_count"`
	PriceChange    float64   `json:"price_change"`
	BidVolume      float64   `json:"bid_volume"`
	AskVolume      float64   `json:"ask_volume"`
	Spread         float64   `json:"spread"`
	SpreadMidPoint float64   `json:"spread_midpoint"`
	LastUpdated    time.Time `json:"last_updated"`
	MarketCap      float64   `json:"market_cap"`
}

// MarketDepthResponse contains orderbook depth information
type MarketDepthResponse struct {
	BidVolume float64 `json:"bid_volume"`
	AskVolume float64 `json:"ask_volume"`
	Spread    float64 `json:"spread"`
}

// AssetStats represents asset statistics
type AssetStats struct {
	Code              string    `json:"code"`
	Issuer            string    `json:"issuer"`
	CirculatingSupply float64   `json:"circulating_supply"`
	MarketCap         float64   `json:"market_cap"`
	Price             float64   `json:"price"`
	Volume24h         float64   `json:"volume_24h"`
	High24h           float64   `json:"high_24h"`
	Low24h            float64   `json:"low_24h"`
	NumHolders        int64     `json:"num_holders"`
	LastUpdated       time.Time `json:"last_updated"`
	PriceChange24h    float64   `json:"price_change_24h"`
}

// NewMarketAnalyticsQuery creates a new query interface
func NewMarketAnalyticsQuery(redisClient *redis.Client, keyPrefix string) *MarketAnalyticsQuery {
	return &MarketAnalyticsQuery{
		client:    redisClient,
		keyPrefix: keyPrefix,
	}
}

// GetLatestMarketStats retrieves the most recent market statistics
func (q *MarketAnalyticsQuery) GetLatestMarketStats(ctx context.Context, tradePair string) (*MarketStatsResponse, error) {
	key := fmt.Sprintf("%s%s:latest", q.keyPrefix, tradePair)

	result, err := q.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get market stats: %w", err)
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no data found for trade pair: %s", tradePair)
	}

	// Parse the results
	stats := &MarketStatsResponse{
		TradePair: tradePair,
	}

	if v, err := strconv.ParseFloat(result["open_price"], 64); err == nil {
		stats.OpenPrice = v
	}
	if v, err := strconv.ParseFloat(result["close_price"], 64); err == nil {
		stats.ClosePrice = v
	}
	if v, err := strconv.ParseFloat(result["high_price"], 64); err == nil {
		stats.HighPrice = v
	}
	if v, err := strconv.ParseFloat(result["low_price"], 64); err == nil {
		stats.LowPrice = v
	}
	if v, err := strconv.ParseFloat(result["volume"], 64); err == nil {
		stats.Volume = v
	}
	if v, err := strconv.ParseInt(result["trade_count"], 10, 64); err == nil {
		stats.TradeCount = v
	}
	if v, err := strconv.ParseFloat(result["price_change"], 64); err == nil {
		stats.PriceChange = v
	}
	if v, err := strconv.ParseFloat(result["bid_volume"], 64); err == nil {
		stats.BidVolume = v
	}
	if v, err := strconv.ParseFloat(result["ask_volume"], 64); err == nil {
		stats.AskVolume = v
	}
	if v, err := strconv.ParseFloat(result["spread"], 64); err == nil {
		stats.Spread = v
	}
	if v, err := strconv.ParseFloat(result["spread_midpoint"], 64); err == nil {
		stats.SpreadMidPoint = v
	}
	if timestamp, ok := result["timestamp"]; ok {
		if t, err := time.Parse(time.RFC3339, timestamp); err == nil {
			stats.LastUpdated = t
		}
	}

	// Get market cap data
	baseAsset := strings.Split(tradePair, "/")[0]
	mcKey := fmt.Sprintf("stellar:market_cap:%s:latest", baseAsset)
	mcData, err := q.client.HGetAll(ctx, mcKey).Result()
	if err == nil && len(mcData) > 0 {
		if mc, err := strconv.ParseFloat(mcData["market_cap"], 64); err == nil {
			stats.MarketCap = mc
		}
	}

	// Try token price data if market cap is still 0
	if stats.MarketCap == 0 {
		priceKey := fmt.Sprintf("stellar:price:%s:USDC:latest", baseAsset)
		priceData, err := q.client.HGetAll(ctx, priceKey).Result()
		if err == nil && len(priceData) > 0 {
			if mc, err := strconv.ParseFloat(priceData["market_cap"], 64); err == nil {
				stats.MarketCap = mc
			}
		}
	}

	return stats, nil
}

// GetPriceHistory retrieves historical price data for a given time range
func (q *MarketAnalyticsQuery) GetPriceHistory(
	ctx context.Context,
	tradePair string,
	period AnalyticsPeriod,
	startTime time.Time,
	endTime time.Time,
) ([]TimeSeriesPoint, error) {
	// Get all available price history keys for this pair
	pattern := fmt.Sprintf("%s%s:*price", q.keyPrefix, tradePair)
	keys, err := q.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get price history keys: %w", err)
	}

	var allPrices []TimeSeriesPoint
	for _, key := range keys {
		results, err := q.client.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
			Min: strconv.FormatInt(startTime.Unix(), 10),
			Max: strconv.FormatInt(endTime.Unix(), 10),
		}).Result()
		if err != nil {
			continue
		}

		for _, result := range results {
			price, err := strconv.ParseFloat(fmt.Sprint(result.Member), 64)
			if err != nil {
				continue
			}
			timestamp := time.Unix(int64(result.Score), 0)
			allPrices = append(allPrices, TimeSeriesPoint{
				Timestamp: timestamp,
				Value:     price,
			})
		}
	}

	// Sort by timestamp
	sort.Slice(allPrices, func(i, j int) bool {
		return allPrices[i].Timestamp.Before(allPrices[j].Timestamp)
	})

	return allPrices, nil
}

// GetVolumeHistory retrieves historical volume data
func (q *MarketAnalyticsQuery) GetVolumeHistory(
	ctx context.Context,
	tradePair string,
	period AnalyticsPeriod,
	startTime time.Time,
	endTime time.Time,
) ([]TimeSeriesPoint, error) {
	key := fmt.Sprintf("%s%s:%s:volume", q.keyPrefix, tradePair, period)

	results, err := q.client.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
		Min: strconv.FormatInt(startTime.Unix(), 10),
		Max: strconv.FormatInt(endTime.Unix(), 10),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get volume history: %w", err)
	}

	points := make([]TimeSeriesPoint, len(results))
	for i, result := range results {
		volume, err := strconv.ParseFloat(fmt.Sprint(result.Member), 64)
		if err != nil {
			continue
		}
		points[i] = TimeSeriesPoint{
			Timestamp: time.Unix(int64(result.Score), 0),
			Value:     volume,
		}
	}

	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp.Before(points[j].Timestamp)
	})

	return points, nil
}

// GetMarketDepthHistory retrieves historical market depth data
func (q *MarketAnalyticsQuery) GetMarketDepthHistory(
	ctx context.Context,
	tradePair string,
	period AnalyticsPeriod,
	startTime time.Time,
	endTime time.Time,
) ([]MarketDepthResponse, error) {
	spreadKey := fmt.Sprintf("%s%s:%s:spread", q.keyPrefix, tradePair, period)

	results, err := q.client.ZRangeByScoreWithScores(ctx, spreadKey, &redis.ZRangeBy{
		Min: strconv.FormatInt(startTime.Unix(), 10),
		Max: strconv.FormatInt(endTime.Unix(), 10),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get market depth history: %w", err)
	}

	depths := make([]MarketDepthResponse, len(results))
	for i, result := range results {
		spread, err := strconv.ParseFloat(fmt.Sprint(result.Member), 64)
		if err != nil {
			continue
		}
		depths[i] = MarketDepthResponse{
			Spread: spread,
		}
	}

	return depths, nil
}

// GetAggregatedStats calculates aggregated statistics for a time period
func (q *MarketAnalyticsQuery) GetAggregatedStats(
	ctx context.Context,
	tradePair string,
	period AnalyticsPeriod,
	startTime time.Time,
	endTime time.Time,
) (*MarketStatsResponse, error) {
	prices, err := q.GetPriceHistory(ctx, tradePair, period, startTime, endTime)
	if err != nil {
		return nil, err
	}

	if len(prices) == 0 {
		return nil, fmt.Errorf("no data available for the specified period")
	}

	// Calculate aggregated statistics
	stats := &MarketStatsResponse{
		TradePair:   tradePair,
		OpenPrice:   prices[0].Value,
		ClosePrice:  prices[len(prices)-1].Value,
		HighPrice:   prices[0].Value,
		LowPrice:    prices[0].Value,
		LastUpdated: endTime,
	}

	for _, p := range prices {
		if p.Value > stats.HighPrice {
			stats.HighPrice = p.Value
		}
		if p.Value < stats.LowPrice {
			stats.LowPrice = p.Value
		}
	}

	stats.PriceChange = ((stats.ClosePrice - stats.OpenPrice) / stats.OpenPrice) * 100

	return stats, nil
}

// GetAvailableTradePairs returns a list of active trading pairs
func (q *MarketAnalyticsQuery) GetAvailableTradePairs(ctx context.Context) ([]string, error) {
	pattern := fmt.Sprintf("%s*:latest", q.keyPrefix)
	keys, err := q.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get trading pairs: %w", err)
	}

	// Extract trading pair from key
	pairs := make([]string, 0, len(keys))
	prefix := q.keyPrefix
	suffix := ":latest"
	for _, key := range keys {
		pair := key[len(prefix) : len(key)-len(suffix)]
		pairs = append(pairs, pair)
	}

	return pairs, nil
}

// GetOverallPriceChange calculates the total price change percentage
func (q *MarketAnalyticsQuery) GetOverallPriceChange(
	ctx context.Context,
	tradePair string,
) (float64, error) {
	startTime := time.Now().Add(-365 * 24 * time.Hour)
	endTime := time.Now()

	stats, err := q.GetAggregatedStats(ctx, tradePair, Day, startTime, endTime)
	if err != nil {
		return 0, err
	}

	return stats.PriceChange, nil
}

// GetTotalMarketVolume calculates total volume across all pairs for a time period
func (q *MarketAnalyticsQuery) GetTotalMarketVolume(
	ctx context.Context,
	startTime time.Time,
	endTime time.Time,
) (float64, error) {
	pattern := fmt.Sprintf("%s*:24hvolume", q.keyPrefix)
	keys, err := q.client.Keys(ctx, pattern).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get volume keys: %w", err)
	}

	var totalVolume float64
	for _, key := range keys {
		result, err := q.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		volume, err := strconv.ParseFloat(result, 64)
		if err != nil {
			continue
		}
		totalVolume += volume
	}

	return totalVolume, nil
}

// GetTopAssets retrieves the top assets based on market cap
func (q *MarketAnalyticsQuery) GetTopAssets(ctx context.Context, limit int) ([]AssetStats, error) {
	// Get all asset keys
	keys, err := q.client.Keys(ctx, "stellar:asset:*").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get asset keys: %w", err)
	}

	var assets []AssetStats
	for _, key := range keys {
		// Parse asset code and issuer from key
		parts := strings.Split(strings.TrimPrefix(key, "stellar:asset:"), ":")
		if len(parts) != 2 {
			continue
		}
		code, issuer := parts[0], parts[1]

		// Get asset data
		assetData, err := q.client.HGetAll(ctx, key).Result()
		if err != nil {
			continue
		}

		// Get price data
		priceKey := fmt.Sprintf("stellar:price:%s:%s:native::latest", code, issuer)
		priceData, err := q.client.HGetAll(ctx, priceKey).Result()
		if err != nil {
			continue
		}

		asset := AssetStats{
			Code:   code,
			Issuer: issuer,
		}

		// Parse asset data
		if supply, err := strconv.ParseFloat(assetData["circulating_supply"], 64); err == nil {
			asset.CirculatingSupply = supply
		}
		if holders, err := strconv.ParseInt(assetData["num_holders"], 10, 64); err == nil {
			asset.NumHolders = holders
		}
		if t, err := time.Parse(time.RFC3339, assetData["last_updated"]); err == nil {
			asset.LastUpdated = t
		}

		// Parse price data
		if price, err := strconv.ParseFloat(priceData["price"], 64); err == nil {
			asset.Price = price
			asset.MarketCap = asset.CirculatingSupply * price
		}
		if vol, err := strconv.ParseFloat(priceData["volume_24h"], 64); err == nil {
			asset.Volume24h = vol
		}
		if high, err := strconv.ParseFloat(priceData["high_24h"], 64); err == nil {
			asset.High24h = high
		}
		if low, err := strconv.ParseFloat(priceData["low_24h"], 64); err == nil {
			asset.Low24h = low
		}

		assets = append(assets, asset)
	}

	// Sort by market cap
	sort.Slice(assets, func(i, j int) bool {
		return assets[i].MarketCap > assets[j].MarketCap
	})

	// Return top N assets
	if len(assets) > limit {
		assets = assets[:limit]
	}

	return assets, nil
}
