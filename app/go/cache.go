package main

import (
	"context"
	"database/sql"
	"github.com/samber/lo"
)

var cache *AppCache = nil

type ChairTotalDistance struct {
	ChairID                string       `db:"chair_id"`
	TotalDistance          int          `db:"total_distance"`
	TotalDistanceUpdatedAt sql.NullTime `db:"total_distance_updated_at"`
}

type AppCache struct {
	chairTotalDistances Cache[string, *ChairTotalDistance]
	latestChairLocation Cache[string, *ChairLocation]
	activeRides         Cache[string, int]
	// access_token -> chair_id
	chairSessions    Cache[string, string]
	latestRideStatus Cache[string, string]
}

func NewAppCache(ctx context.Context) *AppCache {
	c := &AppCache{
		// chair が 530 くらい
		chairTotalDistances: lo.Must1(NewInMemoryLRUCache[string, *ChairTotalDistance](1000)),
		latestChairLocation: lo.Must1(NewInMemoryLRUCache[string, *ChairLocation](1000)),
		activeRides:         lo.Must1(NewInMemoryLRUCache[string, int](1000)),
		chairSessions:       lo.Must1(NewInMemoryLRUCache[string, string](1000)),
		// ride が 1200 くらい?
		latestRideStatus: lo.Must1(NewInMemoryLRUCache[string, string](2000)),
	}

	// chairTotalDistances の初期化
	var totalDistances []*ChairTotalDistance
	if err := db.Select(&totalDistances, `
		WITH tmp AS (
			SELECT chair_id,
				   created_at,
				   ABS(latitude - LAG(latitude) OVER (PARTITION BY chair_id ORDER BY created_at)) +
				   ABS(longitude - LAG(longitude) OVER (PARTITION BY chair_id ORDER BY created_at)) AS distance
			FROM chair_locations
		)
		SELECT chair_id,
			   SUM(IFNULL(distance, 0)) AS total_distance,
			   MAX(created_at)          AS total_distance_updated_at
		FROM tmp
		GROUP BY chair_id;
	`); err != nil {
		panic(err)
	}
	for _, totalDistance := range totalDistances {
		c.chairTotalDistances.Set(ctx, totalDistance.ChairID, totalDistance)
	}

	var chairLocations []*ChairLocation
	if err := db.Select(&chairLocations, `
		WITH tmp AS (
		    SELECT id, MAX(created_at) FROM chair_locations GROUP BY chair_id, id
		)
		SELECT * FROM chair_locations WHERE id IN (SELECT id FROM tmp)
	`); err != nil {
		panic(err)
	}
	for _, chairLocation := range chairLocations {
		c.latestChairLocation.Set(ctx, chairLocation.ChairID, chairLocation)
	}

	var chairs []*Chair
	db.Select(&chairs, `SELECT * FROM chairs`)

	for _, chair := range chairs {
		c.chairSessions.Set(ctx, chair.AccessToken, chair.ID)

		var rides []*Ride
		count := 0

		db.Select(&rides, `SELECT * FROM rides WHERE chair_id = ? ORDER BY created_at DESC`, chair.ID)

		for _, ride := range rides {
			// 過去にライドが存在し、かつ、それが完了していない場合はスキップ
			status := lo.Must1(getLatestRideStatus(ctx, db, ride.ID))

			if status != "COMPLETED" {
				count++
			}
		}

		c.activeRides.Set(ctx, chair.ID, count)
	}

	var rides []*Ride
	db.Select(&rides, `SELECT * FROM rides`)
	for _, ride := range rides {
		var status string
		if err := db.Get(&status, `SELECT status FROM ride_statuses WHERE ride_id = ? ORDER BY created_at DESC LIMIT 1`, ride.ID); err != nil {
			panic(err)
		}
		c.latestRideStatus.Set(ctx, ride.ID, status)
	}

	return c
}

func updateLatestLocationCache(ctx context.Context, loc *ChairLocation) {
	_ = cache.latestChairLocation.Set(ctx, loc.ChairID, loc)
}

func updateTotalDistanceCache(ctx context.Context, prevLoc Maybe[*ChairLocation], loc *ChairLocation) {
	diff := lo.TernaryF(
		prevLoc.Found,
		func() int {
			return calculateDistance(prevLoc.Value.Latitude, prevLoc.Value.Longitude, loc.Latitude, loc.Longitude)
		},
		Const(0),
	)
	current, _ := cache.chairTotalDistances.Get(ctx, loc.ChairID)
	cache.chairTotalDistances.Set(ctx, loc.ChairID, &ChairTotalDistance{
		ChairID: loc.ChairID,
		TotalDistance: lo.TernaryF(current.Found, func() int {
			return current.Value.TotalDistance + diff
		}, Const(0)),
		TotalDistanceUpdatedAt: sql.NullTime{
			Time:  loc.CreatedAt,
			Valid: true,
		},
	})
}
