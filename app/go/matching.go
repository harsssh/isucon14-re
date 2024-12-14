package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
)

var matchingQueue = make(chan string, 100)

func matchingLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case rideID := <-matchingQueue:
			createMatch(ctx, rideID)
		}
	}
}

func initMatchingQueue() {
	var rideIDs []string
	if err := db.Select(&rideIDs, "SELECT id FROM rides WHERE chair_id IS NULL"); err != nil {
		panic(err)
	}
	for _, rideID := range rideIDs {
		matchingQueue <- rideID
	}
}

func createMatch(ctx context.Context, rideID string) {
	matched := &Chair{}
	// 10回ランダムに引いてみる
	for i := 0; i < 10; i++ {
		randomActiveChair := &Chair{}
		if err := db.GetContext(ctx, randomActiveChair, "SELECT * FROM chairs INNER JOIN (SELECT id FROM chairs WHERE is_active = TRUE ORDER BY RAND() LIMIT 1) AS tmp ON chairs.id = tmp.id LIMIT 1"); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				slog.Info("no active chair", rideID)
				// 再度キューに入れておく
				matchingQueue <- rideID
				return
			}
			slog.Error(err.Error())
			return
		}

		maybeActiveRides, _ := cache.activeRides.Get(ctx, randomActiveChair.ID)
		if maybeActiveRides.Value != 0 {
			// COMPLETED でないものはマッチング対象外
			continue
		}

		matched = randomActiveChair
	}

	if matched == nil {
		// 後で引き直す
		matchingQueue <- rideID
		return
	}

	if _, err := db.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", matched.ID, rideID); err != nil {
		slog.Error(err.Error())
		return
	}

	activeRides, err := cache.activeRides.Get(ctx, matched.ID)
	if err != nil {
		slog.Error(err.Error())
		return
	}
	cache.activeRides.Set(ctx, matched.ID, activeRides.Value+1)
}
