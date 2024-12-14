package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"log/slog"
)

var matchingQueue = make(chan string, 100)

func matchingLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case rideID := <-matchingQueue:
			go createMatch(ctx, rideID)
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
	log.Println(len(matchingQueue), "matchingQueue")

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		slog.Error(err.Error())
		return
	}
	defer tx.Rollback()

	var matched *Chair
	// 10回ランダムに引いてみる
	for i := 0; i < 10; i++ {
		//log.Println("try matching", rideID, i)
		randomActiveChair := &Chair{}
		if err := tx.GetContext(ctx, randomActiveChair, "SELECT * FROM chairs INNER JOIN (SELECT id FROM chairs WHERE is_active = TRUE ORDER BY RAND() LIMIT 1) AS tmp ON chairs.id = tmp.id LIMIT 1"); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				//slog.Info("no active chair", rideID)
				// 再度キューに入れておく
				matchingQueue <- rideID
				return
			}
			slog.Error(err.Error())
			return
		}

		var isIdle bool
		if err := tx.GetContext(ctx, &isIdle,
			"SELECT COUNT(*) = 0 FROM (SELECT COUNT(chair_sent_at) = 6 AS completed FROM ride_statuses WHERE ride_id IN (SELECT id FROM rides WHERE chair_id = ?) GROUP BY ride_id) is_completed WHERE completed = FALSE",
			randomActiveChair.ID); err != nil {
			slog.Error(err.Error())
			return
		}
		if !isIdle {
			continue
		}

		//log.Println("matched", rideID)
		matched = randomActiveChair
		break
	}

	if matched == nil {
		//log.Println("no matched", rideID)
		// 後で引き直す
		matchingQueue <- rideID
		return
	}

	if _, err := tx.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", matched.ID, rideID); err != nil {
		slog.Error(err.Error())
		return
	}

	if err := tx.Commit(); err != nil {
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
