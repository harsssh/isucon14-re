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

func createMatch(ctx context.Context, rideID string) {
	slog.Info("start matching", rideID)

	matched := &Chair{}
	empty := false
	for i := 0; i < 10; i++ {
		slog.Info("try matching", i, rideID)
		if err := db.GetContext(ctx, matched, "SELECT * FROM chairs INNER JOIN (SELECT id FROM chairs WHERE is_active = TRUE ORDER BY RAND() LIMIT 1) AS tmp ON chairs.id = tmp.id LIMIT 1"); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				slog.Info("no active chair", rideID)
				//slog.Info("空いている椅子がありません")
				return
			}
			slog.Error(err.Error())
		}

		if err := db.GetContext(ctx, &empty, "SELECT COUNT(*) = 0 FROM (SELECT COUNT(chair_sent_at) = 6 AS completed FROM ride_statuses WHERE ride_id IN (SELECT id FROM rides WHERE chair_id = ?) GROUP BY ride_id) is_completed WHERE completed = FALSE", matched.ID); err != nil {
			slog.Error(err.Error())
			return
		}

		slog.Info("count", rideID)

		if empty {
			break
		}
	}
	if !empty {
		slog.Info("!empty", rideID)
		//slog.Info("空いている椅子がありません")
		return
	}

	slog.Info("create match", rideID)

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

	//slog.Info("マッチングが完了しました")
}
