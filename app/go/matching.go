package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"
)

func matchingLoop(ctx context.Context, interval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			createMatch(ctx)
			time.Sleep(interval)
		}
	}
}

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func createMatch(ctx context.Context) {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		slog.Error(err.Error())
		return
	}
	defer tx.Rollback()

	// MEMO: 一旦最も待たせているリクエストに適当な空いている椅子マッチさせる実装とする。おそらくもっといい方法があるはず…
	ride := &Ride{}
	if err := tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at LIMIT 1`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			//slog.Info("未マッチのライドがありません")
			return
		}
		slog.Error(err.Error())
		return
	}

	matched := &Chair{}
	empty := false
	for i := 0; i < 10; i++ {
		if err := tx.GetContext(ctx, matched, "SELECT * FROM chairs INNER JOIN (SELECT id FROM chairs WHERE is_active = TRUE ORDER BY RAND() LIMIT 1) AS tmp ON chairs.id = tmp.id LIMIT 1"); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				//slog.Info("空いている椅子がありません")
				return
			}
			slog.Error(err.Error())
		}

		if err := tx.GetContext(ctx, &empty, "SELECT COUNT(*) = 0 FROM (SELECT COUNT(chair_sent_at) = 6 AS completed FROM ride_statuses WHERE ride_id IN (SELECT id FROM rides WHERE chair_id = ?) GROUP BY ride_id) is_completed WHERE completed = FALSE", matched.ID); err != nil {
			slog.Error(err.Error())
			return
		}
		if empty {
			break
		}
	}
	if !empty {
		//slog.Info("空いている椅子がありません")
		return
	}

	if _, err := tx.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", matched.ID, ride.ID); err != nil {
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

	//slog.Info("マッチングが完了しました")
}
