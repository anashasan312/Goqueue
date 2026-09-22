// Package queue holds the request and response DTOs for the queue API.
package queue

// QueueStatsRes is the depth snapshot of one queue.
type QueueStatsRes struct {
	Queue     string `json:"queue"`
	Paused    bool   `json:"paused"`
	Weight    uint8  `json:"weight"`
	Pending   int64  `json:"pending"`
	Active    int64  `json:"active"`
	Scheduled int64  `json:"scheduled"`
	Retrying  int64  `json:"retrying"`
	Completed int64  `json:"completed"`
	Dead      int64  `json:"dead"`
	Backlog   int64  `json:"backlog"`
	Total     int64  `json:"total"`
}

// ListQueueStatsRes is the dashboard's top-level view: every queue plus the
// rolled-up totals, so the header does not have to be computed client side.
type ListQueueStatsRes struct {
	Queues []QueueStatsRes `json:"queues"`
	Totals QueueStatsRes   `json:"totals"`
}

// PauseQueueReq toggles consumption of a queue.
type PauseQueueReq struct {
	Paused bool `json:"paused"`
}

// QueueActionRes acknowledges a queue-level action.
type QueueActionRes struct {
	Queue  string `json:"queue"`
	Paused bool   `json:"paused"`
}
