package handlers

// Handlers aggregates every HTTP handler into one injectable struct.
//
// Wire builds this once and the route registration reads from it. Without the
// aggregate, every new handler would mean a new parameter on the server
// constructor and a new provider argument in three places.
type Handlers struct {
	JobHandler       *JobHandler
	QueueHandler     *QueueHandler
	SupportHandler   *SupportHandler
	DashboardHandler *DashboardHandler
}

// NewHandlers builds the aggregate.
func NewHandlers(
	jobHandler *JobHandler,
	queueHandler *QueueHandler,
	supportHandler *SupportHandler,
	dashboardHandler *DashboardHandler,
) *Handlers {
	return &Handlers{
		JobHandler:       jobHandler,
		QueueHandler:     queueHandler,
		SupportHandler:   supportHandler,
		DashboardHandler: dashboardHandler,
	}
}
