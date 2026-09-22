package job

// ListJobsQuery is the query string binding for a job listing.
type ListJobsQuery struct {
	Queue    string `form:"queue"`
	State    string `form:"state" binding:"omitempty,oneof=scheduled pending active retrying completed dead"`
	TaskType string `form:"task_type"`
	Offset   int64  `form:"offset" binding:"omitempty,min=0"`
	Limit    int64  `form:"limit" binding:"omitempty,min=1,max=500"`
}

// ListJobsRes is a page of job summaries plus the totals a client needs to
// render pagination without a second request.
type ListJobsRes struct {
	Jobs   []JobSummaryRes `json:"jobs"`
	Total  int64           `json:"total"`
	Offset int64           `json:"offset"`
	Limit  int64           `json:"limit"`
}

// JobActionRes acknowledges a state-changing action on a job.
type JobActionRes struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

// BulkActionRes reports how many jobs an operation affected.
type BulkActionRes struct {
	Affected int64 `json:"affected"`
}
