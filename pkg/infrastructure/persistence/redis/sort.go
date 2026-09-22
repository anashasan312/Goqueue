package redis

import (
	"sort"

	jobAgg "github.com/anashasan/goqueue/pkg/domain/job_aggregate"
)

// sortJobsByCreatedAtDesc orders jobs newest first, breaking ties on id so that
// two refreshes of the dashboard render the same order.
func sortJobsByCreatedAtDesc(jobs []*jobAgg.Job) {
	sort.SliceStable(jobs, func(i, j int) bool {
		left, right := jobs[i], jobs[j]
		if left.CreatedAt().Equal(right.CreatedAt()) {
			return left.ID() < right.ID()
		}
		return left.CreatedAt().After(right.CreatedAt())
	})
}
