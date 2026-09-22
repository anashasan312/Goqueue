package server

import "github.com/gin-gonic/gin"

// registerAPIRoutes mounts the versioned REST API on an engine.
//
// Route registration is kept out of server.go and grouped by resource so that
// the whole surface of the API is readable in one screen. It is the document a
// new contributor reads to learn what the service does.
func (s *HTTPServer) registerAPIRoutes(engine *gin.Engine) {
	api := engine.Group(APIBasePath)

	registerJobRoutes(api, s.handlers.JobHandler)
	registerQueueRoutes(api, s.handlers.QueueHandler)
}
