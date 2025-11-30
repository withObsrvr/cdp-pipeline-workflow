package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/withobsrvr/flowctl/pkg/console/heartbeat"
)

// initializeHeartbeatClient creates and starts a console heartbeat client if env vars are configured.
// Returns nil if heartbeat is disabled (missing env vars).
func initializeHeartbeatClient() *heartbeat.Client {
	consoleURL := os.Getenv("OBSRVR_CONSOLE_URL")
	if consoleURL == "" {
		log.Printf("Console heartbeat disabled (OBSRVR_CONSOLE_URL not set)")
		return nil
	}

	pipelineID := os.Getenv("PIPELINE_ID")
	sessionID := os.Getenv("OBSRVR_SESSION_ID")
	webhookSecret := os.Getenv("OBSRVR_WEBHOOK_SECRET")

	if pipelineID == "" || sessionID == "" || webhookSecret == "" {
		log.Printf("Console heartbeat disabled (missing env vars: PIPELINE_ID=%v, SESSION_ID=%v, SECRET=%v)",
			pipelineID != "", sessionID != "", webhookSecret != "")
		return nil
	}

	client := heartbeat.NewClient(consoleURL, pipelineID, sessionID, webhookSecret)
	log.Printf("Console heartbeat client initialized for pipeline %s, session %s", pipelineID, sessionID)

	// Start background heartbeat loop (5 minutes)
	go client.StartHeartbeatLoop(context.Background(), 5*time.Minute)

	return client
}
