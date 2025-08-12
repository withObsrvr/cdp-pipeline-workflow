package control

import (
	"context"
	"fmt"
	"time"

	pb "github.com/withobsrvr/flowctl/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn            *grpc.ClientConn
	client          pb.ControlPlaneClient
	serviceInfo     *pb.ServiceInfo
	metricsProvider MetricsProvider
	healthChecker   HealthChecker
}

type MetricsProvider interface {
	GetMetrics() map[string]float64
}

type HealthChecker interface {
	IsHealthy() bool
	GetHealthDetails() map[string]string
}

func NewClient(endpoint string, serviceID string, serviceName string) (*Client, error) {
	conn, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to control plane: %w", err)
	}

	return &Client{
		conn:   conn,
		client: pb.NewControlPlaneClient(conn),
		serviceInfo: &pb.ServiceInfo{
			ServiceId:   serviceID,
			ServiceType: pb.ServiceType_SERVICE_TYPE_PIPELINE,
			Metadata: map[string]string{
				"service_name": serviceName,
			},
		},
	}, nil
}

func (c *Client) SetMetricsProvider(mp MetricsProvider) {
	c.metricsProvider = mp
}

func (c *Client) SetHealthChecker(hc HealthChecker) {
	c.healthChecker = hc
}

func (c *Client) Register(ctx context.Context, metadata map[string]string) error {
	// Merge additional metadata with existing metadata
	for k, v := range metadata {
		c.serviceInfo.Metadata[k] = v
	}

	ack, err := c.client.Register(ctx, c.serviceInfo)
	if err != nil {
		return fmt.Errorf("registration failed: %w", err)
	}

	if ack == nil {
		return fmt.Errorf("registration failed: nil acknowledgment")
	}

	fmt.Printf("Registered CDP pipeline with control plane: %s\n", ack.ServiceId)

	return nil
}

func (c *Client) StartHeartbeat(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.sendHeartbeat(ctx)
		}
	}
}

func (c *Client) sendHeartbeat(ctx context.Context) {
	metrics := make(map[string]float64)
	if c.metricsProvider != nil {
		metrics = c.metricsProvider.GetMetrics()
	}

	heartbeat := &pb.ServiceHeartbeat{
		ServiceId: c.serviceInfo.ServiceId,
		Timestamp: nil, // Let server set timestamp
		Metrics:   metrics,
	}

	_, err := c.client.Heartbeat(ctx, heartbeat)
	if err != nil {
		// Log error but don't stop heartbeat loop
		fmt.Printf("Failed to send heartbeat: %v\n", err)
	}
}

func (c *Client) Close() error {
	return c.conn.Close()
}
