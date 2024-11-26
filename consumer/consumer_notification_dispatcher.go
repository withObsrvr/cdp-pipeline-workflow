package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/sendgrid/sendgrid-go"
	"github.com/sendgrid/sendgrid-go/helpers/mail"
	"github.com/slack-go/slack"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// NotificationRule defines when to send notifications
type NotificationRule struct {
	Type      string   `json:"type"`      // payment, create_account, etc.
	Condition string   `json:"condition"` // gt, lt, eq
	Field     string   `json:"field"`     // amount, starting_balance, etc.
	Value     string   `json:"value"`     // threshold value
	Channels  []string `json:"channels"`  // slack, email, webhook
}

type NotificationDispatcher struct {
	rules           []NotificationRule
	slackClient     *slack.Client
	sendgridClient  *sendgrid.Client
	emailFrom       string
	emailTo         []string
	slackChannels   []string
	webhookURLs     []string
	processors      []processor.Processor
	notificationLog map[string]time.Time // To prevent notification spam
	mutex           sync.RWMutex
}

func NewNotificationDispatcher(config map[string]interface{}) (*NotificationDispatcher, error) {
	// Parse configuration
	slackToken, _ := config["slack_token"].(string)
	sendgridKey, _ := config["sendgrid_key"].(string)
	emailFrom, _ := config["email_from"].(string)
	emailTo, _ := config["email_to"].([]string)
	slackChannels, _ := config["slack_channels"].([]string)
	webhookURLs, _ := config["webhook_urls"].([]string)

	// Parse rules from configuration
	rulesData, ok := config["rules"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("missing or invalid rules configuration")
	}

	var rules []NotificationRule
	for _, r := range rulesData {
		ruleMap, ok := r.(map[string]interface{})
		if !ok {
			continue
		}
		rule := NotificationRule{
			Type:      ruleMap["type"].(string),
			Condition: ruleMap["condition"].(string),
			Field:     ruleMap["field"].(string),
			Value:     ruleMap["value"].(string),
			Channels:  ruleMap["channels"].([]string),
		}
		rules = append(rules, rule)
	}

	dispatcher := &NotificationDispatcher{
		rules:           rules,
		emailFrom:       emailFrom,
		emailTo:         emailTo,
		slackChannels:   slackChannels,
		webhookURLs:     webhookURLs,
		notificationLog: make(map[string]time.Time),
	}

	// Initialize Slack client if token provided
	if slackToken != "" {
		dispatcher.slackClient = slack.New(slackToken)
	}

	// Initialize SendGrid client if key provided
	if sendgridKey != "" {
		dispatcher.sendgridClient = sendgrid.NewSendClient(sendgridKey)
	}

	return dispatcher, nil
}

func (n *NotificationDispatcher) Subscribe(processor processor.Processor) {
	n.processors = append(n.processors, processor)
}

func (n *NotificationDispatcher) Process(ctx context.Context, msg processor.Message) error {
	payloadBytes, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte type for message.Payload, got %T", msg.Payload)
	}

	// Determine operation type and check rules
	var data map[string]interface{}
	if err := json.Unmarshal(payloadBytes, &data); err != nil {
		return fmt.Errorf("error unmarshaling payload: %w", err)
	}

	operationType, ok := data["type"].(string)
	if !ok {
		return fmt.Errorf("missing operation type in payload")
	}

	// Check each rule
	for _, rule := range n.rules {
		if rule.Type == operationType {
			if n.shouldNotify(rule, data) {
				if err := n.dispatchNotifications(rule, data); err != nil {
					log.Printf("Error dispatching notifications: %v", err)
				}
			}
		}
	}

	return nil
}

func (n *NotificationDispatcher) shouldNotify(rule NotificationRule, data map[string]interface{}) bool {
	fieldValue, ok := data[rule.Field].(string)
	if !ok {
		return false
	}

	// Check rate limiting
	key := fmt.Sprintf("%s-%s-%s", rule.Type, rule.Field, fieldValue)
	n.mutex.RLock()
	lastNotification, exists := n.notificationLog[key]
	n.mutex.RUnlock()

	if exists && time.Since(lastNotification) < time.Minute*5 {
		return false
	}

	// Update notification log
	n.mutex.Lock()
	n.notificationLog[key] = time.Now()
	n.mutex.Unlock()

	// Compare values based on condition
	switch rule.Condition {
	case "gt":
		return fieldValue > rule.Value
	case "lt":
		return fieldValue < rule.Value
	case "eq":
		return fieldValue == rule.Value
	default:
		return false
	}
}

func (n *NotificationDispatcher) dispatchNotifications(rule NotificationRule, data map[string]interface{}) error {
	message := n.formatMessage(rule, data)

	for _, channel := range rule.Channels {
		switch channel {
		case "slack":
			if err := n.sendSlackNotification(message); err != nil {
				log.Printf("Error sending Slack notification: %v", err)
			}
		case "email":
			if err := n.sendEmailNotification(message); err != nil {
				log.Printf("Error sending email notification: %v", err)
			}
		case "webhook":
			if err := n.sendWebhookNotification(message); err != nil {
				log.Printf("Error sending webhook notification: %v", err)
			}
		}
	}

	return nil
}

func (n *NotificationDispatcher) formatMessage(rule NotificationRule, data map[string]interface{}) string {
	return fmt.Sprintf("Alert: %s operation with %s %s %s\nDetails: %v",
		rule.Type, rule.Field, rule.Condition, rule.Value, data)
}

func (n *NotificationDispatcher) sendSlackNotification(message string) error {
	if n.slackClient == nil {
		return fmt.Errorf("slack client not initialized")
	}

	for _, channel := range n.slackChannels {
		_, _, err := n.slackClient.PostMessage(
			channel,
			slack.MsgOptionText(message, false),
		)
		if err != nil {
			return fmt.Errorf("error sending slack message: %w", err)
		}
	}
	return nil
}

func (n *NotificationDispatcher) sendEmailNotification(message string) error {
	if n.sendgridClient == nil {
		return fmt.Errorf("sendgrid client not initialized")
	}

	from := mail.NewEmail("Obsrvr Notifications", n.emailFrom)
	subject := "Stellar Network Alert"

	for _, to := range n.emailTo {
		toEmail := mail.NewEmail("", to)
		email := mail.NewSingleEmail(from, subject, toEmail, message, message)
		_, err := n.sendgridClient.Send(email)
		if err != nil {
			return fmt.Errorf("error sending email: %w", err)
		}
	}
	return nil
}

func (n *NotificationDispatcher) sendWebhookNotification(message string) error {
	payload := map[string]interface{}{
		"message":   message,
		"timestamp": time.Now().UTC(),
	}

	log.Printf("Sending webhook notification: %v", payload)

	for _, url := range n.webhookURLs {
		// Create HTTP client with context
		client := &http.Client{Timeout: 10 * time.Second}
		jsonPayload, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("error marshalling payload: %w", err)
		}
		// Send POST request to webhook URL
		resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonPayload))
		if err != nil {
			return fmt.Errorf("error sending webhook request: %w", err)
		}
		defer resp.Body.Close()
		// Handle response
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("received non-200 response: %d", resp.StatusCode)
		}
	}
	return nil
}

func (n *NotificationDispatcher) Close() error {
	// Cleanup if needed
	return nil
}
