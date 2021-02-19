package go_sqs

import (
	"log"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// Handler provides a method to act on the message received
type Handler interface {
	HandleMessage(msg *sqs.Message) error
}

// HandlerFunc is responsible for implementing the Handler
type HandlerFunc func(msg *sqs.Message) error

// HandleMessage is the function which executes the func for the SQS message
func (f HandlerFunc) HandleMessage(msg *sqs.Message) error {
	return f(msg)
}

// Listener struct keeps SQS connection information and details
type Listener struct {
	sqs             *sqs.SQS
	sqsJobUrl       string
	maxNumberOfMsg  int64
	waitTimeSeconds int64
}

// NewListener initializes and generates a new listener instance
func NewListener(sqs *sqs.SQS, jobUrl string, maxNumberOfMsg, waitTimeSeconds int64) *Listener {
	return &Listener{
		sqs:             sqs,
		sqsJobUrl:       jobUrl,
		maxNumberOfMsg:  maxNumberOfMsg,
		waitTimeSeconds: waitTimeSeconds,
	}
}

// Start continuously polls for new messages in the SQS queue - until the service is stopped
func (l *Listener) Start(h Handler) {
	for {
		params := &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(l.sqsJobUrl),
			MaxNumberOfMessages: aws.Int64(l.maxNumberOfMsg),
			MessageAttributeNames: []*string{
				aws.String("All"),
			},
			WaitTimeSeconds: aws.Int64(l.waitTimeSeconds),
		}

		resp, err := l.sqs.ReceiveMessage(params)
		if err != nil {
			continue
		}
		if len(resp.Messages) > 0 {
			l.msgLauncher(h, resp.Messages)
		}
	}
}

func (l *Listener) msgLauncher(h Handler, messages []*sqs.Message) {
	var wg sync.WaitGroup

	for _, message := range messages {
		wg.Add(1)
		go func(m *sqs.Message) {
			defer wg.Done()
			if err := l.handleMessage(m, h); err != nil {
				log.Println("failure while handling message")
			}
		}(message)
	}

	wg.Wait()
}

func (l *Listener) handleMessage(m *sqs.Message, h Handler) error {
	// call the HandleMessage of the Handler to process the message
	err := h.HandleMessage(m)
	if err != nil {
		return err
	}

	// Delete the processed message
	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(l.sqsJobUrl),
		ReceiptHandle: m.ReceiptHandle,
	}
	_, err = l.sqs.DeleteMessage(params)
	if err != nil {
		return err
	}

	return nil
}
