package main

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	sqsworker "github.com/koslib/go-sqs-worker"
)

var (
	sqsJobUrl string
)

func main() {
	sqsJobUrl = "add your SQS url here"

	awsSession := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	}))
	sqsQueue := sqs.New(awsSession)

	publisher := sqsworker.NewPublisher(sqsQueue, sqsJobUrl, 10)

	payload := "this is a test message body"
	err := publisher.PublishMessage(
		payload,
		map[string]*sqs.MessageAttributeValue{
			"Foo": {
				DataType:    aws.String("String"),
				StringValue: aws.String("Bar"),
			},
		},
	)
	if err != nil {
		log.Println("failed to publish message")
	}

	listener := sqsworker.NewListener(sqsQueue, sqsJobUrl, 5, 10)
	listener.Start(MyMsgHandler{})

}

// MyMsgHandler implements the Handler interface
type MyMsgHandler struct {
	// list any dependencies for your handler here
}

// HandleMessage just prints the body of the message received
func (m MyMsgHandler) HandleMessage(msg *sqs.Message) error {
	fmt.Println(msg.Body)
	return nil
}
