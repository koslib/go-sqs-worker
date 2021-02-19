package sqsworker

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Publisher struct {
	sqs                 *sqs.SQS
	sqsQueueUrl         string
	messageDelaySeconds int64
}

func NewPublisher(sqs *sqs.SQS, queueUrl string, msgDelaySec int64) *Publisher {
	return &Publisher{
		sqs:                 sqs,
		sqsQueueUrl:         queueUrl,
		messageDelaySeconds: msgDelaySec,
	}
}

func (p *Publisher) PublishMessage(payload string, msgAttributes map[string]*sqs.MessageAttributeValue) error {
	_, err := p.sqs.SendMessage(&sqs.SendMessageInput{
		DelaySeconds:      aws.Int64(p.messageDelaySeconds),
		MessageAttributes: msgAttributes,
		MessageBody:       aws.String(payload),
		QueueUrl:          aws.String(p.sqsQueueUrl),
	})
	if err != nil {
		return err
	}

	return nil
}
