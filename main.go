package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func main() {
	// Create a new AWS session
	sess, err := session.NewSession(&aws.Config{
		Region:   aws.String("us-west-2"),             // Replace with your desired region
		Endpoint: aws.String("http://localhost:9324"), // ElasticMQ Docker container endpoint
	})
	if err != nil {
		fmt.Println("Failed to create session:", err)
		return
	}

	// Create an SQS service client
	sqsClient := sqs.New(sess)

	// Declare Queue URL
	queueURL := "http://localhost:9324/queue/my-queue"

	declareQueue(sqsClient)
	sendMessage(sqsClient, &queueURL, aws.String("message 1"))
	sendMessage(sqsClient, &queueURL, aws.String("message 2"))
	receiveMessages(sqsClient, queueURL)
}

func declareQueue(sqsClient *sqs.SQS) {
	// Create a queue
	queueName := "my-queue"
	createQueueInput := &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	}

	createResult, err := sqsClient.CreateQueue(createQueueInput)
	if err != nil {
		fmt.Println("Failed to create queue:", err)
		return
	}

	fmt.Println("Queue created:", *createResult.QueueUrl)
}

func sendMessage(sqsClient *sqs.SQS, queueURL *string, message *string) {
	// Send a message to the queue
	sendParams := &sqs.SendMessageInput{
		MessageBody: message,
		QueueUrl:    queueURL,
	}

	sendResult, err := sqsClient.SendMessage(sendParams)
	if err != nil {
		fmt.Println("Failed to send message:", err)
		return
	}

	fmt.Println("Message sent with ID:", *sendResult.MessageId)
}

func receiveMessages(sqsClient *sqs.SQS, queueURL string) {
	// Receive messages from the queue
	receiveParams := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: aws.Int64(10), // Specify the maximum number of messages to receive
		VisibilityTimeout:   aws.Int64(30), // Set the visibility timeout for received messages
		WaitTimeSeconds:     aws.Int64(20), // Wait time for receiving messages (long polling)
	}

	receiveResult, err := sqsClient.ReceiveMessage(receiveParams)
	if err != nil {
		fmt.Println("Failed to receive messages:", err)
		return
	}

	// Process received messages
	for _, message := range receiveResult.Messages {
		fmt.Println("Received message:", *message.Body)

		// Delete the message from the queue
		deleteParams := &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueURL),
			ReceiptHandle: message.ReceiptHandle,
		}

		_, err := sqsClient.DeleteMessage(deleteParams)
		if err != nil {
			fmt.Println("Failed to delete message:", err)
			return
		}
	}

	fmt.Println("Messages processed and deleted.")
}
