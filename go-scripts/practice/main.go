
package main
import "fmt"
// Implement a toy event processing system
// The Run() function is syncing messages from source to destination based on the topic in the message
// Mappings can redirect messages based on topic. They are applied from the outside at any point in time
// Example:
// A -> A
// B -> B
// AddMapping(A, C)
// AddMapping(B, D)
// A -> C
// B -> D
// RemoveMapping(A)
// A -> A
// B -> D
type SrcMessage struct {
    Topic string
    Key int32
    Value string
}

type DestMessage struct {
    Key int32
    Value string
}

type DestClient interface {
    Send(topic string, message DestMessage)
}

func Run(src <-chan SrcMessage, dest DestClient) {
	// Your code here
	for msg := range src {

		destTopic := msg.Topic
		if val, ok := mappings[msg.Topic]; ok {
			destTopic = val
		}

		dstMsg := DestMessage{
			Key: msg.Key,
			Value: msg.Value,
		}

		dest.Send(destTopic, dstMsg)
	}
    
}

var mappings = make(map[string]string, 0)

func AddMapping(fromTopic, toTopic string) {
    // Your code here
	mappings[fromTopic] = toTopic
}

func RemoveMapping(fromTopic string) {
    // Your code here
	delete(mappings, fromTopic)
}

func main() {
  fmt.Println("Started")
  AddMapping("A", "B")
  AddMapping("C", "D")

  RemoveMapping("C")

}
