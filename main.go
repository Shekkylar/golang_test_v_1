package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// Request structure for incoming requests
type Request map[string]interface{}

// Converted structure for outgoing requests
type ConvertedRequest map[string]interface{}

func main() {
	// Create a buffered channel with the same capacity as the number of workers
	ch := make(chan ConvertedRequest, 5)

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Start the workers
	for i := 0; i < 5; i++ { // Number of worker goroutines
		wg.Add(1)
		go worker(ch, &wg)
	}

	// Start the HTTP server using gin
	router := gin.Default()

	router.POST("/submit", func(c *gin.Context) {
		var req Request

		if err := c.BindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request format"})
			return
		}

		convertedReq := convertJSON(req)
		ch <- convertedReq
		c.JSON(http.StatusOK, gin.H{"converted_request": convertedReq})
	})

	// Close the channel when all requests are processed
	go func() {
		wg.Wait()
		close(ch)
	}()

	router.Run(":8080")
}

func worker(ch chan ConvertedRequest, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case req := <-ch:
			// Send the converted request to the external webhook
			sendToWebhook(req)
		}
	}
}

func convertJSON(inputMap map[string]interface{}) map[string]interface{} {
	outputMap := map[string]interface{}{
		"event":            inputMap["ev"],
		"event_type":       inputMap["et"],
		"app_id":           inputMap["id"],
		"user_id":          inputMap["uid"],
		"message_id":       inputMap["mid"],
		"page_title":       inputMap["t"],
		"page_url":         inputMap["p"],
		"browser_language": inputMap["l"],
		"screen_size":      inputMap["sc"],
		"attributes":       make(map[string]interface{}),
		"traits":           make(map[string]interface{}),
	}

	extractTraitsAndAttributes(inputMap, outputMap, "attributes", "atrk", "atrv", "atrt")
	extractTraitsAndAttributes(inputMap, outputMap, "traits", "uatrk", "uatrv", "uatrt")

	return outputMap
}

func extractTraitsAndAttributes(inputMap map[string]interface{}, outputMap map[string]interface{}, outputMapKey, keyPrefix, valuePrefix, typePrefix string) {
	for i := 1; ; i++ {
		key := fmt.Sprintf("%s%d", keyPrefix, i)
		if val, ok := inputMap[key]; ok {
			attributeKey := val.(string)
			attributeValueKey := fmt.Sprintf("%s%d", valuePrefix, i)
			attributeTypeKey := fmt.Sprintf("%s%d", typePrefix, i)

			outputMap[outputMapKey].(map[string]interface{})[attributeKey] = map[string]interface{}{
				"value": inputMap[attributeValueKey],
				"type":  inputMap[attributeTypeKey],
			}
		} else {
			break
		}
	}
}

func sendToWebhook(req ConvertedRequest) {
	webhookURL := "https://webhook.site/fde74d38-1e43-4550-833a-0b65c2662a9a"

	jsonReq, err := json.Marshal(req)
	if err != nil {
		fmt.Println("Error marshalling converted request:", err)
		return
	}

	resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(jsonReq))
	if err != nil {
		fmt.Println("Error sending request to webhook:", err)
		return
	}
	defer resp.Body.Close()

	fmt.Println("Request sent to webhook. Response Status:", resp)
	time.Sleep(time.Second)
}
