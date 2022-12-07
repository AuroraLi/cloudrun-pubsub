// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// [START cloudrun_pubsub_server]
// [START run_pubsub_server]

// Sample run-pubsub is a Cloud Run service which handles Pub/Sub messages.
package main

import (
	"context"
	"encoding/json"
	// "errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	// "strings"
	"time"

	// "encoding/base64"
	"bufio"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/TheZoraiz/ascii-image-converter/aic_package"
)

func main() {

	http.HandleFunc("/", HelloPubSub)
	http.HandleFunc("/test", serveFile)
	// Determine port for HTTP service.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("Defaulting to port %s", port)
	}
	// Start HTTP server.
	log.Printf("this is rev 1")
	log.Printf("Listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}

// [END run_pubsub_server]
// [END cloudrun_pubsub_server]

// [START cloudrun_pubsub_handler]
// [START run_pubsub_handler]

// PubSubMessage is the payload of a Pub/Sub event.
// See the documentation for more details:
// https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
type PubSubMessage struct {
	Message struct {
		Data       []byte            `json:"data,omitempty"`
		ID         string            `json:"messageId"`
		Attributes map[string]string `json:"attributes"`
	} `json:"message"`
	Subscription string `json:"subscription"`
}
type MessageAttributes struct {
	SeqNum    string `json:"seqNum"`
	VideoName string `json:"name"`
}

type ClientUploader struct {
	cl         *storage.Client
	projectID  string
	bucketName string
	uploadPath string
}

var uploader *ClientUploader
var pubSubTopic string
var projectId string

func init() {
	client, err := storage.NewClient(context.Background())
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	uploader = &ClientUploader{
		cl:         client,
		uploadPath: "test-files/",
	}
	pubSubTopic = os.Getenv("PROCESSED_PUBSUB")
	if pubSubTopic == "" {
		pubSubTopic = "processedframes"
	}

	projectId = os.Getenv("PROJECT_ID")
	if projectId == "" {
		projectId = ""
	}

}

func serveFile(w http.ResponseWriter, r *http.Request) {
	fmt.Println(r.URL.Path)
	p := "." + r.URL.Path
	if p == "./test" {
		p = "./static/index.html"
	}
	http.ServeFile(w, r, p)
}

// HelloPubSub receives and processes a Pub/Sub push message.
func HelloPubSub(w http.ResponseWriter, r *http.Request) {
	var m PubSubMessage
	body, err := ioutil.ReadAll(r.Body)
	// log.Printf(string(body))
	if err != nil {
		log.Printf("ioutil.ReadAll: %v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	// byte slice unmarshalling handles base64 decoding.
	if err := json.Unmarshal(body, &m); err != nil {
		log.Printf("json.Unmarshal: %v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	fmt.Printf("request subscription: " + m.Subscription)
	fmt.Printf("request attributes: " + fmt.Sprint(m.Message.Attributes))
	// log.Printf(m)
	log.Printf(m.Message.ID + "is message id")
	log.Printf(m.Message.Attributes["seqNum"] + "is the sequence number")
	log.Printf(m.Message.Attributes["name"] + "is the video name")
	image_data := string(m.Message.Data)
	// log.Printf(image_data)
	if image_data == "" {
		log.Printf("no image data")
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	// decoded_image_data, err := base64.StdEncoding.DecodeString(image_data)
	// if err != nil {
	//     panic(err)
	// }
	// fmt.Printf("Decoded text: %s\n", image_data)

	var filename = m.Message.ID + ".jpeg"
	// var filename = "temp.jpeg"
	// log.Printf(filename)
	writeToFile(filename, string(image_data))
	convertImage(filename)

	// blobFile, err := os.Open(m.Message.ID+"-ascii-art.png")
	// err = uploader.UploadFile(blobFile, m.Message.ID+"-ascii-art.png")

	err = publishProcessedImage(m.Message.ID+"-ascii-art.png", m.Message.Attributes["seqNum"], m.Message.Attributes["name"])
	// files, err := ioutil.ReadDir(".")
	// fmt.Println("the files are:")
	if err != nil {
		log.Fatal(err)
	}

	// for _, file := range files {
	//     fmt.Println(file.Name(), file.IsDir())
	// }

}

// [END run_pubsub_handler]
// [END cloudrun_pubsub_handler]

func writeToFile(filename string, data string) {
	fo, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	// close fo on exit and check for its returned error
	defer func() {
		if err := fo.Close(); err != nil {
			panic(err)
		}
	}()
	// make a write buffer
	w := bufio.NewWriter(fo)
	if _, err := w.Write([]byte(data)); err != nil {
		panic(err)
	}
	if err = w.Flush(); err != nil {
		panic(err)
	}
}

func convertImage(file string) {
	fmt.Println("filename: " + file)
	flags := aic_package.DefaultFlags()
	flags.SaveImagePath = "."
	// flags.OnlySave = true
	flags.Dimensions = []int{50, 25}
	asciiArt, err := aic_package.Convert(file, flags)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("the converted file is %v\n", asciiArt)

}
func (c *ClientUploader) UploadFile(file multipart.File, object string) error {
	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	// Upload an object with storage.Writer.
	wc := c.cl.Bucket(c.bucketName).Object(c.uploadPath + object).NewWriter(ctx)
	if _, err := io.Copy(wc, file); err != nil {
		return fmt.Errorf("io.Copy: %v", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("Writer.Close: %v", err)
	}

	return nil
}

func publishProcessedImage(filename string, seqNum string, videoName string) error {
	// projectID := "my-project-id"
	// topicID := "my-topic"
	log.Printf("Hey new version")
	log.Printf("trying to publish to " + projectId + " at topic" + pubSubTopic)
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	fileInfo, _ := file.Stat()
	var size int64 = fileInfo.Size()
	log.Printf("the file has %v bytes", size)
	fmt.Printf("the file has %v bytes", size)
	bytes := make([]byte, size)
	// read file into bytes
	buffer := bufio.NewReader(file)
	_, err = buffer.Read(bytes)
	if err != nil {
		log.Printf("error reading file:" + err.Error())
		fmt.Printf("error reading file:" + err.Error())
	}
	//
	// log.Printf("the file bytes are:"+string(bytes))
	log.Printf("the seq are:" + seqNum)
	log.Printf("the video is:" + videoName)

	t := client.Topic(pubSubTopic)
	result := t.Publish(ctx, &pubsub.Message{
		Data: bytes,
		Attributes: map[string]string{
			"seqNum":    seqNum,
			"videoName": videoName,
		},
	})
	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	id, err := result.Get(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Published message with custom attributes to topic %s; msg ID: %v\n", pubSubTopic, id)
	return nil

}
