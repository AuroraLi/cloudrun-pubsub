// Copyright 2021 Google, LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const express = require("express");
const app = express();

const path = require('path');
var ffmpeg = require('fluent-ffmpeg');
var fs = require('fs');

const imageTopic = process.env.IMAGE_TOPIC || "processedframes";
const outputBucket = process.env.OUTPUT_BUCKET || "";
const project_id = process.env.PROJECT_ID || "";


const {Storage} = require('@google-cloud/storage');
// Creates a client
const storage = new Storage({projectId: project_id});
// Imports the Google Cloud client library
const {PubSub,v1} = require('@google-cloud/pubsub');
// Creates a client; cache this for further use
const pubSubClient = new PubSub({project_id});

// Creates a client; cache this for further use.
const subClient = new v1.SubscriberClient();

async function createSubscriptionWithFilter(subscriptionNameOrId,filterString, frameNumber,videoName) {

    const callback = function(err, subscription, apiResponse) {
        if (err){
            console.log("cannot create subscrpition: " + err)
            throw err;
        }
        console.log("here is the new subscription")
        console.log(subscription)
        pubSubClient.getSubscriptions()
        listenForMessages(subscriptionNameOrId,frameNumber,{})

        
    };
    // Creates a new subscription
    pubSubClient
      .topic(imageTopic)
      .createSubscription(subscriptionNameOrId, {
        filter: filterString,
      }).then((data) => {
    //     const subscription = data[0];
    //     const apiResponse = data[1];
    //     console.log("the subscription is " + subscription);
    //     console.log("api response: "+ apiResponse);
    //     console.log(pubSubClient.getSubscriptions());
    //     console.log(`Created subscription ${subscriptionNameOrId} with filter ${filterString}.`)
           listenForMessages(subscriptionNameOrId,frameNumber,processVideo(videoName,subscriptionNameOrId))
     } );
    // listenForMessages(subscriptionNameOrId,frameNumber);
    // return pubSubClient.subscription(subscriptionNameOrId)
  }

async function deleteSubscription(subscriptionNameOrId) {
// Deletes the subscription
    await pubSubClient.subscription(subscriptionNameOrId).delete();
    console.log(`Subscription ${subscriptionNameOrId} deleted.`);
}

// function listenForMessages(subscriptionName,frameNumber) {
//     // References an existing subscription
//     console.log(subscriptionName)
//     let subscription = pubSubClient.subscription(subscriptionName)
//     // Create an event handler to handle messages
//     let messageCount = 0;
//     const messageHandler = message => {
//       console.log(`Received message ${message.id}:`);
//       console.log(`\tData: ${message.data}`);
//       console.log(`\tAttributes: ${message.attributes}`);
//       messageCount += 1;
     
//       fs.writeFile(`pic${message.attributes.seqNum}.png`,message.data,function (err){
//           if (err) return console.log(err);
//           console.log(`Saved frame number ${message.attributes.seqNum}`)
//       })
//       // "Ack" (acknowledge receipt of) the message
//       message.ack();
//     if (messageCount == frameNumber){
//         subscription.removeListener('message', messageHandler);
//         console.log(`${messageCount} message(s) received.`);
//         return
//     }
//     };
  
//     // Listen for new messages until timeout is hit
//     subscription.on('message', messageHandler);
  
//     setTimeout(() => {
//       subscription.removeListener('message', messageHandler);
//       console.log(`${messageCount} message(s) received. Timed out`);
//     }, 500000);
//     // return
//   }
  
async function listenForMessages(subscriptionNameOrId,frameNumber, callback) {
    // The low level API client requires a name only.
    const formattedSubscription =
      subscriptionNameOrId.indexOf('/') >= 0
        ? subscriptionNameOrId
        : subClient.subscriptionPath(project_id, subscriptionNameOrId);
    console.log(formattedSubscription)
    // The maximum number of messages returned for this request.
    // Pub/Sub may return fewer than the number specified.
    
    const request = {
      subscription: formattedSubscription,
      maxMessages: frameNumber,
    };
    // References an existing subscription
  
    // // The subscriber pulls a specified number of messages.
    const [response] = await subClient.pull(request);
  
    // // Process the messages.
    const ackIds = [];
    console.log(response);
    for (const message of response.receivedMessages) {
      console.log(`Received message: ${message.message.data}`);
      fs.writeFile(`pic${message.attributes.seqNum}.png`,message.data,function (err){
          if (err) return console.log(err);
          console.log(`Saved frame number ${message.attributes.seqNum}`)
      })
      ackIds.push(message.ackId);
    }
//     let messageCount = 0;
//     const messageHandler = message => {
//     messageCount += 1;
//     fs.writeFile(`pic${message.attributes.seqNum}.png`,message.data,function (err){
//               if (err) return console.log(err);
//               console.log(`Saved frame number ${message.attributes.seqNum}`)
//           })

//     // "Ack" (acknowledge receipt of) the message
//     message.ack();
//   };
//   // Listen for new messages until timeout is hit
//   subscription.on('message', messageHandler);

//   setTimeout(() => {
//     subscription.removeListener('message', messageHandler);
//     console.log(`${messageCount} message(s) received.`);
    //  callback();
//   }, 100 * 1000);

  
    if (ackIds.length !== 0) {
      // Acknowledge all of the messages. You could also acknowledge
      // these individually, but this is more efficient.
      const ackRequest = {
        subscription: formattedSubscription,
        ackIds: ackIds,
      };
  
      await subClient.acknowledge(ackRequest);
    }
  
    console.log('Done.');
    callback();
    
  }

async function getVideo(frameNumber, videoName, callback){
    var filter = `attributes.videoName = \"${videoName}\"`
    var subscriptionName = "test" + Date.now();
    let response = await createSubscriptionWithFilter(subscriptionName,filter, frameNumber,videoName)//.then(listenForMessages(subscriptionName,frameNumber,processVideo(videoName,subscriptionName)));
    console.log(response)
    // await listenForMessages(subscriptionName,frameNumber)

    callback();
}
function processVideo(videoName, subscriptionName){
    const ffmpeg_cmd = ffmpeg()
    //      .input(`${outputPath}/${md5}.mp4`)
          .input("pic%05d.png")
          .inputOptions(
              '-r', '1',
              '-f', 'image2')
          .native()
          .noAudio()
          .output(videoName)
          .outputOptions(
            '-r', '1'
          );
    
        ffmpeg_cmd.on('start', function() {
          console.log(`Started processing video`);
        });
    
        ffmpeg_cmd.on('error', function(err) {
          console.error(`error occured: ${err}`);
    
        //   throw new Error(`Unable to process ${videoName}: ${err}`);
        });
    
        ffmpeg_cmd.on('end', function() {
          console.log("Finished processing video");
          // post files to queue
        })
        try {
            ffmpeg_cmd.run();
            
        }
        catch (e) {
            console.log(e);
            // res.status(500).send();
        }
        deleteSubscription(subscriptionName)
}

async function uploadToBucket(filename){
    // Check file
    if (fs.existsSync(filename)){
        console.log(filename + "already created")
        await storage.bucket(outputBucket).upload(filename, {
        destination: "/",
        // Support for HTTP requests made with `Accept-Encoding: gzip`
        gzip: true,
        metadata: {
            // Enable long-lived HTTP caching headers
            // Use only if the contents of the file will never change
            // (If the contents will change, use cacheControl: 'no-cache')
            cacheControl: 'public, max-age=31536000',
        },
      });
    }
    else {console.log("Video is not available")}
   
}

app.use(express.json());
app.get("/", (req, res) => {
    res.sendFile(path.join(__dirname, "/3-upload.html"));
  });
app.post('/', async (req, res) => {
    try{
        console.log(req.body);
    const totalFrame = req.body.frames;
    const videoName = req.body.videoName;
    const upload = uploadToBucket(videoName)
    getVideo(totalFrame,videoName,uploadToBucket(videoName));
    // uploadToBucket(videoName);
    res.send('File uploaded!')
    }
    catch (e) {
        console.log(e)
    }
    // Use the mv() method to place the file somewhere on your server
    // sampleFile.mv(uploadPath, function(err) {
    //     if (err)
    //     return res.status(500).send(err);

    //     try {
    //         writeFrames(uploadPath, dir); //req.files.foo.tempFilePath+'/'+req.files.foo.name

    //         res.status(200).send();
    //     } catch (e) {
    //         console.log(e);
    //         res.status(500).send();
    //   }
        //res.send('File uploaded!');
    // });

    
});

module.exports = app;
