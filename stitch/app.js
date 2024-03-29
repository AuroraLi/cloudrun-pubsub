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

function pad(num, size) {
    num = num.toString();
    while (num.length < size) num = "0" + num;
    return num;
}


async function deleteSubscription(subscriptionNameOrId) {
// Deletes the subscription
    await pubSubClient.subscription(subscriptionNameOrId).delete();
    console.log(`Subscription ${subscriptionNameOrId} deleted.`);
}


async function listenForMessages(subscriptionNameOrId,frameNumber,videoName, callback) {
    // The low level API client requires a name only.
    const formattedSubscription =
      subscriptionNameOrId.indexOf('/') >= 0
        ? subscriptionNameOrId
        : subClient.subscriptionPath(project_id, subscriptionNameOrId);
    console.log(`looking for ${frameNumber} frames`)
    // The maximum number of messages returned for this request.
    // Pub/Sub may return fewer than the number specified.
    const subscription = pubSubClient.subscription(formattedSubscription);

    var dir = `./${videoName}`;
    var frameArray = new Array(frameNumber).fill(0);

    // for (var i = 1; i <= frameNumber; i++) {
    //     frameArray.push(i);
    // }
    counter = 0
    
    const messageHandler = message => {
        imageName = pad(message.attributes.seqNum,3)
        
        if (!fs.existsSync(dir)){ fs.mkdirSync(dir); }
        messageFrameNumber = parseInt(message.attributes.seqNum)-1
        // var index = frameArray.indexOf(messageFrameNumber);
        // if (index !== -1) {
        if (frameArray[messageFrameNumber]==0){
        // if (messageFrameNumber in frameArray) {
            // console.log(`processing ${messageFrameNumber} now because it is in the array`)
            fs.writeFile(`${dir}/pic${imageName}.png`,message.data,function (err){
              if (err) {
                  console.log(`unable to save frame ${message.attributes.seqNum} due to error`);
                  console.log(err); 
                  return};
              console.log(`Saved frame number ${message.attributes.seqNum}`)
              frameArray[messageFrameNumber]=1;
              message.ack();
              counter ++;
              console.log(`got ${counter} messages`)
              if (frameArray.indexOf(0)==-1 || counter == frameNumber) {
                  console.log(`All ${frameNumber} frames are received`);
                  console.log(`array is 0 at ${frameArray.indexOf(0)} for some reason`);
                  subscription.removeListener('message', messageHandler);
                  if(callback) callback(subscriptionNameOrId, videoName); 
                  return;
              }
            //   else {console.log(`missing ${frameArray.length} frames`);}
            })
        }
        else {
            console.log(`skipping this ${message.attributes.seqNum} because it is not in the frame array`);
            fs.readdir(`./${videoName}/`, (err, files) => {
                files.forEach(file => {
                //   console.log(file);
                  if (file==`pic${imageName}.png`){
                      console.log(`see, ${file} is saved already`)
                  }
                });
              });
            message.ack();
        };
      };
  // Listen for new messages until timeout is hit
  subscription.on('message', messageHandler);
  
  setTimeout(() => {
    // subscription.on('message', messageHandler);
    // if(callback) callback(subscriptionNameOrId, videoName); 
    //  callback();
    console.log('Done?');
    if (frameArray.indexOf(0)!=-1 && counter < frameNumber){
        console.log(`still waiting for ${frameArray.length} messages`);
        subscription.open();
    }
  }, 600 * 1000);


    // const request = {
    //   subscription: formattedSubscription,
    //   maxMessages: MAX_DIGEST,
    // };
    
    // // The subscriber pulls messages.
    // const [response] = await subClient.pull(request);
  
    // // // Process the messages.
    // const ackIds = [];
    // var dir = `./${videoName}`;
    // // console.log(response);
    // for (const message of response.receivedMessages) {
        
    //     if (!fs.existsSync(dir)){
    //         fs.mkdirSync(dir);
    //     }
    // //   console.log(`Received message: ${JSON.stringify(message)}`);
    //   imageName = pad(message.message.attributes.seqNum,3)
    //   fs.writeFile(`${dir}/pic${imageName}.png`,message.message.data,function (err){
    //       if (err) return console.log(err); })
    //   ackIds.push(message.ackId);
    // }  
    // if (ackIds.length !== 0) {
    //   // Acknowledge all of the messages. You could also acknowledge
    //   // these individually, but this is more efficient.
    //   const ackRequest = {
    //     subscription: formattedSubscription,
    //     ackIds: ackIds,
    //   };
  
    //   await subClient.acknowledge(ackRequest);
    // }
  
    // if(callback) callback(subscriptionNameOrId, videoName); 
    
  }

async function processVideo(videoName,subscriptionName){
    fs.readdir(`./${videoName}/`, (err, files) => {
        files.forEach(file => {
          console.log(file);
        });
      });
      console.log(videoName)
    const ffmpeg_cmd = ffmpeg()
    //      .input(`${outputPath}/${md5}.mp4`)
          .input(`./${videoName}/pic%03d.png`)
          .inputOptions(
            //   '-r', '1',
              '-f', 'image2')
          .native()
          .noAudio()
          .output(`${videoName}.mp4`)
        //   .outputOptions('-r', '1')
            ;
    
        ffmpeg_cmd.on('start', function() {
          console.log(`Started processing video`);
        });
    
        ffmpeg_cmd.on('error', function(err) {
          console.error(`error occured: ${err}`);
    
        //   throw new Error(`Unable to process ${videoName}: ${err}`);
        });
    
        ffmpeg_cmd.on('end', function(stdout, stderr) {
            console.log(stderr)
          console.log("Finished processing video");
          // post files to queue
          uploadToBucket(videoName)
          deleteSubscription(subscriptionName)
        })
        try {
            ffmpeg_cmd.run();
            
        }
        catch (e) {
            console.log(e);
            // res.status(500).send();
        }
        // deleteSubscription(subscriptionName)
}

async function uploadToBucket(filename){
    // Check file
    console.log(filename)
    file = `${filename}.mp4`
    if (fs.existsSync(file)){
        console.log(file + "already created")
        await storage.bucket(outputBucket).upload(file, {
        destination: file,
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
    else {
        console.log("Video is not available");
        // await new Promise(r => setTimeout(r, 2000));

    }
   
}

app.use(express.json());
app.get("/", (req, res) => {
    res.sendFile(path.join(__dirname, "/3-upload.html"));
  });
app.post('/', async (req, res) => {
    try{
        console.log(req.body);
    console.log(`total frame is ${req.body.frame}, type ${typeof req.body.frame}`)
    const totalFrame  = parseInt(req.body.frame);
    console.log(`total frame is ${totalFrame}, type ${typeof totalFrame}`)
    const videoName = req.body.videoName;
    const subscriptionName = req.body.subscriptionName;
    // await listenForMessages(subscriptionName,totalFrame,processVideo(videoName,subscriptionName));
    await listenForMessages(subscriptionName,totalFrame,videoName, function (subscriptionName,videoName){
        processVideo(videoName, subscriptionName)
        
    })
    // getVideo(totalFrame,videoName,console.log("test"));
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
