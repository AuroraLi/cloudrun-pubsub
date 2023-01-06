import express from "express";
import bodyParser from "body-parser";
import { MD5 } from "crypto-js";
// import { v1, PubSub, Subscription } from "@google-cloud/pubsub";
import {PubSub, PublishOptions, Attributes, Topic} from '@google-cloud/pubsub';
import { Storage } from "@google-cloud/storage";
import * as fs from "fs";
import * as chokidar from "chokidar";
import path from "path";
import { CloudEvent, HTTP } from "cloudevents";
import fetch from 'node-fetch';

import ffmpeg from 'fluent-ffmpeg';

const app = express();
const port = process.env.PORT || 8080;
const projectId = process.env.PROJECT_ID || "";
const topicName = process.env.PUBSUB_TOPIC || "inputframes";
const outputDir = process.env.OUTPUT_DIR || "/tmp";
const stitchEndpoint = process.env.STITCH_ENDPOINT || "";
const processedTopic = process.env.PROCESSED_TOPIC || "processedframes"
const framePS = process.env.FRAME_PER_SEC || "4"
const ffmpegWaitMs = process.env.FFMPEG_WAIT_MS || "20000"

const storage = new Storage();
const pubSub = new PubSub();
app.use(bodyParser.json());

async function writeFrame(seqNum: number, data: Buffer, videoName: string) { //: Promise<string>
//   const topic = pubsub.projectTopicPath(projectId, topicName);
//   console.log(topic);
//   console.log(pubsub.initialize());
  console.log(`Writing frame ${seqNum}, ${data.byteLength} bytes to ${projectId}, ${topicName}`);

  const customAttributes: Attributes = {
    seqNum: seqNum.toString(),
    name: videoName
  };

  console.log(`Publishing frame ${seqNum}`);
  const messageId = await pubSub.topic(topicName).publish(data, customAttributes, function(err){
      if (err) {
          console.log(err)
        };
  });
console.log(`Message ${messageId} published.`);


    // const wait = flow.publish(messageData);
    // // const wait = topic.publish(data,customAttributes)
    // if (wait) {
    //   await wait;
    // }
    // //  const messageIds = await flow.all();
    //  console.log(`Published ${seqNum} with flow control settings.`);

} 
async function writeFrames(inputFile: string, outputPath: string) {
  // gcs bucket URLs look like: projects/_/buckets/<bucketName/objects/objectPath
//   console.log(inputFile)
  const re = /projects\/_\/buckets\/(?<bucketName>[^/]+)\/objects\/(?<objectPath>.*)/;
  const rexpResult = inputFile.match(re);
  if (rexpResult === null) {
    throw new Error(`Unable to parse inputFile: ${inputFile}`);
  }

  if (rexpResult.length !== 3) {
    throw new Error(`Unable to parse inputFile: ${inputFile}`);
  }
  if (!rexpResult[2].endsWith(".mp4")){
      console.log("this file is not a video, skip");
      return
  }


  var videoName = rexpResult[2].split(".")[0]
//   await pubSub.createTopic(newSubscriptionName);
//   console.log(`Topic ${newSubscriptionName} created.`);
  var subscriptionName = createProcessedSubscription(videoName)
  const myBucket = storage.bucket(rexpResult[1]);
  const file = myBucket.file(rexpResult[2]);
  console.log(`bucket: ${rexpResult[1]}, path: ${rexpResult[2]}`);
  const filestream = file.createReadStream();

  // md5 hash of the full path
  const md5 = MD5(inputFile);
  const tmpdir = fs.mkdtempSync(outputPath)
  console.log(`saving to ${tmpdir}/`)
  const frameRe = /.*frame-(?<seqNum>[0-9]+).jpg/;
  let totalFrame = 0;

  // start watching the directory -- does this scale?
  const fswatcher = chokidar.watch(tmpdir, {
    awaitWriteFinish: true,
  });
  //create pub sub publisher
//   const options: PublishOptions = {
//     flowControlOptions: {
//       maxOutstandingMessages: 50,
//       maxOutstandingBytes: 10 * 1024 * 1024, // 10 MB
//     },
//   };
//   const topic = pubSub.topic(topicName,options);
//   const flow = topic.flowControlled();
  

  fswatcher.on('ready', () => {
    console.log(`watching tmp directory: ${tmpdir}`);
  });

  fswatcher.on('add', (path) => {
    console.log(`new file added: ${path}`);
    totalFrame++;
    
    // new file was added
    const frameRes = frameRe.exec(path as string);

    if (frameRes == null) {
      console.error(`unknown file ${path}`);
      return;
    }

    console.log(`frame number ${totalFrame}`)
    fs.readFile(`${path}`, (err, data) => {
      if (err) {
        throw new Error(`Cannot read file ${path}: ${err}`);
      }

      writeFrame(parseInt(frameRes[1]), data, videoName)
        .then((value) => {
          fs.unlinkSync(path);
        })
        .catch((error) => {
          console.error(`Unable to write frame ${frameRes[1]}: ${error}`);
        });
    });
  });

  fswatcher.on('error', (error) => {
    console.error(`error reading directory ${tmpdir}: ${error}`);
  });

  /*
  file.download({
    destination: `${outputPath}/${md5}.mp4`,
  })
  .then((resp) => {
    */
    const ffmpeg_cmd = ffmpeg()
//      .input(`${outputPath}/${md5}.mp4`)
      .input(filestream)
      .native()
      .noAudio()
      .output(`${tmpdir}/frame-%07d.jpg`)
      .outputOptions(
        '-q:v', '8',
        '-vf', `fps=${framePS}/1`
      );

    ffmpeg_cmd.on('start', function() {
      console.log(`Started processing video`);
    });

    ffmpeg_cmd.on('error', function(err) {
      console.error(`error occured: ${err}`);

    //   fs.unlinkSync(`${outputPath}/${md5}.mp4`);
      throw new Error(`Unable to process ${inputFile}: ${err}`);
    });

    ffmpeg_cmd.on('end', async function() {
      console.log("Finished processing video");
      await new Promise(resolve => setTimeout(resolve, parseInt(ffmpegWaitMs)));
        // call stitch for the video
        const response = fetch(stitchEndpoint,{
          method: "POST",
          body: JSON.stringify({
              frame: totalFrame,
              videoName: videoName,
              subscriptionName: subscriptionName
          }),
          headers: {
            'Content-Type': 'application/json',
            Accept: 'application/json',
          }
      });

      console.log(videoName +"is the filter videoname of stitcher")
      
    })

    ffmpeg_cmd.run();
/*
  })
  .catch((err: Error) => {
    if (err) {
      console.log("error: " + err);
      throw new Error(`Could not download ${inputFile}: ${err}`);
    }
  });
*/

}

function createProcessedSubscription(videoName: string){
    
    var subscriptionName = videoName.substr(0,4) + Date.now()
    var filterString = `attributes.videoName="${videoName}"`
    // Creates a new subscription
   pubSub
  .topic(processedTopic)
  .createSubscription(subscriptionName, {
    filter: filterString,
  });
console.log(
  `Created subscription ${subscriptionName} with filter ${filterString}.`
);
    return subscriptionName;
}

app.get("/", (req, res) => {
    res.sendFile(path.join(__dirname, "/3-upload.html"));
  });

app.post("/", (req, res) => {
    const receivedEvent = HTTP.toEvent({ headers: req.headers, body: req.body });
    // console.log(receivedEvent);
    // console.log(req.headers);
    // console.log(req.body);
  try {
      var video= req.header('ce-resourcename');
    //   console.log(video);
    //   console.log(pubsub.initialize());
        writeFrames(video as string, `${outputDir}/`);
        res.status(200).send();

  } catch (e) {
    console.log(e);
    res.status(500).send();
  }
});

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`) 
})