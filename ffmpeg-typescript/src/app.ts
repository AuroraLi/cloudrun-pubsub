import express from "express";
import bodyParser from "body-parser";
import { MD5 } from "crypto-js";
// import { v1, PubSub, Subscription } from "@google-cloud/pubsub";
import {PubSub, PublishOptions, Attributes} from '@google-cloud/pubsub';
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


const storage = new Storage();
// const pubsub = new v1.PublisherClient();
const pubSub = new PubSub();
app.use(bodyParser.json());

async function writeFrame(inputFile: string, seqNum: number, data: Buffer, videoName: string) { //: Promise<string>
//   const topic = pubsub.projectTopicPath(projectId, topicName);
//   console.log(topic);
//   console.log(pubsub.initialize());
  const options: PublishOptions = {
    flowControlOptions: {
      maxOutstandingMessages: 50,
      maxOutstandingBytes: 10 * 1024 * 1024, // 10 MB
    },
  };
  const topic = pubSub.topic(topicName,options);
  const flow = topic.flowControlled();
  console.log(`Writing frame ${seqNum}, ${data.byteLength} bytes to ${projectId}, ${topicName}`);

  const customAttributes: Attributes = {
    seqNum: seqNum.toString(),
    name: videoName
  };
  const messageData = {
    data: data,
    attributes: customAttributes
  };

  
  const retrySettings = {
    retryCodes: [
      10, // 'ABORTED'
      1, // 'CANCELLED',
      4, // 'DEADLINE_EXCEEDED'
      13, // 'INTERNAL'
      8, // 'RESOURCE_EXHAUSTED'
      14, // 'UNAVAILABLE'
      2, // 'UNKNOWN'
    ],
    backoffSettings: {
      // The initial delay time, in milliseconds, between the completion
      // of the first failed request and the initiation of the first retrying request.
      initialRetryDelayMillis: 100,
      // The multiplier by which to increase the delay time between the completion
      // of failed requests, and the initiation of the subsequent retrying request.
      retryDelayMultiplier: 1.3,
      // The maximum delay time, in milliseconds, between requests.
      // When this value is reached, retryDelayMultiplier will no longer be used to increase delay time.
      maxRetryDelayMillis: 60000,
      // The initial timeout parameter to the request.
      initialRpcTimeoutMillis: 5000,
      // The multiplier by which to increase the timeout parameter between failed requests.
      rpcTimeoutMultiplier: 1.0,
      // The maximum timeout parameter, in milliseconds, for a request. When this value is reached,
      // rpcTimeoutMultiplier will no longer be used to increase the timeout.
      maxRpcTimeoutMillis: 600000,
      // The total time, in milliseconds, starting from when the initial request is sent,
      // after which an error will be returned, regardless of the retrying attempts made meanwhile.
      totalTimeoutMillis: 600000,
    },
  };

//   console.log(`Publishing frame ${seqNum}`);
//   const messageId = await pubSub.topic(topicName)
//     .publish(data, customAttributes);
//   console.log(`Message ${messageId} published.`);
// }
//    console.log(request.topic);
//   console.log(pubsub.publish(request, { retry: retrySettings,}));
  
//   return pubsub.publish(request, {
//     retry: retrySettings,
//   }).then((res) => {
//     console.log(`Message ${res[0].messageIds} published.`);
//     if (res[0] && res[0].messageIds) {
//       return res[0].messageIds[0] || "";
//     }

//     return "";
//   }).catch((err) => {
//     console.error(`Could not publish message: ${err}`);
//     throw new Error(`Could not publish message: ${err}`);
//   });
    const wait = flow.publish(messageData);
    // const wait = topic.publish(data,customAttributes)
    if (wait) {
      await wait;
    }
    //  const messageIds = await flow.all();
     console.log(`Published ${seqNum} with flow control settings.`);

} 
// generated screenshots:
// ./screenshot-1.jpg
// ./screenshot-2.jpg
// ./screenshot-3.jpg
// default behavior is to extract all frames
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
  const myBucket = storage.bucket(rexpResult[1]);
  const file = myBucket.file(rexpResult[2]);
  console.log(`bucket: ${rexpResult[1]}, path: ${rexpResult[2]}`);
  const filestream = file.createReadStream();

  // md5 hash of the full path
  const md5 = MD5(inputFile);
  const tmpdir = fs.mkdtempSync(outputPath)
  console.log(`saving to ${tmpdir}/`)
  const frameRe = /.*frame-(?<seqNum>[0-9]+).jpg/;

  // start watching the directory -- does this scale?
  const fswatcher = chokidar.watch(tmpdir, {
    awaitWriteFinish: true,
  });

  fswatcher.on('ready', () => {
    console.log(`watching tmp directory: ${tmpdir}`);
  });

  fswatcher.on('add', (path) => {
    // new file was added
    const frameRes = frameRe.exec(path as string);

    if (frameRes == null) {
      console.error(`unknown file ${path}`);
      return;
    }

    console.log(`new file added: ${path}`);

    fs.readFile(`${path}`, (err, data) => {
      if (err) {
        throw new Error(`Cannot read file ${path}: ${err}`);
      }

      writeFrame(inputFile, parseInt(frameRes[1]), data, videoName)
        .then((value) => {
            console.log("The frame value is" + value)
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
        '-vf', 'fps=1/1'
      );

    ffmpeg_cmd.on('start', function() {
      console.log(`Started processing video`);
    });

    ffmpeg_cmd.on('error', function(err) {
      console.error(`error occured: ${err}`);

    //   fs.unlinkSync(`${outputPath}/${md5}.mp4`);
      throw new Error(`Unable to process ${inputFile}: ${err}`);
    });

    ffmpeg_cmd.on('end', function(stdout, stderr) {
      console.log("Finished processing video");
      console.log(stdout +"is the ffmpeg output")
      const response = fetch(stitchEndpoint,{
          method: "POST",
          body: JSON.stringify({
              frame: 10,
              videoName: videoName
          }),
          headers: {
            'Content-Type': 'application/json',
            Accept: 'application/json',
          }
      })
      console.log(videoName +"is the filter videoname of stitcher")
      // post files to queue
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

app.get("/", (req, res) => {
    res.sendFile(path.join(__dirname, "/3-upload.html"));
  });

app.post("/", (req, res) => {
    const receivedEvent = HTTP.toEvent({ headers: req.headers, body: req.body });
    // console.log(receivedEvent);
    // console.log(req.headers);
    // console.log(req.body);
  try {
      let video = req.header('ce-resourcename');
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