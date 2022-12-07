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
const fileUpload = require("express-fileupload");
const path = require('path');
app.use(fileUpload({
    useTempFiles : true,
    tempFileDir : '/tmp/'
}));

const {ExecutionsClient} = require('@google-cloud/workflows');
const client = new ExecutionsClient();

const {Storage} = require('@google-cloud/storage');
// Creates a client
const storage = new Storage();

const vision = require('@google-cloud/vision');

// Creates a client
const visionclient = new vision.ImageAnnotatorClient();

const GOOGLE_CLOUD_PROJECT = process.env.GOOGLE_CLOUD_PROJECT;
const WORKFLOW_REGION = process.env.WORKFLOW_REGION;
const WORKFLOW_NAME = process.env.WORKFLOW_NAME;
const GCS_REGION = process.env.GCS_REGION;
const BUCKET_NAME = process.env.BUCKET_NAME;
const LABELERURL = process.env.LABELERURL;
const WATERMARKERURL = process.env.WATERMARKERURL;
const RESIZEURL = process.env.RESIZEURL;

app.use(express.json());
app.get("/", (req, res) => {
    res.sendFile(path.join(__dirname, "/3-upload.html"));
  });
app.post('/upload', async (req, res) => {

  console.log('Request received:');
  delete req.headers.Authorization; // do not log authorization header
  console.log({headers: req.headers, body: req.body});
  let safe = false;
  try {
    const [result] = await visionclient.safeSearchDetection(req.files.foo.tempFilePath);
    const detections = result.safeSearchAnnotation;
    console.log('Safe search:');
    console.log(`Adult: ${detections.adult}`);
    console.log(`Medical: ${detections.medical}`);
    console.log(`Spoof: ${detections.spoof}`);
    console.log(`Violence: ${detections.violence}`);
    console.log(`Racy: ${detections.racy}`);
    safe = detections.adult !='VERY_LIKELY' //&& detections.Medical !='VERY_LIKELY'
    // && response.Racy < Likelihood.Possible
    // && response.Spoof < Likelihood.Possible
    // && response.Violence < Likelihood.Possible;
  } catch (e) {
    console.error(`Error checking file ${e}`);
    res.status(500).send(`Error checking file: ${e}`);
    throw e;
  }
  const fileName = Date.now() + req.files.foo.name;
  try {
      console.log(`Upload file to GCS: ${GOOGLE_CLOUD_PROJECT}, ${GCS_REGION}, ${BUCKET_NAME}` );
      const generationMatchPrecondition = 0
      const options = {
        destination: fileName,
        // Optional:
        // Set a generation-match precondition to avoid potential race conditions
        // and data corruptions. The request to upload is aborted if the object's
        // generation number does not match your precondition. For a destination
        // object that does not yet exist, set the ifGenerationMatch precondition to 0
        // If the destination object already exists in your bucket, set instead a
        // generation-match precondition using its generation number.
        preconditionOpts: {ifGenerationMatch: generationMatchPrecondition},
      };
  
      await storage.bucket(BUCKET_NAME).upload(req.files.foo.tempFilePath, options);
      console.log(`${fileName} uploaded to ${BUCKET_NAME}`);
  } catch (e) {
    console.error(`Error uploading file ${e}`);
    res.status(500).send(`Error uploading file: ${e}`);
    throw e;
  }

  try {
    // [START eventarc_workflows_execute]
    console.log(`Workflow path: ${GOOGLE_CLOUD_PROJECT}, ${WORKFLOW_REGION}, ${WORKFLOW_NAME}`);
    console.log(JSON.stringify({bucket: BUCKET_NAME, file: fileName, LABELER_URL: LABELERURL, WATERMARKER_URL: WATERMARKERURL, RESIZE_URL: RESIZEURL, safe: safe})    )
    const execResponse = await client.createExecution({
      parent: client.workflowPath(GOOGLE_CLOUD_PROJECT, WORKFLOW_REGION, WORKFLOW_NAME),
      execution: {
        argument: JSON.stringify({bucket: BUCKET_NAME, file: fileName, LABELER_URL: LABELERURL, WATERMARKER_URL: WATERMARKERURL, RESIZE_URL: RESIZEURL, safe: safe})
      }
    });
    console.log(`Execution response: ${JSON.stringify(execResponse)}`);

    const execName = execResponse[0].name;
    console.log(`Created execution: ${execName}`);

    res.status(200).send(`Created execution: ${execName}`);
    // [END eventarc_workflows_execute]
  } catch (e) {
    console.error(`Error executing workflow: ${e}`);
    res.status(500).send(`Error executing workflow: ${e}`);
    throw e;
  }
  res.send("File uploaded!");
});

module.exports = app;
