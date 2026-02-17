/**
 *  Script execution command sample:
 *  Read-Mode:- node migrations/updateProjectDocuments/updateUserRoleInformation.js --update=false --fromDate=2025-09-01 --toDate=2025-09-30
 *  Write-Mode:- node migrations/updateProjectDocuments/updateUserRoleInformation.js --update=true --fromDate=2025-09-01 --toDate=2025-09-30
 * 
 */

const path = require("path");
const fs = require("fs");
require("dotenv").config({ path: path.join(__dirname, "../../") + "/.env" });
const { MongoClient, ObjectId } = require("mongodb");
const UTILS = require("../../generics/helpers/utils");
const request = require("request");
const _ = require("lodash");

const MONGO_URI = process.env.MONGODB_URL;
const COLLECTION = "projects";
const BATCH_SIZE = 100;
const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
const outputFilePath = path.join(__dirname, `project-update-log.json-${timestamp}.json`);

let doUpdate = false;
const doUpdateArg = process.argv.find(arg => arg.startsWith('--update='));
const fromDateArg = process.argv.find(arg => arg.startsWith('--fromDate='));
const toDateArg = process.argv.find(arg => arg.startsWith('--toDate='));

if (!fromDateArg || !toDateArg) {
    console.error('Error: Both --fromDate and --toDate must be provided.');
    console.error('Usage: node script.js --fromDate=2025-09-01 --toDate=2025-10-01');
    process.exit(1);
}

doUpdate = doUpdateArg ? doUpdateArg.split('=')[1] : null;
doUpdate = doUpdate == "true" ? true : false;
let dateRange;
const fromDateValue = fromDateArg.split('=')[1];
const fromDateISO = `${fromDateValue}T00:00:00.000Z`;
const fromDate = new Date(fromDateISO);

const toDateValue = toDateArg.split('=')[1];
const toDateISO = `${toDateValue}T00:00:00.000Z`;
let toDate = new Date(toDateISO);
toDate.setUTCDate(toDate.getUTCDate() + 1);

dateRange = `${fromDateISO} to ${toDateISO}`;

let executionMode = "READ-MODE"
if(doUpdate) executionMode = "WRITE-MODE";

function fetchUdiseCode(project){
    const uuid = project.userRoleInformation.school;
    // if school udise code is found in userProfile.userLocations, use it to update the DB 
    if(project.userProfile && project.userProfile.userLocations && Array.isArray(project.userProfile.userLocations) && project.userProfile.userLocations.length > 0){
        const locations = project.userProfile.userLocations;
        const schoolObj = locations.find(loc => (loc.type === "school" && loc.id === uuid));
        if(schoolObj && schoolObj.code) return schoolObj.code;
    }
    return null;
}

const locationSearch = function (neededUdiseCodes) {
  return new Promise(async (resolve, reject) => {
      try {

        let bodyData={};
        bodyData["request"] = {};
        bodyData["request"]["filters"] = {
            "id" : neededUdiseCodes.map(obj => obj.uuid)
        }
        const url = `${process.env.USER_SERVICE_URL}/v1/location/search`;
        const options = {
            headers : {
                "content-type": "application/json"
            },
            json : bodyData
        };

        let result = {
            success : true
        };

        request.post(url,options,requestCallback);        

        function requestCallback(err, data) {   
            if (err) {
                result.success = false;
            } else {
                let response = data.body;
                
                if( response.responseCode === "OK" &&
                    response.result &&
                    response.result.response &&
                    response.result.response.length > 0
                ) {                    
                    result["data"] = response.result.response;
                    result["count"] = response.result.count;                    
                } else {
                    result.success = false;
                }
            }
            return resolve(result);
        }

      } catch (error) {
            console.log("Consoling the error from locationSearch(): ", error)
            return reject(error);
      }
  })
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function runMigration() {
    const client = new MongoClient(MONGO_URI, {
        useNewUrlParser: true,
        useUnifiedTopology: true
    });
    await client.connect();
    console.log("DB connected successfully.");

    const db = client.db();
    const collection = db.collection(COLLECTION);
    
    let projectsEligibleForUpdate = [];
    let failedProjectUpdateStatus = {};
    
    let projectIds = await collection.find(
                            {
                                createdAt : {$gte: fromDate, $lt: toDate }
                            },
                            {
                                projection: {
                                    _id: 1
                                }
                            }
                        )
                        .toArray();
                        
    projectIds = projectIds.map(project => project._id);

    const chunks = _.chunk(projectIds, BATCH_SIZE);

    for (const [index, chunk] of chunks.entries()) {
        console.log(`Processing batch: ${index+1}.`);
        const projects = await collection
                        .find(
                            { _id: { $in: chunk} },
                            { projection: { _id: 1, userRoleInformation: 1, userProfile: 1 } }
                        )
                        .toArray();


        if (projects.length === 0) {
            console.log("No more documents to process.");
            continue;
        }

        let bulkOps = [];
        let neededUdiseCodes = [];
        for (const project of projects) {
            // If userRoleInformation does not exist, skip
            if (!project.userRoleInformation || !project.userRoleInformation.school) continue;

            // If userRoleInformation.school is not uuid, skip
            if(!UTILS.checkValidUUID(project.userRoleInformation.school)) continue;

            // Track project IDs that are eligible for update
            projectsEligibleForUpdate.push(project._id.toString());

            // Try to fetch the corresponding UDISE code
            // (may return null/undefined if not yet available)
            const udiseCode = fetchUdiseCode(project);


            if(!udiseCode){
                // If UDISE code is not found locally,
                // collect UUIDs to fetch from user-service in bulk later
                neededUdiseCodes.push({
                    projectId : project._id,
                    uuid : project.userRoleInformation.school
                });
            }else{
                // If UDISE code is already available,
                // prepare a bulk DB update to replace UUID with UDISE code
                if(doUpdate){
                    bulkOps.push({
                        updateOne: {
                            filter: { _id: project._id },
                            update: {
                                $set: {
                                    "userRoleInformation.school": udiseCode
                                }
                            }
                        }
                    });
                }
            }
        }

        if(neededUdiseCodes.length > 0){
            // Call Location Search API to fetch UDISE codes for school UUIDs
            const response = await locationSearch(neededUdiseCodes);

            // If the API call itself fails, mark all related projects as failed
            if(!response.success){
                neededUdiseCodes.forEach(obj => {
                    failedProjectUpdateStatus[obj.projectId.toString()] = {
                        success : false,
                        message : "Location Search api call failed."
                    }
                })
            }else{
                // API call succeeded; attempt to map UUIDs to UDISE codes
                neededUdiseCodes.forEach(obj => {
                    // Find matching location entry using school UUID
                    const matchItem = response.data.find(item => item.id == obj.uuid);

                    // If a valid UDISE code is found, prepare a bulk DB update
                    if(matchItem && matchItem.code && (matchItem.code != "")){
                        if(doUpdate){
                            bulkOps.push({
                                updateOne: {
                                    filter: { _id: obj.projectId },
                                    update: {
                                        $set: {
                                            "userRoleInformation.school": matchItem.code
                                        }
                                    }
                                }
                            });
                        }
                    }
                    else{
                        // If no UDISE code is found for the UUID, mark this project update as failed
                        failedProjectUpdateStatus[obj.projectId.toString()] = {
                            success : false,
                            message : "Udise code not found in locationSearch api."
                        }
                    }
                    // Attach resolved UDISE code (or null) to the object for tracking / debugging purposes
                    obj.udise = matchItem ? matchItem.code : null;
                })           
            }
        }
        // Update DB for this batch        
        if (doUpdate && (bulkOps.length > 0)) {
            const result = await collection.bulkWrite(bulkOps);
            console.log(`Batch-${index+1} updated: Matched ${result.matchedCount}, Modified ${result.modifiedCount}`);
        }

        // Pause for 5 seconds before processing next batch
        console.log("⏳ Waiting for 3 seconds before processing next batch...");
        await sleep(3 * 1000); // 3 seconds
    }

    await client.close();
    console.log("DB connection closed.");

    fs.writeFileSync(
        outputFilePath,
        JSON.stringify({executionMode, dateRange, projectsEligibleForUpdate, failedProjectUpdateStatus}, null, 2),
        "utf8"
    )
    console.log("Script log file created at:", outputFilePath);
    console.log("Script execution completed.");
}

runMigration().catch(err => {
    console.error("Error running migration:", err);
    process.exit(1);
});
