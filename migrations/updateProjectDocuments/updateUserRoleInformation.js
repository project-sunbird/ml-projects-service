const path = require("path");
const fs = require("fs");
require("dotenv").config({ path: path.join(__dirname, "../../") + "/.env" });
const { MongoClient, ObjectId } = require("mongodb");
const UTILS = require("../../generics/helpers/utils");
const request = require("request");

const MONGO_URI = process.env.MONGODB_URL;
const COLLECTION = "projects";
const BATCH_SIZE = 100;
const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
const outputFilePath = path.join(__dirname, `project-update-log.json-${timestamp}.json`);

let doUpdate = false;
const doUpdateArg = process.argv.find(arg => arg.startsWith('--update='));
doUpdate = doUpdateArg ? doUpdateArg.split('=')[1] : null;
doUpdate = doUpdate == "true" ? true : false
let executionMode = "READ-MODE"
if(doUpdate) executionMode = "WRITE-MODE";

function fetchUdiseCode(doc){
    const uuid = doc.userRoleInformation.school
    // if school udise code is found in userProfile.userLocations, use it to update the DB 
    if(doc.userProfile && doc.userProfile.userLocations && Array.isArray(doc.userProfile.userLocations) && doc.userProfile.userLocations.length > 0){
        const locations = doc.userProfile.userLocations;
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
        console.log(url)
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
                
                if( response.responseCode === CONSTANTS.common.OK &&
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

    const db = client.db();
    const collection = db.collection(COLLECTION);

    // Fetch documents created from 1st September 2025 (UTC)
    const fromDate = new Date("2025-09-01T00:00:00.000Z");

    console.log("Fetching projects created since:", fromDate.toISOString());

    let lastId = null;

    let projectsEligibleForUpdate = [];
    let failedProjectUpdateStatus = {};

    while (true) {
        const query = {
            createdAt: { $gte: fromDate.toISOString() }
        };

        if (lastId) {
            query._id = { $gt: lastId };
        }

        // Projection: fetch only required fields
        const projection = {
            _id: 1,
            userRoleInformation: 1,
            userProfile: 1
        };

        const docs = await collection
            .find(query, { projection })
            .sort({ _id: 1 })
            .limit(BATCH_SIZE)
            .toArray();

        if (docs.length === 0) {
            console.log("No more documents to process.");
            break;
        }

        let bulkOps = [];
        let neededUdiseCodes = [];
        for (const doc of docs) {
            // If userRoleInformation does not exist, skip
            if (!doc.userRoleInformation || !doc.userRoleInformation.school) continue;

            // If userRoleInformation.school is not uuid, skip
            if(!UTILS.checkValidUUID(doc.userRoleInformation.school)) continue;

            // Track project IDs that are eligible for update
            projectsEligibleForUpdate.push(doc._id.toString());

            // Try to fetch the corresponding UDISE code
            // (may return null/undefined if not yet available)
            const udiseCode = fetchUdiseCode(doc);


            if(!udiseCode){
                // If UDISE code is not found locally,
                // collect UUIDs to fetch from user-service in bulk later
                neededUdiseCodes.push({
                    projectId : doc._id,
                    uuid : doc.userRoleInformation.school
                });
            }else{
                // If UDISE code is already available,
                // prepare a bulk DB update to replace UUID with UDISE code
                bulkOps.push({
                    updateOne: {
                        filter: { _id: doc._id },
                        update: {
                            $set: {
                                "userRoleInformation.school": udiseCode
                            }
                        }
                    }
                });
            }
        }
        if(doUpdate){

            if(neededUdiseCodes.length > 0){
                // Call Location Search API to fetch UDISE codes for school UUIDs
                const response = await locationSearch(neededUdiseCodes);
    
                // If the API call itself fails, mark all related projects as failed
                if(!response.success){
                    console.log("Location Search api failed for the current batch!");
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
            if (bulkOps.length > 0) {
                const result = await collection.bulkWrite(bulkOps);
                console.log(`Batch updated: Matched ${result.matchedCount}, Modified ${result.modifiedCount}`);
            }
        }

        // Move cursor forward
        lastId = docs[docs.length - 1]._id;
        console.log(`Processed batch ending at _id: ${lastId}`);

        // Pause for 30 seconds before processing next batch
        console.log("⏳ Waiting for 30 seconds before processing next batch...");
        await sleep(30 * 1000); // 30 seconds
    }

    await client.close();

    fs.writeFileSync(
        outputFilePath,
        JSON.stringify({executionMode, projectsEligibleForUpdate, failedProjectUpdateStatus}, null, 2),
        "utf8"
    )
    console.log("Script log file created at:", outputFilePath);

    console.log("Script execution completed.");
}

runMigration().catch(err => {
    console.error("Error running migration:", err);
    process.exit(1);
});
