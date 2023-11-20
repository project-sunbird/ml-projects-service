/**
 * name : updateDistrictName.js
 * author : Ankit Shahu
 * created-date : 10-Nov-2022
 * Description : Update District Name in projects where district name miss matching
 */

const path = require("path");
let rootPath = path.join(__dirname, "../../");
require("dotenv").config({ path: rootPath + "/.env" });

let _ = require("lodash");
let mongoUrl = process.env.MONGODB_URL;
let dbName = mongoUrl.split("/").pop();
let url = mongoUrl.split(dbName)[0];
var MongoClient = require("mongodb").MongoClient;
var ObjectId = require("mongodb").ObjectID;

var fs = require("fs");
const request = require("request");

const userServiceUrl = "http://learner-service:9000";
const endPoint = "/v1/location/search";

(async () => {
  let connection = await MongoClient.connect(url, { useNewUrlParser: true });
  let db = connection.db(dbName);
  try {
    let updatedProjectIds = [];

    let projectIds = [
      new ObjectId("629739efe95af10009e7e57c"),
      new ObjectId("6299eb19e95af10009e8e28f"),
      new ObjectId("629a34b3e95af10009e91338"),
      new ObjectId("629a3555e95af10009e913a6"),
      new ObjectId("62b0416910889b000746d088"),
      new ObjectId("62b2a009cd5f3c00077a1c47"),
      new ObjectId("62b56b29cd5f3c00077a3282"),
      new ObjectId("62b98078cd5f3c00077a4814"),
      new ObjectId("63019b518f78ad0007651260"),
      new ObjectId("6307813b8f78ad0007652524"),
      new ObjectId("62985b30e95af10009e81ea0"),
      new ObjectId("62985b345e195a0007d8fff0"),
      new ObjectId("62985b365e195a0007d8fff4"),
    ];

    let filedMissmatch = "district";

    //get project information from db
    let projectDocuments = await db
      .collection("projects")
      .find({
        _id: { $in: projectIds },
      })
      .project({
        _id: 1,
        userProfile: 1,
        userRoleInformation: 1,
      })
      .toArray();
    let updateProjectDocument = [];

    for (let count = 0; count < projectDocuments.length; count++) {
      let filterData = {
        id: projectDocuments[count].userRoleInformation[filedMissmatch],
      };
      let correctDistrictData = await locationSearch(filterData);
      let userLocationsWithCorrectData = [];
      if (correctDistrictData.success) {
        let userLocations = projectDocuments[count].userProfile.userLocations;
        userLocationsWithCorrectData = userLocations.filter(function (
          locations
        ) {
          return (
            locations.id !==
            projectDocuments[count].userRoleInformation[filedMissmatch]
          );
        });
        userLocationsWithCorrectData.push(...correctDistrictData.data);
      }
      if (userLocationsWithCorrectData > 0) {
        let updateObject = {
          updateOne: {
            filter: {
              _id: projectDocuments[count]._id,
            },
            update: {
              $set: {
                "userProfile.userLocations": userLocationsWithCorrectData,
              },
            },
          },
        };
        updateProjectDocument.push(updateObject);
        updatedProjectIds.push(projectDocuments[count]._id);
      }
    }
    // will create BulkWrite Query to otimize excution
    if (updateProjectDocument > 0) {
      await db.collection("projects").bulkWrite(updateProjectDocument);
    }
    fs.writeFile(
      `updateDistrictName.json`,

      JSON.stringify(updatedProjectIds),

      function (err) {
        if (err) {
          console.error("Crap happens");
        }
      }
    );
    // used to get location information from learn service
    function locationSearch(filterData) {
      return new Promise(async (resolve, reject) => {
        try {
          let bodyData = {};
          bodyData["request"] = {};
          bodyData["request"]["filters"] = filterData;
          const url = userServiceUrl + endPoint;
          const options = {
            headers: {
              "content-type": "application/json",
            },
            json: bodyData,
          };

          request.post(url, options, requestCallback);

          let result = {
            success: true,
          };

          function requestCallback(err, data) {
            if (err) {
              result.success = false;
            } else {
              let response = data.body;
              if (
                response.responseCode === "OK" &&
                response.result &&
                response.result.response &&
                response.result.response.length > 0
              ) {
                let entityResult = new Array();
                response.result.response.map((entityData) => {
                  let entity = _.omit(entityData, ["identifier"]);
                  entityResult.push(entity);
                });
                result["data"] = entityResult;
                result["count"] = response.result.count;
              } else {
                result.success = false;
              }
            }
            return resolve(result);
          }

          setTimeout(function () {
            return resolve(
              (result = {
                success: false,
              })
            );
          }, 5000);
        } catch (error) {
          return reject(error);
        }
      });
    }

    //write updated project ids to file

    console.log("Updated Project Count : ", updatedProjectIds.length);
    console.log("completed");
    connection.close();
  } catch (error) {
    console.log(error);
  }
})().catch((err) => console.error(err));
