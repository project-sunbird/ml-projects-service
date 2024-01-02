/**
 * name : updateUserProfileDistrictNameMissing.js
 * author : Ankit Shahu
 * created-date : 10-Nov-2023
 * Description : update delhi projects where district name is missing
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
const userReadEndpoint = "/private/user/v1/read";
const endPoint = "/v1/location/search";
const orgSearchEndPoint = "/v1/org/search";

(async () => {
  let connection = await MongoClient.connect(url, { useNewUrlParser: true });
  let db = connection.db(dbName);
  try {
    let updatedProjectIds = [];
    let failedToGetProfileProjectIds = [];

    let projectIds = [
      new ObjectId("63708ef290fd230009ff55f6"),
      new ObjectId("636c882190fd230009eea38e"),
    ];

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
        userId: 1,
      })
      .toArray();

    // update Projects With Profile and UserRoleInformation if both are missing
    if (projectDocuments.length > 0) {
      let updateProjectUserRoleAndProfile = [];
      for (let count = 0; count < projectDocuments.length; count++) {
        let projectId = projectDocuments[count]._id;
        let userId = projectDocuments[count].userId;

        //call profile api to get user profile
        let profile = await profileReadPrivate(userId);

        if (profile.success && profile.data && profile.data.response) {
          //get userRoleInformation from Resuable function
          let userDetailsForProject =
            await getUserRoleAndProfileWithUpdatedData(
              profile.data.response
            );
          let updateObject = {
            updateOne: {
              filter: {
                _id: projectId,
              },
              update: {
                $set: {
                  userRoleInformation:
                    userDetailsForProject.userRoleInformation,
                  userProfile: userDetailsForProject.userProfile,
                },
              },
            },
          };
          updateProjectUserRoleAndProfile.push(updateObject);
          updatedProjectIds.push(projectId);
        } else if (!profile.success) {
          failedToGetProfileProjectIds.push(projectId);
        }
      }
      // will create BulkWrite Query to otimize excution
      if (updateProjectUserRoleAndProfile.length > 0) {
        await db
          .collection("projects")
          .bulkWrite(updateProjectUserRoleAndProfile);
      }
    }
    //write updated project ids to file
    fs.writeFile(
      `updateUserProfileDistrictNameMissing.json`,

      JSON.stringify({
        updatedProjectIds: updatedProjectIds,
        failedToGetProfileProjectIds: failedToGetProfileProjectIds,
      }),

      function (err) {
        if (err) {
          console.error("Crap happens");
        }
      }
    );
    // this function is used to get userRoleInformation from userProfile
    async function getUserRoleAndProfileWithUpdatedData(
      profile
    ) {
        let userProfile = profile;
        let userRoleInformation = {}
        let Roles = [];
        for (
          let counter = 0;
          counter < profile.profileUserTypes.length;
          counter++
        ) {
          if (
            profile.profileUserTypes[counter].hasOwnProperty("subType") &&
            profile.profileUserTypes[counter].subType !== null
          ) {
            Roles.push(profile.profileUserTypes[counter].subType.toUpperCase());
          } else {
            Roles.push(profile.profileUserTypes[counter].type.toUpperCase());
          }
        }
        userRoleInformation.role = Roles.join(",");
        

        // If userProfile.userLocations has to be updated, get all values and set in userProfile.
        if(userProfile.profileLocation) {
          //update userLocations in userProfile
          let locationIds = [];
          let locationCodes = [];
          let userLocations = new Array;

          let schoolData = userProfile.organisations.filter(organisations=>{
            if(organisations.organisationId !== userProfile.rootOrgId){
              return organisations;
            }
          })
          if(schoolData.length > 0){
            let orgSearchQuery = {
              "id": schoolData[0].id
            }
            let schoolCode = await orgSearch(orgSearchQuery);
            if ( schoolCode.success && schoolCode.data.content.length > 0) {
              locationCodes = schoolCode.data.content[0].externalId;
            }
          }
      
            

            userProfile.profileLocation.forEach( locationId => {
              locationIds.push(locationId.id);
            })

            //query for fetch location using id
            if ( locationIds.length > 0 ) {
                let locationQuery = {
                    "id" : locationIds
                }

                let entityData = await locationSearch(locationQuery);
                if ( entityData.success ) {
                    userLocations = entityData.data;
                }
            }

            // query for fetch location using code
            if ( locationCodes.length > 0 ) {
                let codeQuery = {
                    "code" : locationCodes
                }

                let entityData = await locationSearch(codeQuery);
                if ( entityData.success ) {
                    userLocations =  userLocations.concat(entityData.data);
                }
            }

            if ( userLocations.length > 0 ) {
                userProfile["userLocations"] = userLocations;
            }
        }

        userProfile.userLocations.forEach((locations)=>{
          if(locations.type === "school"){
            userRoleInformation[locations.type] = locations.code
          }else {
            userRoleInformation[locations.type] = locations.id
          }
        })
        return {userProfile: userProfile, userRoleInformation: userRoleInformation}
    }

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
              console.log("failed to get location")
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
            console.log("failed to get location")
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

    function orgSearch(filterData) {
      return new Promise(async (resolve, reject) => {
        try {
          let bodyData = {};
          bodyData["request"] = {};
          bodyData["request"]["filters"] = filterData;
          const url = userServiceUrl + orgSearchEndPoint;
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
              console.log("failed to get org")
            } else {
              let response = data.body;
              if (
                response.responseCode === "OK" &&
                response.result &&
                response.result.response
              ) {
                result["data"] = response.result.response;
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

    function profileReadPrivate(userId) {
      return new Promise(async (resolve, reject) => {
        try {
          let url = userServiceUrl + userReadEndpoint + "/" + userId;
          const options = {
            headers: {
              "content-type": "application/json",
            },
          };
          request.get(url, options, userReadCallback);
          let result = {
            success: true,
          };
          function userReadCallback(err, data) {
            if (err) {
              result.success = false;
              console.log("failed to get profile"+ userId)
            } else {
              let response = JSON.parse(data.body);
              if (response.responseCode === "OK") {
                result["data"] = response.result;
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

    console.log("Updated Project Count : ", updatedProjectIds.length);
    console.log("completed");
    connection.close();
  } catch (error) {
    console.log(error);
  }
})().catch((err) => console.error(err));
