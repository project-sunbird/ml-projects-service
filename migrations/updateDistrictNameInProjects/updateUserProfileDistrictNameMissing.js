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
(async () => {
  let connection = await MongoClient.connect(url, { useNewUrlParser: true });
  let db = connection.db(dbName);
  try {
    let updatedProjectIds = [];
    let failedToGetProfileProjectIds = [];

    let projectIds = [
      new ObjectId("638ef0d0be39f5000813f984"),
      new ObjectId("63b3b13d01da8e0008597faa"),
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
              profile.data.response,
              projectDocuments[count].userRoleInformation
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
                  userProfile: userDetailsForProject.profile,
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
    function getUserRoleAndProfileWithUpdatedData(
      profile,
      userRoleInformation
    ) {
      for (let counter = 0; counter < profile.userLocations.length; counter++) {
        if (profile.userLocations[counter].type === "school") {
          userRoleInformation.school = profile.userLocations[counter].code;
        } else {
          userRoleInformation[profile.userLocations[counter].type] =
            profile.userLocations[counter].id;
        }
      }
      if (userRoleInformation.hasOwnProperty("role")) {
        //get RoleInformation
        let userRoles = userRoleInformation.role.split(",");
        profile.profileUserType = new Array();
        for (let j = 0; j < userRoles.length; j++) {
          if (userRoles[j].toUpperCase() === "TEACHER") {
            // If subRole is teacher
            profile.profileUserType.push({
              subType: null,
              type: "teacher",
            });
          } else {
            // If subRole is not teacher
            profile.profileUserType.push({
              subType: userRoles[j].toLowerCase(),
              type: "administrator",
            });
          }
        }
      }
      return { userRoleInformation: userRoleInformation, profile: profile };
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
