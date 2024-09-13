/**
 * name : updateProjectWithProfileData.js
 * author : Ankit Shahu
 * created-date : 10-Nov-2023
 * Description : Migration script for update userProfile in project
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
const userReadEndpoint = "/private/user/v1/read";

(async () => {
  let connection = await MongoClient.connect(url, { useNewUrlParser: true });
  let db = connection.db(dbName);
  try {
    let updatedProjectIds = {
      userRoleInformationMissingProjectIds: [],
      userProfileMissingProjectIds: [],
      bothDataMissingProjectIds: [],
      failedToGetProfileProjectIds: [],
    };

    // get all projects id where user profile is not there.
    let projectDocument = await db
      .collection("projects")
      .find({ userProfile: { $exists: false } })
      .project({ _id: 1 })
      .toArray();

    //make it in chunks so that we can iterate over
    let chunkOfProjectDocument = _.chunk(projectDocument, 100);
    let projectIds;

    for (
      let pointerToProject = 0;
      pointerToProject < chunkOfProjectDocument.length;
      pointerToProject++
    ) {
      projectIds = await chunkOfProjectDocument[pointerToProject].map(
        (projectDoc) => {
          return projectDoc._id;
        }
      );

      //get project documents along with userId, userProfile and userRoleInformation
      let projectWithIncompleteData = await db
        .collection("projects")
        .find({
          _id: { $in: projectIds },
        })
        .project({
          _id: 1,
          userId: 1,
          userProfile: 1,
          userRoleInformation: 1,
        })
        .toArray();

      let projectSeggregate = {
        userProfileMissing: [],
        userRoleInformationMissing: [],
        bothDataMissing: [],
      };

      // seggredate project data based on condition
      for (let count = 0; count < projectWithIncompleteData.length; count++) {
        if (
          !projectWithIncompleteData[count].hasOwnProperty(
            "userRoleInformation"
          ) &&
          !projectWithIncompleteData[count].hasOwnProperty("userProfile")
        ) {
          projectSeggregate.bothDataMissing.push(
            projectWithIncompleteData[count]
          );
        } else if (
          !projectWithIncompleteData[count].hasOwnProperty(
            "userRoleInformation"
          )
        ) {
          projectSeggregate.userRoleInformationMissing.push(
            projectWithIncompleteData[count]
          );
        } else if (
          !projectWithIncompleteData[count].hasOwnProperty("userProfile")
        ) {
          projectSeggregate.userProfileMissing.push(
            projectWithIncompleteData[count]
          );
        }
      }

      //update userRoleInformations if userProfile is present in project
      if (projectSeggregate.userRoleInformationMissing.length > 0) {
        let updateUserRoleInformationInProjects = [];
        for (
          let count = 0;
          count < projectSeggregate.userRoleInformationMissing.length;
          count++
        ) {
          let userRoleInformationForProject =
            await getUserRoleInformationFromProfile(
              projectSeggregate.userRoleInformationMissing[count].userProfile
            );
          let updateProjectWithRoleInformation = {
            updateOne: {
              filter: {
                _id: projectSeggregate.userRoleInformationMissing[count]._id,
              },
              update: {
                $set: { userRoleInformation: userRoleInformationForProject },
              },
            },
          };

          updateUserRoleInformationInProjects.push(
            updateProjectWithRoleInformation
          );
          updatedProjectIds.userRoleInformationMissingProjectIds.push(
            projectSeggregate.userRoleInformationMissing[count]._id
          );
        }
        // will create BulkWrite Query to otimize excution
        await db
          .collection("projects")
          .bulkWrite(updateUserRoleInformationInProjects);
      }

      // update Projects With Profile and UserRoleInformation if both are missing
      if (projectSeggregate.bothDataMissing.length > 0) {
        let updateProjectWithProfileAndUserRoleInformation = [];
        for (
          let count = 0;
          count < projectSeggregate.bothDataMissing.length;
          count++
        ) {
          let projectIdWithoutUserProfile =
            projectSeggregate.bothDataMissing[count]._id;
          let userId = projectSeggregate.bothDataMissing[count].userId;

          //call profile api to get user profile
          let profile = await profileReadPrivate(userId);

          if (profile.success && profile.data && profile.data.response) {
            //get userRoleInformation from Resuable function
            let userRoleInformationForProject =
              await getUserRoleInformationFromProfile(profile.data.response);
            let updateObject = {
              updateOne: {
                filter: {
                  _id: projectIdWithoutUserProfile,
                },
                update: {
                  $set: {
                    userRoleInformation: userRoleInformationForProject,
                    userProfile: profile.data.response,
                  },
                },
              },
            };
            updateProjectWithProfileAndUserRoleInformation.push(updateObject);
            updatedProjectIds.bothDataMissingProjectIds.push(
              projectIdWithoutUserProfile
            );
          } else if (!profile.success) {
            updatedProjectIds.failedToGetProfileProjectIds.push(
              projectIdWithoutUserProfile
            );
          }
        }
        // will create BulkWrite Query to otimize excution
        if (updateProjectWithProfileAndUserRoleInformation.length > 0) {
          await db
            .collection("projects")
            .bulkWrite(updateProjectWithProfileAndUserRoleInformation);
        }
      }

      // Update Project with user Profile if userRoleInformation is present
      if (projectSeggregate.userProfileMissing.length > 0) {
        let updateProjectWithUserProfile = [];
        for (
          let count = 0;
          count < projectSeggregate.userProfileMissing.length;
          count++
        ) {
          let projectIdWithoutUserProfile =
            projectSeggregate.userProfileMissing[count]._id;
          let userId = projectSeggregate.userProfileMissing[count].userId;

          //call profile api to get user profile
          let profile = await profileReadPrivate(userId);
          if (profile.success && profile.data && profile.data.response) {
            let userProfile = profile.data.response;
            let userRoleInformation = await getUserRoleInformationFromProfile(
              userProfile
            );
            let bothRoleInformationEqual = _.isEqual(
              userRoleInformation,
              projectSeggregate.userProfileMissing[count].userRoleInformation
            );

            if (!bothRoleInformationEqual) {
              let userRoleInformationkeys = Object.keys(
                projectSeggregate.userProfileMissing[count].userRoleInformation
              );
              userProfile = _.omit(userProfile, ["userLocations"]);
              userProfile.userLocations = [];
              for (let i = 0; i < userRoleInformationkeys.length; i++) {
                if (userRoleInformationkeys[i].toUpperCase() === "SCHOOL") {
                  //get school data
                  let filterData = {
                    code: projectSeggregate.userProfileMissing[count]
                      .userRoleInformation[userRoleInformationkeys[i]],
                  };
                  let schoolData = await locationSearch(filterData);
                  if (schoolData.success) {
                    userProfile.userLocations.push(...schoolData.data);
                  }
                } else if (
                  userRoleInformationkeys[i].toUpperCase() === "ROLE"
                ) {
                  //get RoleInformation
                  let userRoles =
                    projectSeggregate.userProfileMissing[
                      count
                    ].userRoleInformation[userRoleInformationkeys[i]].split(
                      ","
                    );
                  userProfile.profileUserType = new Array();
                  for (let j = 0; j < userRoles.length; j++) {
                    if (userRoles[j].toUpperCase() === "TEACHER") {
                      // If subRole is teacher
                      userProfile.profileUserType.push({
                        subType: null,
                        type: "teacher",
                      });
                    } else {
                      // If subRole is not teacher
                      userProfile.profileUserType.push({
                        subType: userRoles[j].toLowerCase(),
                        type: "administrator",
                      });
                    }
                  }
                } else {
                  // get Other RoleInformation data
                  let filterData = {
                    id: projectSeggregate.userProfileMissing[count]
                      .userRoleInformation[userRoleInformationkeys[i]],
                  };
                  let userLocations = await locationSearch(filterData);
                  if (userLocations.success) {
                    userProfile.userLocations.push(...userLocations.data);
                  }
                }
              }
            }

            let updateObject = {
              updateOne: {
                filter: {
                  _id: projectIdWithoutUserProfile,
                },
                update: {
                  $set: {
                    userProfile: userProfile,
                  },
                },
              },
            };

            updateProjectWithUserProfile.push(updateObject);
            updatedProjectIds.userProfileMissingProjectIds.push(
              projectIdWithoutUserProfile
            );
          } else if (!profile.success) {
            updatedProjectIds.failedToGetProfileProjectIds.push(
              projectIdWithoutUserProfile
            );
          }
        }
        // will create BulkWrite Query to otimize excution
        if (updateProjectWithUserProfile.length > 0) {
          await db
            .collection("projects")
            .bulkWrite(updateProjectWithUserProfile);
        }
      }
      //write updated project ids to file
      fs.writeFile(
        `updatedProjectIds.json`,

        JSON.stringify(updatedProjectIds),

        function (err) {
          if (err) {
            console.error("Crap happens");
          }
        }
      );
    }
    // this function is used to get userRoleInformation from userProfile
    function getUserRoleInformationFromProfile(profile) {
      let userRoleInformationForProject = {};
      for (let counter = 0; counter < profile.userLocations.length; counter++) {
        if (profile.userLocations[counter].type === "school") {
          userRoleInformationForProject.school =
            profile.userLocations[counter].code;
        } else {
          userRoleInformationForProject[profile.userLocations[counter].type] =
            profile.userLocations[counter].id;
        }
      }
      let Roles = [];
      for (
        let counter = 0;
        counter < profile.profileUserType.length;
        counter++
      ) {
        if (
          profile.profileUserType[counter].hasOwnProperty("subType") &&
          profile.profileUserType[counter].subType !== null
        ) {
          Roles.push(profile.profileUserType[counter].subType.toUpperCase());
        } else {
          Roles.push(profile.profileUserType[counter].type.toUpperCase());
        }
      }
      userRoleInformationForProject.role = Roles.join(",");
      return userRoleInformationForProject;
    }
    // this function is used to get location data
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
    //this function to get the user profileData from learn service

    function profileReadPrivate(userId) {
      return new Promise(async (resolve, reject) => {
        try {
          //  <--- Important : This url endpoint is private do not use it for regular workflows --->
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

let;
