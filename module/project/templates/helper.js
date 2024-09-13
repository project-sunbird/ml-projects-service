/**
 * name : helper.js
 * author : Aman
 * created-date : 16-July-2020
 * Description : Project templates helper functionality.
 */

/**
 * ProjectTemplatesHelper
 * @class
 */

// Dependencies

const libraryCategoriesHelper = require(MODULES_BASE_PATH +
  "/library/categories/helper");
const coreService = require(GENERICS_FILES_PATH + "/services/core");
const kafkaProducersHelper = require(GENERICS_FILES_PATH + "/kafka/producers");
const learningResourcesHelper = require(MODULES_BASE_PATH +
  "/learningResources/helper");
const surveyService = require(GENERICS_FILES_PATH + "/services/survey");
const projectTemplateQueries = require(DB_QUERY_BASE_PATH +
  "/projectTemplates");
const projectTemplateTaskQueries = require(DB_QUERY_BASE_PATH +
  "/projectTemplateTask");
const projectQueries = require(DB_QUERY_BASE_PATH + "/projects");
const projectCategoriesQueries = require(DB_QUERY_BASE_PATH +
  "/projectCategories");
const solutionsQueries = require(DB_QUERY_BASE_PATH + "/solutions");
const certificateTemplateQueries = require(DB_QUERY_BASE_PATH +
  "/certificateTemplates");

module.exports = class ProjectTemplatesHelper {
  /**
   * Extract csv information.
   * @method
   * @name extractCsvInformation
   * @param {Object} csvData - csv data.
   * @returns {Object} Extra csv information.
   */

  static extractCsvInformation(csvData) {
    return new Promise(async (resolve, reject) => {
      try {
        let categoryIds = [];
        let roleIds = [];
        let tasksIds = [];
        // <- Entitytype validation removed {release-5.0.0} - entity generalisation
        // let entityTypes = [];

        csvData.forEach((template) => {
          let parsedData = UTILS.valueParser(template);

          categoryIds = _.concat(categoryIds, parsedData.categories);

          tasksIds = _.concat(tasksIds, parsedData.tasks);

          if (parsedData.recommendedFor) {
            parsedData.recommendedFor = parsedData.recommendedFor.map(
              (role) => {
                return role.toUpperCase();
              }
            );

            roleIds = _.concat(roleIds, parsedData.recommendedFor);
          }
          // <- Entitytype validation removed {release-5.0.0} - entity generalisation
          // if( parsedData.entityType ) {
          //     entityTypes.push(parsedData.entityType);
          // }
        });

        let categoriesData = {};

        if (categoryIds.length > 0) {
          let categories = await projectCategoriesQueries.categoryDocuments(
            {
              externalId: { $in: categoryIds },
            },
            ["externalId", "name"]
          );

          if (!(categories.length > 0)) {
            throw {
              status: HTTP_STATUS_CODE["bad_request"].status,
              message: CONSTANTS.apiResponses.LIBRARY_CATEGORIES_NOT_FOUND,
            };
          }

          categoriesData = categories.reduce(
            (ac, category) => ({
              ...ac,
              [category.externalId]: {
                _id: ObjectId(category._id),
                externalId: category.externalId,
                name: category.name,
              },
            }),
            {}
          );
        }

        let recommendedFor = {};

        if (roleIds.length > 0) {
          let userRolesData = await coreService.rolesDocuments(
            {
              code: { $in: roleIds },
            },
            ["code"]
          );

          if (!userRolesData.success) {
            throw {
              message: CONSTANTS.apiResponses.USER_ROLES_NOT_FOUND,
              status: HTTP_STATUS_CODE["bad_request"].status,
            };
          }

          recommendedFor = userRolesData.data.reduce(
            (ac, role) => ({
              ...ac,
              [role.code]: {
                roleId: ObjectId(role._id),
                code: role.code,
              },
            }),
            {}
          );
        }
        // <- Entitytype validation removed {release-5.0.0} - entity generalisation
        // let entityTypesData = {};

        // if( entityTypes.length > 0 ) {

        //     let entityTypesDocument =
        //     await coreService.entityTypesDocuments();

        //     if( !entityTypesDocument.success ) {
        //         throw {
        //             message : CONSTANTS.apiResponses.ENTITY_TYPES_NOT_FOUND,
        //             status : HTTP_STATUS_CODE['bad_request'].status
        //         }
        //     }

        //     entityTypesData = entityTypesDocument.data.reduce((ac,entityType)=> ({
        //         ...ac,
        //         [entityType.name] : {
        //             _id : ObjectId(entityType._id),
        //             name : entityType.name
        //         }
        //     }),{});

        // }

        return resolve({
          success: true,
          data: {
            categories: categoriesData,
            roles: recommendedFor,
            // <- Entitytype validation removed {release-5.0.0} - entity generalisation
            // entityTypes : entityTypesData
          },
        });
      } catch (error) {
        return resolve({
          success: false,
          message: error.message,
          status: error.status
            ? error.status
            : HTTP_STATUS_CODE["internal_server_error"].status,
        });
      }
    });
  }

  /**
   * Template data.
   * @method
   * @name templateData
   * @param {Object} data  - csv data.
   * @param {Object} csvInformation - csv information.
   * @returns {Object} Template data.
   */

  static templateData(data, csvInformation) {
    return new Promise(async (resolve, reject) => {
      try {
        let templatesDataModel = Object.keys(
          schemas["project-templates"].schema
        );
        let parsedData = UTILS.valueParser(data);
        delete parsedData._arrayFields;

        let categories = [];

        if (parsedData.categories && parsedData.categories.length > 0) {
          parsedData.categories.forEach((category) => {
            if (csvInformation.categories[category]) {
              return categories.push(csvInformation.categories[category]);
            }
          });
        }

        parsedData.categories = categories;

        let recommendedFor = [];

        if (parsedData.recommendedFor && parsedData.recommendedFor.length > 0) {
          parsedData.recommendedFor.forEach((recommended) => {
            if (csvInformation.roles[recommended]) {
              return recommendedFor.push(csvInformation.roles[recommended]);
            }
          });
        }

        parsedData.recommendedFor = recommendedFor;
        // <- Entitytype validation removed {release-5.0.0} - entity generalisation
        // if( parsedData.entityType && parsedData.entityType !== "" ) {
        //     parsedData.entityType = csvInformation.entityTypes[parsedData.entityType].name;
        // }

        let learningResources =
          await learningResourcesHelper.extractLearningResourcesFromCsv(
            parsedData
          );
        parsedData.learningResources = learningResources.data;

        parsedData.metaInformation = {};
        let booleanData = UTILS.getAllBooleanDataFromModels(
          schemas["project-templates"].schema
        );

        Object.keys(parsedData).forEach((eachParsedData) => {
          if (!templatesDataModel.includes(eachParsedData)) {
            if (!eachParsedData.startsWith("learningResources")) {
              parsedData.metaInformation[eachParsedData] =
                parsedData[eachParsedData];
              delete parsedData[eachParsedData];
            }
          } else {
            if (booleanData.includes(eachParsedData)) {
              parsedData[eachParsedData] = UTILS.convertStringToBoolean(
                parsedData[eachParsedData]
              );
            }
          }
        });

        parsedData.isReusable = true;

        return resolve(parsedData);
      } catch (error) {
        return reject(error);
      }
    });
  }

  /**
   * Bulk created project templates.
   * @method
   * @name bulkCreate - bulk create project templates.
   * @param {Array} templates - csv templates data.
   * @param {String} userId - logged in user id.
   * @returns {Object} Bulk create project templates.
   */

  static bulkCreate(templates, userId) {
    return new Promise(async (resolve, reject) => {
      try {
        const fileName = `project-templates-creation`;
        let fileStream = new CSV_FILE_STREAM(fileName);
        let input = fileStream.initStream();

        (async function () {
          await fileStream.getProcessorPromise();
          return resolve({
            isResponseAStream: true,
            fileNameWithPath: fileStream.fileNameWithPath(),
          });
        })();

        let csvInformation = await this.extractCsvInformation(templates);

        if (!csvInformation.success) {
          return resolve(csvInformation);
        }

        for (let template = 0; template < templates.length; template++) {
          let currentData = templates[template];

          let templateData = await projectTemplateQueries.templateDocument(
            {
              status: CONSTANTS.common.PUBLISHED,
              externalId: currentData.externalId,
              isReusable: true,
            },
            ["_id"]
          );

          if (templateData.length > 0 && templateData[0]._id) {
            currentData["_SYSTEM_ID"] =
              CONSTANTS.apiResponses.PROJECT_TEMPLATE_EXISTS;
          } else {
            let templateData = await this.templateData(
              currentData,
              csvInformation.data,
              userId
            );

            templateData.status = CONSTANTS.common.PUBLISHED_STATUS;
            templateData.createdBy =
              templateData.updatedBy =
              templateData.userId =
                userId;
            templateData.isReusable = true;

            let createdTemplate = await projectTemplateQueries.createTemplate(
              templateData
            );

            if (!createdTemplate._id) {
              currentData["_SYSTEM_ID"] =
                CONSTANTS.apiResponses.PROJECT_TEMPLATE_NOT_FOUND;
            } else {
              currentData["_SYSTEM_ID"] = createdTemplate._id;

              if (
                templateData.categories &&
                templateData.categories.length > 0
              ) {
                let categories = templateData.categories.map((category) => {
                  return category._id;
                });

                let updatedCategories = await libraryCategoriesHelper.update(
                  {
                    _id: { $in: categories },
                  },
                  {
                    $inc: { noOfProjects: 1 },
                  }
                );

                if (!updatedCategories.success) {
                  currentData["_SYSTEM_ID"] = updatedCategories.message;
                }
              }

              // <- Dirty fix . Not required
              // const kafkaMessage =
              // await kafkaProducersHelper.pushProjectToKafka({
              //     internal : false,
              //     text :
              //     templateData.categories.length === 1 ?
              //     `A new project has been added under ${templateData.categories[0].name} category in library.` :
              //     `A new project has been added in library`,
              //     type : "information",
              //     action : "mapping",
              //     payload : {
              //         project_id: createdTemplate._id
              //     },
              //     is_read : false,
              //     internal : false,
              //     title : "New project Available!",
              //     created_at : new Date(),
              //     appType : process.env.IMPROVEMENT_PROJECT_APP_TYPE,
              //     inApp:false,
              //     push: true,
              //     pushToTopic: true,
              //     topicName : process.env.NODE_ENV + "-" + process.env.IMPROVEMENT_PROJECT_APP_NAME + process.env.TOPIC_FOR_ALL_USERS
              // });

              // if (kafkaMessage.status !== CONSTANTS.common.SUCCESS) {
              //     currentData["_SYSTEM_ID"] = CONSTANTS.apiResponses.COULD_NOT_PUSHED_TO_KAFKA;
              // }
            }
          }

          input.push(currentData);
        }

        input.push(null);
      } catch (error) {
        return reject(error);
      }
    });
  }

  /**
   * Bulk update project templates.
   * @method
   * @name bulkUpdate - bulk update project templates.
   * @param {Array} templates - csv templates data.
   * @param {String} userId - logged in user id.
   * @returns {Object} Bulk Update Project templates.
   */

  static bulkUpdate(templates, userId) {
    return new Promise(async (resolve, reject) => {
      try {
        const fileName = `project-templates-updation`;
        let fileStream = new CSV_FILE_STREAM(fileName);
        let input = fileStream.initStream();

        (async function () {
          await fileStream.getProcessorPromise();
          return resolve({
            isResponseAStream: true,
            fileNameWithPath: fileStream.fileNameWithPath(),
          });
        })();

        let csvInformation = await this.extractCsvInformation(templates);

        if (!csvInformation.success) {
          return resolve(csvInformation);
        }

        for (let template = 0; template < templates.length; template++) {
          const currentData = templates[template];

          if (!currentData._SYSTEM_ID) {
            currentData["UPDATE_STATUS"] =
              CONSTANTS.apiResponses.MISSING_PROJECT_TEMPLATE_ID;
          } else {
            const template = await projectTemplateQueries.templateDocument(
              {
                status: CONSTANTS.common.PUBLISHED,
                _id: currentData._SYSTEM_ID,
                status: CONSTANTS.common.PUBLISHED,
              },
              ["_id", "categories", "isReusable"]
            );

            if (!(template.length > 0 && template[0]._id)) {
              currentData["UPDATE_STATUS"] =
                CONSTANTS.apiResponses.PROJECT_TEMPLATE_NOT_FOUND;
            } else {
              let templateData = await this.templateData(
                _.omit(currentData, ["_SYSTEM_ID"]),
                csvInformation.data,
                userId
              );

              if (template[0].isReusable === false) {
                templateData.isReusable = false;
              }

              templateData.updatedBy = userId;

              let projectTemplateUpdated =
                await projectTemplateQueries.findOneAndUpdate(
                  {
                    _id: currentData._SYSTEM_ID,
                  },
                  {
                    $set: templateData,
                  },
                  {
                    new: true,
                  }
                );

              if (!projectTemplateUpdated || !projectTemplateUpdated._id) {
                currentData["UPDATE_STATUS"] =
                  CONSTANTS.apiResponses.PROJECT_TEMPLATE_NOT_UPDATED;
              }

              // Add projects count to categories
              if (
                templateData.categories &&
                templateData.categories.length > 0
              ) {
                let categories = templateData.categories.map((category) => {
                  return category._id;
                });

                let updatedCategories = await libraryCategoriesHelper.update(
                  {
                    _id: { $in: categories },
                  },
                  {
                    $inc: { noOfProjects: 1 },
                  }
                );

                if (!updatedCategories.success) {
                  currentData["UPDATE_STATUS"] = updatedCategories.message;
                }
              }

              // Remove project count from existing categories
              if (template[0].categories && template[0].categories.length > 0) {
                const categoriesIds = template[0].categories.map((category) => {
                  return category._id;
                });

                let categoriesUpdated = await libraryCategoriesHelper.update(
                  {
                    _id: { $in: categoriesIds },
                  },
                  {
                    $inc: { noOfProjects: -1 },
                  }
                );

                if (!categoriesUpdated.success) {
                  currentData["UPDATE_STATUS"] = updatedCategories.message;
                }
              }

              currentData["UPDATE_STATUS"] = CONSTANTS.common.SUCCESS;
            }
          }

          input.push(templates[template]);
        }

        input.push(null);
      } catch (error) {
        return reject(error);
      }
    });
  }

  /**
   * Bulk update project templates.
   * @method
   * @name importProjectTemplate - import templates from existing project templates.
   * @param {String} templateId - project template id.
   * @param {String} userId - logged in user id.
   * @param {String} userToken - logged in user token.
   * @param {String} solutionId - solution id.
   * @param {Object} updateData - template update data.
   * @returns {Object} imported templates data.
   */

  static importProjectTemplate(
    templateId,
    userId,
    userToken,
    solutionId,
    updateData = {}
  ) {
    return new Promise(async (resolve, reject) => {
      try {
        let projectTemplateData = await projectTemplateQueries.templateDocument(
          {
            status: CONSTANTS.common.PUBLISHED,
            externalId: templateId,
            isReusable: true,
          }
        );

        if (!(projectTemplateData.length > 0)) {
          throw new Error(CONSTANTS.apiResponses.PROJECT_TEMPLATE_NOT_FOUND);
        }

        let newProjectTemplate = { ...projectTemplateData[0] };
        newProjectTemplate.externalId =
          projectTemplateData[0].externalId + "-" + UTILS.epochTime();
        newProjectTemplate.createdBy = newProjectTemplate.updatedBy = userId;

        let solutionData = await surveyService.listSolutions([solutionId]);

        if (!solutionData.success) {
          throw {
            message: CONSTANTS.apiResponses.SOLUTION_NOT_FOUND,
            status: HTTP_STATUS_CODE["bad_request"].status,
          };
        }

        if (
          solutionData.data[0].type !== CONSTANTS.common.IMPROVEMENT_PROJECT
        ) {
          throw {
            message:
              CONSTANTS.apiResponses.IMPROVEMENT_PROJECT_SOLUTION_NOT_FOUND,
            status: HTTP_STATUS_CODE["bad_request"].status,
          };
        }

        if (solutionData.data[0].projectTemplateId) {
          throw {
            message: CONSTANTS.apiResponses.PROJECT_TEMPLATE_EXISTS_IN_SOLUTION,
            status: HTTP_STATUS_CODE["bad_request"].status,
          };
        }

        if (
          projectTemplateData[0].entityType &&
          projectTemplateData[0].entityType !== "" &&
          projectTemplateData[0].entityType !== solutionData.data[0].entityType
        ) {
          throw {
            message: CONSTANTS.apiResponses.ENTITY_TYPE_MIS_MATCHED,
            status: HTTP_STATUS_CODE["bad_request"].status,
          };
        }

        newProjectTemplate.solutionId = solutionData.data[0]._id;
        newProjectTemplate.solutionExternalId = solutionData.data[0].externalId;
        newProjectTemplate.programId = solutionData.data[0].programId;
        newProjectTemplate.programExternalId =
          solutionData.data[0].programExternalId;

        newProjectTemplate.parentTemplateId = projectTemplateData[0]._id;

        let updationKeys = Object.keys(updateData);
        if (updationKeys.length > 0) {
          updationKeys.forEach((singleKey) => {
            if (newProjectTemplate[singleKey]) {
              newProjectTemplate[singleKey] = updateData[singleKey];
            }
          });
        }

        let tasksIds;

        if (projectTemplateData[0].tasks) {
          tasksIds = projectTemplateData[0].tasks;
        }

        newProjectTemplate.isReusable = false;

        let duplicateTemplateDocument =
          await projectTemplateQueries.createTemplate(
            _.omit(newProjectTemplate, ["_id"])
          );

        if (!duplicateTemplateDocument._id) {
          throw new Error(CONSTANTS.apiResponses.PROJECT_TEMPLATES_NOT_CREATED);
        }

        //duplicate task
        if (Array.isArray(tasksIds) && tasksIds.length > 0) {
          await this.duplicateTemplateTasks(
            tasksIds,
            duplicateTemplateDocument._id,
            duplicateTemplateDocument.externalId
          );
        }

        await surveyService.updateSolution(
          userToken,
          {
            projectTemplateId: duplicateTemplateDocument._id,
            name: duplicateTemplateDocument.title,
          },
          newProjectTemplate.solutionExternalId
        );

        await this.ratings(
          projectTemplateData[0]._id,
          updateData.rating,
          userToken
        );

        return resolve({
          success: true,
          message: CONSTANTS.apiResponses.DUPLICATE_PROJECT_TEMPLATES_CREATED,
          data: {
            _id: duplicateTemplateDocument._id,
          },
        });
      } catch (error) {
        return resolve({
          status: error.status
            ? error.status
            : HTTP_STATUS_CODE["internal_server_error"].status,
          success: false,
          message: error.message,
          data: {},
        });
      }
    });
  }

  /**
   * Create ratings.
   * @method
   * @name ratings
   * @param {String} templateId - project template id.
   * @param {String} rating - rating for template.
   * @returns {Object} rating object.
   */

  static ratings(templateId, rating, userToken) {
    return new Promise(async (resolve, reject) => {
      try {
        let userProfileData = await coreService.getProfile(userToken);

        if (!userProfileData.success) {
          throw {
            status: HTTP_STATUS_CODE["bad_request"].status,
            message: CONSTANTS.apiResponses.USER_PROFILE_NOT_FOUND,
          };
        }

        let templateData = await projectTemplateQueries.templateDocument(
          {
            status: CONSTANTS.common.PUBLISHED,
            _id: templateId,
            isReusable: true,
          },
          ["averageRating", "noOfRatings", "ratings"]
        );

        let updateRating = {
          ratings: { ...templateData[0].ratings },
        };

        updateRating.ratings[rating] += 1;

        let userCurrentRating = 0;
        let projectIndex = -1;

        if (
          userProfileData.data &&
          userProfileData.data.ratings &&
          userProfileData.data.ratings.length > 0
        ) {
          projectIndex = userProfileData.data.ratings.findIndex(
            (project) => project._id.toString() === templateId.toString()
          );

          if (!(projectIndex < 0)) {
            userCurrentRating =
              userProfileData.data.ratings[projectIndex].rating;
            updateRating.ratings[userCurrentRating] -= 1;
          }
        } else {
          userProfileData.data.ratings = [];
        }

        let ratingUpdated = {};

        if (userCurrentRating === rating) {
          ratingUpdated = templateData[0];
        } else {
          let calculateRating = _calculateRating(updateRating.ratings);
          updateRating.averageRating = calculateRating.averageRating;
          updateRating.noOfRatings = calculateRating.noOfRatings;

          ratingUpdated = await projectTemplateQueries.findOneAndUpdate(
            {
              _id: templateId,
            },
            {
              $set: updateRating,
            },
            {
              new: true,
            }
          );

          let improvementProjects = [...userProfileData.data.ratings];
          if (projectIndex >= 0) {
            improvementProjects[projectIndex].rating = rating;
          } else {
            improvementProjects.push({
              _id: ObjectId(templateId),
              externalId: ratingUpdated.externalId,
              rating: rating,
              type: CONSTANTS.common.IMPROVEMENT_PROJECT,
            });
          }

          await coreService.updateUserProfile(userToken, {
            ratings: improvementProjects,
          });
        }

        return resolve(
          _.pick(ratingUpdated, ["averageRating", "noOfRatings", "ratings"])
        );
      } catch (error) {
        return resolve({
          success: false,
          message: error.message,
          status: error.status
            ? error.status
            : HTTP_STATUS_CODE["internal_server_error"].status,
        });
      }
    });
  }

  /**
   * Project template tasks
   * @method
   * @name duplicateTemplateTasks
   * @param {Array} taskIds - Task ids
   * @returns {Object} Duplicated tasks.
   */

  static duplicateTemplateTasks(
    taskIds = [],
    duplicateTemplateId,
    duplicateTemplateExternalId
  ) {
    return new Promise(async (resolve, reject) => {
      try {
        let newTaskId = [];

        for (
          let pointerToTask = 0;
          pointerToTask < taskIds.length;
          pointerToTask++
        ) {
          let taskId = taskIds[pointerToTask];
          let taskData = await projectTemplateTaskQueries.taskDocuments({
            _id: taskId,
            parentId: { $exists: false },
          });

          if (taskData && taskData.length > 0) {
            taskData = taskData[0];
          }

          if (taskData && Object.keys(taskData).length > 0) {
            //duplicate parent task
            let newProjectTemplateTask = { ...taskData };
            newProjectTemplateTask.projectTemplateId = duplicateTemplateId;
            newProjectTemplateTask.projectTemplateExternalId =
              duplicateTemplateExternalId;
            newProjectTemplateTask.externalId =
              taskData.externalId + "-" + UTILS.epochTime();

            let duplicateTemplateTask =
              await database.models.projectTemplateTasks.create(
                _.omit(newProjectTemplateTask, ["_id"])
              );

            newTaskId.push(duplicateTemplateTask._id);

            //duplicate child task
            if (
              duplicateTemplateTask.children &&
              duplicateTemplateTask.children.length > 0
            ) {
              let childTaskIdArray = [];
              let childTaskIds = duplicateTemplateTask.children;

              if (childTaskIds && childTaskIds.length > 0) {
                for (
                  let pointerToChild = 0;
                  pointerToChild < childTaskIds.length;
                  pointerToChild++
                ) {
                  let childtaskId = childTaskIds[pointerToChild];
                  let childTaskData =
                    await projectTemplateTaskQueries.taskDocuments({
                      _id: childtaskId,
                    });

                  if (childTaskData && childTaskData.length > 0) {
                    childTaskData = childTaskData[0];
                  }

                  if (childTaskData && Object.keys(childTaskData).length > 0) {
                    let newProjectTemplateChildTask = { ...childTaskData };
                    newProjectTemplateChildTask.projectTemplateId =
                      duplicateTemplateId;
                    newProjectTemplateChildTask.projectTemplateExternalId =
                      duplicateTemplateExternalId;
                    newProjectTemplateChildTask.parentId =
                      duplicateTemplateTask._id;
                    newProjectTemplateChildTask.externalId =
                      childTaskData.externalId + "-" + UTILS.epochTime();

                    let duplicateChildTemplateTask =
                      await database.models.projectTemplateTasks.create(
                        _.omit(newProjectTemplateChildTask, ["_id"])
                      );

                    childTaskIdArray.push(duplicateChildTemplateTask._id);
                    newTaskId.push(duplicateChildTemplateTask._id);
                  }
                }
                //update new subtask ids to parent task
                if (childTaskIdArray && childTaskIdArray.length > 0) {
                  let updateTaskData =
                    await projectTemplateTaskQueries.updateTaskDocument(
                      {
                        _id: duplicateTemplateTask._id,
                      },
                      {
                        $set: {
                          children: childTaskIdArray,
                        },
                      }
                    );
                }
              }
            }
          }
        }

        let updateDuplicateTemplate;
        //adding duplicate tasj to duplicate template
        if (newTaskId && newTaskId.length > 0) {
          updateDuplicateTemplate =
            await projectTemplateQueries.findOneAndUpdate(
              {
                _id: duplicateTemplateId,
              },
              {
                $set: {
                  tasks: newTaskId,
                },
              }
            );
        }

        return resolve(updateDuplicateTemplate);
      } catch (error) {
        return reject(error);
      }
    });
  }

  /**
   * Templates list.
   * @method
   * @name listByIds
   * @param {Array} externalIds - External ids
   * @returns {Array} List of templates data.
   */

  static listByIds(externalIds) {
    return new Promise(async (resolve, reject) => {
      try {
        let templateData = await projectTemplateQueries.templateDocument(
          {
            externalId: { $in: externalIds },
          },
          ["title", "metaInformation.goal", "externalId"]
        );

        if (!(templateData.length > 0)) {
          throw {
            status: HTTP_STATUS_CODE.bad_request.status,
            message: CONSTANTS.apiResponses.PROJECT_TEMPLATE_NOT_FOUND,
          };
        }

        templateData = templateData.map((template) => {
          if (template.metaInformation && template.metaInformation.goal) {
            template.goal = template.metaInformation.goal;
            delete template.metaInformation;
          }

          return template;
        });

        return resolve({
          success: false,
          data: templateData,
          message: CONSTANTS.apiResponses.PROJECT_TEMPLATES_FETCHED,
        });
      } catch (error) {
        return reject(error);
      }
    });
  }

  /**
   * Template details.
   * @method
   * @name details
   * @param {String} templateId - Project template id.
   * @param {String} userId - logged in user id.
   * @params {String} link - solution link.
   * @returns {Array} Project templates data.
   */

  static details(templateId = "", link = "", userId = "", isAPrivateProgram) {
    return new Promise(async (resolve, reject) => {
      try {
        let solutionsResult = {};
        let findQuery = {};
        //get data when link is given
        if (link) {
          let queryData = {};
          queryData["link"] = link;

          let solutionDocument = await solutionsQueries.solutionsDocument(
            queryData,
            [
              "_id",
              "name",
              "programId",
              "programName",
              "projectTemplateId",
              "link",
            ]
          );

          if (!(solutionDocument.length > 0)) {
            throw {
              message: CONSTANTS.apiResponses.SOLUTION_NOT_FOUND,
              status: HTTP_STATUS_CODE["bad_request"].status,
            };
          }
          let solutiondata = solutionDocument;
          templateId = solutiondata[0].projectTemplateId;
          if (!templateId) {
            return resolve({
              success: false,
              data: solutiondata,
              message: CONSTANTS.apiResponses.TEMPLATE_ID_NOT_FOUND_IN_SOLUTION,
            });
          }
          solutionsResult = solutiondata;
          templateId = templateId.toString();
        }

        if (templateId) {
          let validateTemplateId = UTILS.isValidMongoId(templateId);
          if (validateTemplateId) {
            findQuery["_id"] = templateId;
          } else {
            findQuery["externalId"] = templateId;
          }
        }
        //getting template data using templateId

        let templateData = await projectTemplateQueries.templateDocument(
          findQuery,
          "all",
          [
            "ratings",
            "noOfRatings",
            "averageRating",
            "parentTemplateId",
            "userId",
            "createdBy",
            "updatedBy",
            "createdAt",
            "updatedAt",
            "__v",
          ]
        );

        if (!(templateData.length > 0)) {
          throw {
            status: HTTP_STATUS_CODE.bad_request.status,
            message: CONSTANTS.apiResponses.PROJECT_TEMPLATE_NOT_FOUND,
          };
        }
        if (
          templateData[0].certificateTemplateId &&
          templateData[0].certificateTemplateId !== ""
        ) {
          let certificateTemplateDetails =
            await certificateTemplateQueries.certificateTemplateDocument(
              {
                _id: templateData[0].certificateTemplateId,
              },
              ["criteria"]
            );

          //certificate template data do not exists.
          if (!(certificateTemplateDetails.length > 0)) {
            throw {
              message: CONSTANTS.apiResponses.CERTIFICATE_TEMPLATE_NOT_FOUND,
            };
          }
          templateData[0].criteria = certificateTemplateDetails[0].criteria;
        }

        if (templateData[0].tasks && templateData[0].tasks.length > 0) {
          templateData[0].tasks = await this.tasksAndSubTasks(
            templateData[0]._id
          );
        }
        let result = await _templateInformation(templateData[0]);
        if (!result.success) {
          return resolve(result);
        }

        if (!templateData[0].isReusable && userId !== "") {
          templateData[0].projectId = "";

          const projectIdQuery = {
            userId: userId,
            projectTemplateId: templateData[0]._id,
          };

          if (isAPrivateProgram !== "") {
            projectIdQuery.isAPrivateProgram = isAPrivateProgram;
          }
          let project = await projectQueries.projectDocument(projectIdQuery, [
            "_id",
            "hasAcceptedTAndC",
          ]);

          if (project && project.length > 0) {
            templateData[0].projectId = project[0]._id;
            templateData[0].hasAcceptedTAndC = project[0].hasAcceptedTAndC;
          }
        }
        if (!result.data.programInformation) {
          result.data.programInformation = {
            programId: solutionsResult.programId,
            programName: solutionsResult.programName,
          };
        }
        result.data.solutionInformation = {
          _id: solutionsResult._id,
          name: solutionsResult.name,
          link: solutionsResult.link,
        };
        return resolve({
          success: false,
          data: result.data,
          message: CONSTANTS.apiResponses.PROJECT_TEMPLATE_DETAILS_FETCHED,
        });
      } catch (error) {
        return reject(error);
      }
    });
  }

  /**
   * Tasks and sub tasks.
   * @method
   * @name tasksAndSubTasks
   * @param {Array} templateId - Template id.
   * @returns {Array} Tasks and sub task.
   */

  static tasksAndSubTasks(templateId) {
    return new Promise(async (resolve, reject) => {
      try {
        const templateDocument = await projectTemplateQueries.templateDocument(
          {
            _id: templateId,
            status: CONSTANTS.common.PUBLISHED,
          },
          ["tasks", "taskSequence"]
        );

        let tasks = [];

        if (
          templateDocument[0].taskSequence &&
          templateDocument[0].taskSequence.length > 0
        ) {
          let projectionKey = CONSTANTS.common.TASK_SEQUENCE;
          let findQuery = {
            externalId: {
              $in: templateDocument[0].taskSequence,
            },
          };

          tasks = await _taskAndSubTaskinSequence(findQuery, projectionKey);
        } else {
          if (
            templateDocument[0].tasks &&
            templateDocument[0].tasks.length > 0
          ) {
            let projectionKey = CONSTANTS.common.CHILDREN;
            if (templateDocument[0].tasks) {
              let findQuery = {
                _id: {
                  $in: templateDocument[0].tasks,
                },
                parentId: { $exists: false },
              };

              tasks = await _taskAndSubTaskinSequence(findQuery, projectionKey);
            }
          }
        }

        return resolve(tasks);
      } catch (error) {
        return reject(error);
      }
    });
  }

  /**
   * Template update.
   * @method
   * @name update
   * @param {String} templateId - Project template id.
   * @param {Object} templateData - template updation data
   * @param {String} userId - logged in user id.
   * @returns {Array} Project templates data.
   */

  static update(templateId, templateData, userId) {
    return new Promise(async (resolve, reject) => {
      try {
        let findQuery = {};

        let validateTemplateId = UTILS.isValidMongoId(templateId);

        if (validateTemplateId) {
          findQuery["_id"] = templateId;
        } else {
          findQuery["externalId"] = templateId;
        }

        let templateDocument = await projectTemplateQueries.templateDocument(
          findQuery,
          ["_id"]
        );

        if (!(templateDocument.length > 0)) {
          throw {
            status: HTTP_STATUS_CODE.bad_request.status,
            message: CONSTANTS.apiResponses.PROJECT_TEMPLATE_NOT_FOUND,
          };
        }

        let updateObject = {
          $set: {},
        };

        let templateUpdateData = templateData;

        Object.keys(templateUpdateData).forEach((updationData) => {
          updateObject["$set"][updationData] = templateUpdateData[updationData];
        });

        updateObject["$set"]["updatedBy"] = userId;

        let templateUpdatedData = await projectTemplateQueries.findOneAndUpdate(
          {
            _id: templateDocument[0]._id,
          },
          updateObject,
          { new: true }
        );

        if (!templateUpdatedData._id) {
          throw {
            message: CONSTANTS.apiResponses.PROJECT_TEMPLATE_NOT_UPDATED,
          };
        }

        return resolve({
          success: true,
          data: templateUpdatedData,
          message: CONSTANTS.apiResponses.PROJECT_TEMPLATE_UPDATED,
        });
      } catch (error) {
        return reject(error);
      }
    });
  }
};

/**
 * Calculate average rating and no of ratings.
 * @method
 * @name _calculateRating
 * @param {Object} ratings - Ratings data.
 * @returns {Object} rating object.
 */

function _calculateRating(ratings) {
  let sum = 0;
  let noOfRatings = 0;

  Object.keys(ratings).forEach((rating) => {
    sum += rating * ratings[rating];
    noOfRatings += ratings[rating];
  });

  return {
    averageRating: (sum / noOfRatings).toFixed(2),
    noOfRatings: noOfRatings,
  };
}

/**
 * Project information.
 * @method
 * @name _templateInformation
 * @param {Object} project - Project data.
 * @returns {Object} Project information.
 */

function _templateInformation(project) {
  return new Promise(async (resolve, reject) => {
    try {
      if (project.programId) {
        let programs = await surveyService.listProgramsBasedOnIds([
          project.programId,
        ]);

        if (!programs.success) {
          throw {
            message: CONSTANTS.apiResponses.PROGRAM_NOT_FOUND,
            status: HTTP_STATUS_CODE["bad_request"].status,
          };
        }

        project.programInformation = {
          programId: project.programId,
          programName: programs.data[0].name,
        };

        delete project.programId;
        delete project.programExternalId;
      }

      if (project.metaInformation) {
        Object.keys(project.metaInformation).forEach((projectMetaKey) => {
          project[projectMetaKey] = project.metaInformation[projectMetaKey];
        });
      }
      delete project.metaInformation;
      delete project.__v;

      project.status = project.status
        ? project.status
        : CONSTANTS.common.NOT_STARTED_STATUS;

      return resolve({
        success: true,
        data: project,
      });
    } catch (error) {
      return resolve({
        message: error.message,
        success: false,
        status: error.status
          ? error.status
          : HTTP_STATUS_CODE["internal_server_error"].status,
      });
    }
  });
}

/**
 * Task and SubTask In Order.
 * @method
 * @name _taskAndSubTaskinSequence
 * @param {Object} query - template Query.
 * @param {String} projectionValue - children or taskSequence.
 * @returns {Object} Task and SubTask information.
 */

function _taskAndSubTaskinSequence(query, projectionValue) {
  return new Promise(async (resolve, reject) => {
    try {
      let tasks = [];
      tasks = await projectTemplateTaskQueries.taskDocuments(query, "all", [
        "projectTemplateId",
        "__v",
        "projectTemplateExternalId",
      ]);

      for (let task = 0; task < tasks.length; task++) {
        if (
          tasks[task][projectionValue] &&
          tasks[task][projectionValue].length > 0
        ) {
          let subTaskQuery;
          if (projectionValue == CONSTANTS.common.CHILDREN) {
            subTaskQuery = {
              _id: {
                $in: tasks[task][projectionValue],
              },
            };
          } else {
            subTaskQuery = {
              externalId: {
                $in: tasks[task][projectionValue],
              },
            };
          }

          let subTasks = await projectTemplateTaskQueries.taskDocuments(
            subTaskQuery,
            "all",
            ["projectTemplateId", "__v", "projectTemplateExternalId"]
          );
          tasks[task].children = subTasks;
        }
      }

      return resolve(tasks);
    } catch (error) {
      return resolve({
        message: error.message,
        success: false,
        status: error.status
          ? error.status
          : HTTP_STATUS_CODE["internal_server_error"].status,
      });
    }
  });
}
