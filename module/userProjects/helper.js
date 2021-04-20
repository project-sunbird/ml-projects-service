/**
 * name : helper.js
 * author : Aman
 * created-date : 16-July-2020
 * Description : Projects helper functionality.
 */

// Dependencies

const kendraService = require(GENERICS_FILES_PATH + "/services/kendra");
const libraryCategoriesHelper = require(MODULES_BASE_PATH + "/library/categories/helper");
const projectTemplatesHelper = require(MODULES_BASE_PATH + "/project/templates/helper");
const projectTemplateTasksHelper = require(MODULES_BASE_PATH + "/project/templateTasks/helper");
const { v4: uuidv4 } = require('uuid');
const assessmentService = require(GENERICS_FILES_PATH + "/services/assessment");
const dhitiService = require(GENERICS_FILES_PATH + "/services/dhiti");

/**
    * UserProjectsHelper
    * @class
*/

module.exports = class UserProjectsHelper {

    /**
     * Lists of projects.
     * @method
     * @name projectDocument
     * @param {Array} [filterData = "all"] - project filter query.
     * @param {Array} [fieldsArray = "all"] - projected fields.
     * @param {Array} [skipFields = "none"] - field not to include
     * @returns {Array} Lists of projects. 
     */

    static projectDocument(
        filterData = "all",
        fieldsArray = "all",
        skipFields = "none"
    ) {
        return new Promise(async (resolve, reject) => {
            try {

                let queryObject = (filterData != "all") ? filterData : {};
                let projection = {}

                if (fieldsArray != "all") {
                    fieldsArray.forEach(field => {
                        projection[field] = 1;
                    });
                }

                if (skipFields !== "none") {
                    skipFields.forEach(field => {
                        projection[field] = 0;
                    });
                }

                let templates =
                    await database.models.projects.find(
                        queryObject,
                        projection
                    ).lean();

                return resolve(templates);

            } catch (error) {
                return reject(error);
            }
        });
    }

    /**
      * List of projects.
      * @method
      * @name list
      * @param {String} userId - logged in user id.
      * @param {Boolean} updateLastDownloadedAt - update last downloaded at. 
      * @returns {Object} Projects.
     */

    static list(userId, updateLastDownloadedAt) {
        return new Promise(async (resolve, reject) => {
            try {

                let projects =
                    await this.projectDocument(
                        {
                            userId: userId,
                            isDeleted: false
                        }, "all", [
                        "createdBy",
                        "updatedBy",
                        "rootOrganisations",
                        "taskReport",
                        "createdFor",
                        "projectTemplateId",
                        "projectTemplateExternalId",
                        "__v"
                    ]);

                if (!projects.length > 0) {

                    throw {
                        status: HTTP_STATUS_CODE['ok'].status,
                        message: CONSTANTS.apiResponses.PROJECT_NOT_FOUND
                    };
                }

                let projectIds = [];

                let updatedDate = "";

                if (updateLastDownloadedAt) {
                    updatedDate = new Date();
                }

                for (let project = 0; project < projects.length; project++) {
                    let projectInformation = await _projectInformation(projects[project]);

                    if (!projectInformation.success) {
                        return resolve(projectInformation);
                    }

                    if (updatedDate !== "") {
                        projectInformation.data.lastDownloadedAt = updatedDate;
                    }

                    projectIds.push(projectInformation.data._id);
                }

                if (updatedDate !== "") {
                    await database.models.projects.updateMany({
                        _id: { $in: projectIds }
                    }, {
                        $set: {
                            lastDownloadedAt: updatedDate
                        }
                    });
                }

                return resolve({
                    success: true,
                    message: CONSTANTS.apiResponses.PROJECTS_FETCHED,
                    data: projects
                });

            } catch (error) {
                return resolve({
                    status: error.status ? error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                    success: false,
                    message: error.message,
                    data: []
                });
            }
        })
    }

    /**
    * List of projects meta form.
    * @method
    * @name metaForm
    * @returns {Object} List of projects meta form.
   */

    static metaForm() {
        return new Promise(async (resolve, reject) => {
            try {

                let forms = await kendraService.formDetails("projects");

                if (!forms.success) {

                    throw {
                        status: HTTP_STATUS_CODE['ok'].status,
                        message: CONSTANTS.apiResponses.PROJECTS_FORM_NOT_FOUND
                    };

                }

                let categoriesData =
                    await libraryCategoriesHelper.categoryDocuments({}, ["name", "externalId"]);

                if (!categoriesData.length > 0) {

                    throw {
                        status: HTTP_STATUS_CODE['bad_request'].status,
                        message: CONSTANTS.apiResponses.LIBRARY_CATEGORIES_NOT_FOUND
                    };

                }

                categoriesData = categoriesData.map(category => {
                    return {
                        _id: category._id,
                        label: category.name,
                        value: category.externalId
                    };
                });

                categoriesData.push({
                    _id: "",
                    label: CONSTANTS.common.OTHERS,
                    value: CONSTANTS.common.OTHERS.toLowerCase()
                });

                let formsData = forms.data;

                formsData[formsData.length - 1].options = categoriesData;

                return resolve({
                    success: true,
                    message: CONSTANTS.apiResponses.PROJECTS_METAFORM_FETCHED,
                    data: formsData
                });

            } catch (error) {
                return resolve({
                    status:
                        error.status ?
                            error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                    success: false,
                    message: error.message,
                    data: []
                });
            }
        })
    }

    /**
      * List of projects tasks meta form.
      * @method
      * @name tasksMetaForm
      * @returns {Object} List of projects tasks meta form.
     */

    static tasksMetaForm() {
        return new Promise(async (resolve, reject) => {
            try {

                let forms = await kendraService.formDetails("projectTasks");

                if (!forms.success) {

                    throw {
                        status: HTTP_STATUS_CODE['ok'].status,
                        message: CONSTANTS.apiResponses.PROJECT_TASKS_FORM_NOT_FOUND
                    }
                }

                return resolve({
                    success: true,
                    message: CONSTANTS.apiResponses.PROJECT_TASKS_METAFORM_FETCHED,
                    data: forms.data
                });

            } catch (error) {
                return resolve({
                    status:
                        error.status ?
                            error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                    success: false,
                    message: error.message,
                    data: []
                });
            }
        })
    }

    /**
      * Bulk create user projects.
      * @method
      * @name bulkCreate - Bulk create user projects.
      * @param {Array} csvData - csv data.
      * @param {String} userToken - logged in user token.
      * @returns {Object}  Bulk create user projects.
     */

    static bulkCreate(csvData, userToken) {
        return new Promise(async (resolve, reject) => {
            try {

                const fileName = `bulk-create-user-`;
                let fileStream = new CSV_FILE_STREAM(fileName);
                let input = fileStream.initStream();

                (async function () {
                    await fileStream.getProcessorPromise();
                    return resolve({
                        isResponseAStream: true,
                        fileNameWithPath: fileStream.fileNameWithPath()
                    });
                })();

                let templateIds = [];
                let entityIds = [];

                csvData.forEach(data => {
                    templateIds.push(data.templateId);

                    if (data.entityId && data.entityId !== "") {
                        entityIds.push(data.entityId);
                    }
                });

                let entityDocument = {};

                if (entityIds.length > 0) {

                    const entitiesData = await _entitiesInformation(entityIds);

                    if (!entitiesData.success) {
                        return resolve(entitiesData);
                    }

                    entityDocument =
                        entitiesData.data.reduce((ac, entity) => ({ ...ac, [entity._id.toString()]: entity }), {});
                }

                let templateData = {};
                let solutionIds = [];
                let programIds = [];

                if (templateIds.length > 0) {

                    const projectTemplates =
                        await projectTemplatesHelper.templateDocument({
                            status: CONSTANTS.common.PUBLISHED,
                            externalId: {
                                $in: templateIds
                            },
                            isReusable: false
                        }, ["_id", "solutionExternalId", "programExternalId", "externalId", "programId"]);

                    if (!projectTemplates.length > 0) {
                        throw {
                            message: CONSTANTS.apiResponses.PROJECT_TEMPLATE_NOT_FOUND,
                            status: HTTP_STATUS_CODE['bad_request'].status
                        }
                    }

                    projectTemplates.forEach(template => {

                        templateData[template.externalId] = template;

                        if (template.solutionExternalId) {
                            solutionIds.push(template.solutionExternalId);
                        }

                        if (template.programId) {
                            programIds.push(template.programId);
                        }

                    })

                }

                let solutions = {};

                if (solutionIds.length > 0) {

                    let solutionData =
                        await assessmentService.listSolutions(solutionIds);

                    if (!solutionData.success) {
                        throw {
                            message: CONSTANTS.apiResponses.SOLUTION_NOT_FOUND,
                            status: HTTP_STATUS_CODE['bad_request'].status
                        }
                    }

                    if (
                        solutionData.data && solutionData.data.length > 0
                    ) {
                        solutionData.data.forEach(solution => {
                            solutions[solution.externalId] = {
                                "name": solution.name,
                                "externalId": solution.externalId,
                                "description": solution.description,
                                "_id": solution._id
                            };
                        })
                    }

                }

                let programs = {};

                if (programIds.length > 0) {

                    let programData =
                        await assessmentService.listProgramsBasedOnIds(programIds);

                    if (!programData.success) {
                        throw {
                            message: CONSTANTS.apiResponses.PROGRAM_NOT_FOUND,
                            status: HTTP_STATUS_CODE['bad_request'].status
                        }
                    }

                    if (programData.data && programData.data.length > 0) {

                        programData.data.forEach(program => {
                            programs[program.externalId] = {
                                _id: program._id,
                                name: program.name,
                                description: program.description ? program.description : "",
                                externalId: program.externalId
                            };
                        })
                    }
                }

                for (
                    let pointerToCsvData = 0;
                    pointerToCsvData < csvData.length;
                    pointerToCsvData++
                ) {

                    let currentCsvData = csvData[pointerToCsvData];

                    if (!templateData[currentCsvData.templateId]) {
                        currentCsvData["STATUS"] =
                            CONSTANTS.apiResponses.PROJECT_TEMPLATE_NOT_FOUND;
                        input.push(currentCsvData);
                        continue;
                    }

                    let currentTemplateData = templateData[currentCsvData.templateId];

                    let projectCreation =
                        await this.userAssignedProjectCreation(
                            currentTemplateData._id,
                            currentCsvData["keycloak-userId"],
                            userToken
                        );

                    if (!projectCreation.success) {
                        return resolve(projectCreation);
                    }

                    let solutionInformation = {};

                    if (currentTemplateData.solutionExternalId) {

                        if (!solutions[currentTemplateData.solutionExternalId]) {

                            currentCsvData["STATUS"] =
                                CONSTANTS.apiResponses.SOLUTION_NOT_FOUND;
                            input.push(currentCsvData);
                            continue;
                        }

                        solutionInformation = solutions[currentTemplateData.solutionExternalId];
                        projectCreation.data.solutionInformation = solutionInformation;
                    }

                    if (currentTemplateData.programExternalId) {

                        if (!programs[currentTemplateData.programExternalId]) {
                            currentCsvData["STATUS"] =
                                CONSTANTS.apiResponses.PROGRAM_NOT_FOUND;
                            input.push(currentCsvData);
                            continue;
                        }

                        projectCreation.data.programInformation =
                            programs[currentTemplateData.programExternalId];

                        projectCreation.data.isAPrivateProgram =
                            programs[currentTemplateData.programExternalId].isAPrivateProgram;
                    }

                    if (
                        projectCreation.data.assesmentOrObservationTask &&
                        !currentCsvData.entityId
                    ) {
                        currentCsvData["STATUS"] =
                            CONSTANTS.apiResponses.ENTITY_REQUIRED_FOR_ASSESSMENT_OBSERVATION;
                        input.push(currentCsvData);
                        continue;
                    }

                    if (currentCsvData.entityId) {

                        if (!entityDocument[currentCsvData.entityId]) {
                            currentCsvData["STATUS"] =
                                CONSTANTS.apiResponses.ENTITIES_NOT_FOUND;
                            input.push(currentCsvData);
                            continue;
                        }

                        if (
                            currentTemplateData.entityType && currentTemplateData.entityType !== "" &&
                            currentTemplateData.entityType !== entityDocument[currentCsvData.entityId].entityType
                        ) {
                            currentCsvData["STATUS"] =
                                CONSTANTS.apiResponses.ENTITY_TYPE_MIS_MATCHED;
                            input.push(currentCsvData);
                            continue;
                        }

                        let entities = [];

                        if (
                            solutionInformation.entities &&
                            solutionInformation.entities.length > 0
                        ) {

                            let entityIndex =
                                solutionInformation.entities.findIndex(entity => entity._id === csvData[pointerToCsvData].entityId);

                            if (entityIndex < 0) {

                                entities =
                                    solutionInformation.entities.push(ObjectId(currentCsvData.entityId))
                            }
                        } else {
                            entities = [ObjectId(currentCsvData.entityId)];
                        }

                        if (entities.length > 0) {

                            let solutionUpdated =
                                await assessmentService.updateSolution(
                                    userToken,
                                    {
                                        entities: entities
                                    },
                                    solutionInformation.externalId
                                );

                            if (!solutionUpdated.success) {
                                throw {
                                    status: HTTP_STATUS_CODE['bad_request'].status,
                                    message: CONSTANTS.apiResponses.SOLUTION_NOT_UPDATED
                                }
                            }
                        }

                        projectCreation.data.entityId = ObjectId(currentCsvData.entityId);

                        projectCreation.data.entityInformation =
                            entityDocument[currentCsvData.entityId];
                    }

                    projectCreation.data.status = CONSTANTS.common.NOT_STARTED_STATUS;
                    projectCreation.data.lastDownloadedAt = new Date();
                    const project =
                        await database.models.projects.create(projectCreation.data);

                    if (project._id) {
                        currentCsvData["STATUS"] = project._id;
                    } else {
                        currentCsvData["STATUS"] = CONSTANTS.apiResponses.PROJECT_NOT_CREATED;
                    }

                    input.push(currentCsvData);
                }

                input.push(null);

            } catch (error) {
                return reject(error);
            }
        })
    }

    /**
      * Projects boolean data.
      * @method
      * @name booleanData     
      * @returns {Array} Boolean data.
    */

    static booleanData() {

        const projectsSchema = schemas["projects"].schema;
        const projectSchemaKey = Object.keys(projectsSchema);

        let booleanProjects = [];

        projectSchemaKey.forEach(projectSchema => {
            const currentSchema = projectsSchema[projectSchema];

            if (
                currentSchema.hasOwnProperty('default') &&
                typeof currentSchema.default === "boolean"
            ) {
                booleanProjects.push(projectSchema);
            }
        });

        return booleanProjects;
    }

    /**
      * Projects object id field.
      * @method
      * @name mongooseIdData     
      * @returns {Array} Projects object id field.
    */

    static mongooseIdData() {

        const projectsSchema = schemas["projects"].schema;
        const projectSchemaKey = Object.keys(projectsSchema);

        let mongooseIds = [];

        projectSchemaKey.forEach(projectSchema => {

            const currentSchemaType = projectsSchema[projectSchema];

            if (currentSchemaType === "ObjectId") {
                mongooseIds.push(projectSchema);
            }
        });

        return mongooseIds;
    }

    /**
      * Sync project.
      * @method
      * @name importFromLibrary 
      * @param {String} projectTemplateId - project template id.
      * @param {Object} requestedData - body data.
      * @param {String} userId - Logged in user id.
      * @param {String} userToken - User token.
      * @returns {Object} Project created information.
     */

    static importFromLibrary(projectTemplateId, requestedData, userToken, userId) {
        return new Promise(async (resolve, reject) => {
            try {

                let libraryProjects =
                    await libraryCategoriesHelper.projectDetails(
                        projectTemplateId
                    );

                if (
                    libraryProjects.data &&
                    !Object.keys(libraryProjects.data).length > 0
                ) {
                    throw {
                        message: CONSTANTS.apiResponses.PROJECT_TEMPLATE_NOT_FOUND,
                        status: HTTP_STATUS_CODE['bad_request'].status
                    };
                }

                let taskReport = {};

                if (
                    libraryProjects.data.tasks &&
                    libraryProjects.data.tasks.length > 0
                ) {

                    libraryProjects.data.tasks = await _projectTask(
                        libraryProjects.data.tasks,
                        true
                    );

                    taskReport.total = libraryProjects.data.tasks.length;

                    libraryProjects.data.tasks.forEach(task => {
                        if (!taskReport[task.status]) {
                            taskReport[task.status] = 1;
                        } else {
                            taskReport[task.status] += 1;
                        }
                    });

                    libraryProjects.data["taskReport"] = taskReport;
                }

                if (requestedData.entityId && requestedData.entityId !== "") {

                    let entityInformation =
                        await _entitiesInformation([requestedData.entityId]);

                    if (!entityInformation.success) {
                        return resolve(entityInformation);
                    }

                    libraryProjects.data["entityInformation"] = entityInformation.data[0];
                    libraryProjects.data.entityId = entityInformation.data[0]._id;
                }

                if (
                    (requestedData.programId && requestedData.programId !== "") ||
                    (requestedData.programName && requestedData.programName !== "")
                ) {

                    let programAndSolutionInformation =
                        await this.createProgramAndSolution(
                            requestedData.programId,
                            requestedData.programName,
                            requestedData.entityId ? [requestedData.entityId] : "",
                            userToken
                        );

                    if (!programAndSolutionInformation.success) {
                        return resolve(programAndSolutionInformation);
                    }

                    if (
                        libraryProjects.data["entityInformation"] &&
                        libraryProjects.data["entityInformation"].entityType !==
                        programAndSolutionInformation.data.solutionInformation.entityType
                    ) {
                        throw {
                            message: CONSTANTS.apiResponses.ENTITY_TYPE_MIS_MATCHED,
                            status: HTTP_STATUS_CODE['bad_request'].status
                        }
                    }

                    libraryProjects.data = _.merge(
                        libraryProjects.data,
                        programAndSolutionInformation.data
                    )
                }

                let userOrganisations =
                    await kendraService.getUserOrganisationsAndRootOrganisations(
                        userToken
                    );

                if (!userOrganisations.success) {
                    throw {
                        message: CONSTANTS.apiResponses.USER_ORGANISATION_NOT_FOUND,
                        status: HTTP_STATUS_CODE['bad_request'].status
                    }
                }

                libraryProjects.data.createdFor = userOrganisations.data.createdFor;
                libraryProjects.data.rootOrganisations = userOrganisations.data.rootOrganisations;

                libraryProjects.data.userId = libraryProjects.data.updatedBy = libraryProjects.data.createdBy = userId;
                libraryProjects.data.lastDownloadedAt = new Date();
                libraryProjects.data.status = CONSTANTS.common.NOT_STARTED_STATUS;

                if (requestedData.startDate) {
                    libraryProjects.data.startDate = requestedData.startDate;
                }

                if (requestedData.endDate) {
                    libraryProjects.data.endDate = requestedData.endDate;
                }

                libraryProjects.data.projectTemplateId = libraryProjects.data._id;
                libraryProjects.data.projectTemplateExternalId = libraryProjects.data.externalId;

                let projectCreation = await database.models.projects.create(
                    _.omit(libraryProjects.data, ["_id"])
                );

                if (requestedData.rating && requestedData.rating > 0) {
                    await projectTemplatesHelper.ratings(
                        projectTemplateId,
                        requestedData.rating,
                        userToken
                    );
                }

                projectCreation = await _projectInformation(projectCreation._doc);

                return resolve({
                    success: true,
                    message: CONSTANTS.apiResponses.PROJECTS_FETCHED,
                    data: projectCreation.data
                });

            } catch (error) {
                return resolve({
                    success: false,
                    message: error.message,
                    data: {}
                });
            }
        })
    }

    /**
      * Create user projects.
      * @method
      * @name create 
      * @param userId - Logged in user id.
      * @param userToken - Logged in user token.
      * @returns {Object} Return _id and lastDownloadedAt 
     */

    static create(userId, userToken) {
        return new Promise(async (resolve, reject) => {
            try {

                let creationData = {
                    lastDownloadedAt: new Date(),
                    createdFor: [],
                    rootOrganisations: []
                }

                creationData["userId"] = creationData["createdBy"] = creationData["updatedBy"] = userId;

                let userOrganisations =
                    await kendraService.getUserOrganisationsAndRootOrganisations(
                        userToken
                    );

                if (!userOrganisations.success) {
                    throw {
                        message: CONSTANTS.apiResponses.USER_ORGANISATION_NOT_FOUND,
                        status: HTTP_STATUS_CODE['bad_request'].status
                    }
                }

                if (userOrganisations.data) {
                    creationData.createdFor = userOrganisations.data.createdFor;
                    creationData.rootOrganisations = userOrganisations.data.rootOrganisations;
                }

                let userProject = await database.models.projects.create(
                    creationData
                );

                return resolve({
                    success: true,
                    message: CONSTANTS.apiResponses.CREATED_USER_PROJECT,
                    data: _.pick(userProject, ["_id", "lastDownloadedAt"])
                });

            } catch (error) {
                return resolve({
                    status:
                        error.status ?
                            error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                    success: false,
                    message: error.message,
                    data: {}
                });
            }
        })
    }

    /**
      * Sync project.
      * @method
      * @name sync 
      * @param {String} projectId - id of the project.
      * @param {String} lastDownloadedAt - last downloaded at time.
      * @param {Object} data - body data.
      * @param {String} userId - Logged in user id.
      * @param {String} userToken - User token.
      * @param {String} [appName = ""] - App Name.
      * @param {String} [appVersion = ""] - App Version.
      * @returns {Object} Project created information.
    */

    static sync(projectId, lastDownloadedAt, data, userId, userToken, appName = "", appVersion = "") {
        return new Promise(async (resolve, reject) => {
            try {

                const userProject = await this.projectDocument({
                    _id: projectId,
                    userId: userId
                }, [
                    "_id",
                    "tasks",
                    "programInformation._id",
                    "solutionInformation._id",
                    "solutionInformation.externalId",
                    "entityInformation._id",
                    "lastDownloadedAt",
                    "appInformation"
                ]);

                if (!userProject.length > 0) {

                    throw {
                        status: HTTP_STATUS_CODE['bad_request'].status,
                        message: CONSTANTS.apiResponses.USER_PROJECT_NOT_FOUND
                    };
                }

                if (userProject[0].lastDownloadedAt.toISOString() !== lastDownloadedAt) {
                    throw {
                        status: HTTP_STATUS_CODE['bad_request'].status,
                        message: CONSTANTS.apiResponses.USER_ALREADY_SYNC
                    };
                }

                const projectsModel = Object.keys(schemas["projects"].schema);

                let updateProject = {};
                let projectData = await _projectData(data);
                if (projectData && projectData.success == true) {
                    updateProject = _.merge(updateProject, projectData.data);
                }

                let createNewProgramAndSolution = false;
                let solutionExists = false;

                if (data.programId && data.programId !== "") {

                    // Check if program already existed in project and if its not an existing program.

                    if (!userProject[0].programInformation) {
                        createNewProgramAndSolution = true;
                    } else if (
                        userProject[0].programInformation &&
                        userProject[0].programInformation._id &&
                        userProject[0].programInformation._id.toString() !== data.programId
                    ) {
                        // Not an existing program.

                        solutionExists = true;
                    }

                } else if (data.programName) {

                    if (!userProject[0].solutionInformation) {
                        createNewProgramAndSolution = true;
                    } else {
                        solutionExists = true;
                        // create new program using current name and add existing solution and remove program from it.
                    }
                }

                let addOrUpdateEntityToProject = false;

                if (data.entityId) {

                    // If entity is not present in project or new entity is updated.
                    if (
                        !userProject[0].entityInformation ||
                        (
                            userProject[0].entityInformation &&
                            userProject[0].entityInformation._id !== data.entityId
                        )
                    ) {
                        addOrUpdateEntityToProject = true;
                    }
                }

                if (addOrUpdateEntityToProject) {

                    let entityInformation =
                        await _entitiesInformation([data.entityId]);

                    if (!entityInformation.success) {
                        return resolve(entityInformation);
                    }

                    updateProject["entityInformation"] = entityInformation.data[0];
                    updateProject.entityId = entityInformation.data[0]._id;
                }

                if (createNewProgramAndSolution || solutionExists) {

                    let programAndSolutionInformation =
                        await this.createProgramAndSolution(
                            data.programId,
                            data.programName,
                            updateProject.entityId ? [updateProject.entityId] : "",
                            userToken,
                            userProject[0].solutionInformation && userProject[0].solutionInformation._id ?
                                userProject[0].solutionInformation._id : ""
                        );

                    if (!programAndSolutionInformation.success) {
                        return resolve(programAndSolutionInformation);
                    }

                    if (solutionExists) {

                        let updateProgram =
                            await assessmentService.removeSolutionsFromProgram(
                                userToken,
                                userProject[0].programInformation._id,
                                [userProject[0].solutionInformation._id]
                            );

                        if (!updateProgram.success) {
                            throw {
                                status: HTTP_STATUS_CODE['bad_request'].status,
                                message: CONSTANTS.apiResponses.PROGRAM_NOT_UPDATED
                            }
                        }
                    }

                    updateProject =
                        _.merge(updateProject, programAndSolutionInformation.data);
                }

                let booleanData = this.booleanData(schemas["projects"].schema);
                let mongooseIdData = this.mongooseIdData(schemas["projects"].schema);

                if (data.tasks) {

                    let taskReport = {};

                    updateProject.tasks = await _projectTask(
                        data.tasks
                    );

                    if (
                        userProject[0].tasks &&
                        userProject[0].tasks.length > 0
                    ) {

                        updateProject.tasks.forEach(task => {

                            task.updatedBy = userId;
                            task.updatedAt = new Date();

                            let taskIndex =
                                userProject[0].tasks.findIndex(
                                    projectTask => projectTask._id === task._id
                                );

                            if (taskIndex < 0) {
                                userProject[0].tasks.push(
                                    task
                                );
                            } else {
                                userProject[0].tasks[taskIndex] = task;
                            }
                        });

                        updateProject.tasks = userProject[0].tasks;
                    }

                    taskReport.total = updateProject.tasks.length;

                    updateProject.tasks.forEach(task => {
                        if (!taskReport[task.status]) {
                            taskReport[task.status] = 1;
                        } else {
                            taskReport[task.status] += 1;
                        }
                    });

                    updateProject["taskReport"] = taskReport;
                }

                Object.keys(data).forEach(updateData => {
                    if (
                        !updateProject[updateData] &&
                        projectsModel.includes(updateData)
                    ) {

                        if (booleanData.includes(updateData)) {

                            updateProject[updateData] =
                                UTILS.convertStringToBoolean(data[updateData]);

                        } else if (mongooseIdData.includes(updateData)) {
                            updateProject[updateData] = ObjectId(data[updateData]);
                        } else {
                            updateProject[updateData] = data[updateData];
                        }
                    }
                });

                updateProject.updatedBy = userId;
                updateProject.updatedAt = new Date();

                if (!userProject[0].appInformation) {
                    updateProject["appInformation"] = {};

                    if (appName !== "") {
                        updateProject["appInformation"]["appName"] = appName;
                    }

                    if (appVersion !== "") {
                        updateProject["appInformation"]["appVersion"] = appVersion;
                    }
                }

                let projectUpdated =
                    await database.models.projects.findOneAndUpdate(
                        {
                            _id: userProject[0]._id
                        },
                        {
                            $set: updateProject
                        }, {
                        new: true
                    }
                    );

                if (!projectUpdated._id) {
                    throw {
                        message: CONSTANTS.apiResponses.USER_PROJECT_NOT_UPDATED,
                        status: HTTP_STATUS_CODE['bad_request'].status
                    }
                }

                return resolve({
                    success: true,
                    message: CONSTANTS.apiResponses.USER_PROJECT_UPDATED,
                    data : {
                        programId : 
                        projectUpdated.programInformation && projectUpdated.programInformation._id ?
                        projectUpdated.programInformation._id : "",
                        hasAcceptedTAndC : projectUpdated.hasAcceptedTAndC ? projectUpdated.hasAcceptedTAndC : false
                    } 
                });

            } catch (error) {
                return resolve({
                    status:
                        error.status ?
                            error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                    success: false,
                    message: error.message,
                    data: {}
                });
            }
        })
    }

    /**
     * Program and solution information
     * @method
     * @name createProgramAndSolution 
     * @param {String} entityId - entity id.
     * @param {String} userToken - Logged in user token.
     * @param {String} [ programId = "" ] - Program Id.
     * @param {String} [ programName = "" ] - Program Name.
     * @returns {Object} Created program and solution data.
    */

    static createProgramAndSolution(
        programId = "",
        programName = "",
        entities,
        userToken,
        solutionId
    ) {
        return new Promise(async (resolve, reject) => {
            try {

                let result = {};

                let programAndSolutionData = {
                    type: CONSTANTS.common.IMPROVEMENT_PROJECT,
                    subType: CONSTANTS.common.IMPROVEMENT_PROJECT,
                    isReusable: false,
                    solutionId: solutionId,
                    entities: entities
                };

                if (programName !== "") {
                    programAndSolutionData["programName"] = programName;
                }

                if (programId !== "") {
                    programAndSolutionData["programId"] = programId;
                }

                let solutionAndProgramCreation =
                    await kendraService.createUserProgramAndSolution(
                        programAndSolutionData,
                        userToken
                    );

                if (!solutionAndProgramCreation.success) {
                    throw {
                        status: HTTP_STATUS_CODE['bad_request'].status,
                        message: CONSTANTS.apiResponses.SOLUTION_PROGRAMS_NOT_CREATED
                    };
                }

                result.solutionInformation = _.pick(
                    solutionAndProgramCreation.data.solution,
                    ["name", "externalId", "description", "_id", "entityType"]
                );

                result.solutionInformation._id =
                    ObjectId(result.solutionInformation._id);

                result["solutionId"] = ObjectId(result.solutionInformation._id);
                result["solutionExternalId"] = result.solutionInformation.externalId;

                result.programInformation = _.pick(
                    solutionAndProgramCreation.data.program,
                    ["_id", "name", "externalId", "description", "isAPrivateProgram"]
                );

                result["programId"] = ObjectId(result.programInformation._id);
                result["programExternalId"] = result.programInformation.externalId;
                result["isAPrivateProgram"] = result.programInformation.isAPrivateProgram;

                result.programInformation._id =
                    ObjectId(result.programInformation._id);

                return resolve({
                    success: true,
                    data: result
                });

            } catch (error) {
                return resolve({
                    status:
                        error.status ?
                            error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                    success: false,
                    message: error.message,
                    data: {}
                });
            }
        });
    }

    /**
     * Project details.
     * @method
     * @name details 
     * @param {String} projectId - project id.
     * @returns {Object} 
    */

    static details(projectId, userId) {
        return new Promise(async (resolve, reject) => {
            try {

                const projectDetails = await this.projectDocument({
                    _id: projectId,
                    userId: userId
                }, "all",
                    [
                        "taskReport",
                        "createdFor",
                        "rootOrganisations",
                        "projectTemplateId",
                        "projectTemplateExternalId",
                        "userId",
                        "createdBy",
                        "updatedBy",
                        "createdAt",
                        "updatedAt",
                        "__v"
                    ]);

                if (!projectDetails.length > 0) {

                    throw {
                        status: HTTP_STATUS_CODE["bad_request"].status,
                        message: CONSTANTS.apiResponses.PROJECT_NOT_FOUND
                    }
                }

                let result = await _projectInformation(projectDetails[0]);

                if (!result.success) {
                    return resolve(projectInformation);
                }

                return resolve({
                    success: true,
                    message: CONSTANTS.apiResponses.PROJECT_DETAILS_FETCHED,
                    data: result.data
                });

            } catch (error) {
                return resolve({
                    status:
                        error.status ?
                            error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                    success: false,
                    message: error.message,
                    data: []
                });
            }
        })
    }

    /**
      * List of library projects.
      * @method
      * @name projects
      * @param pageSize - Size of page.
      * @param pageNo - Recent page no.
      * @param search - search text.
      * @param fieldsArray - array of projections fields.
      * @param groupBy - groupBy query.
      * @returns {Object} List of library projects.
     */

    static projects(query, pageSize, pageNo, searchQuery, fieldsArray, groupBy = "") {
        return new Promise(async (resolve, reject) => {
            try {

                let matchQuery = {
                    $match: query
                };

                if (searchQuery && searchQuery.length > 0) {
                    matchQuery["$match"]["$or"] = searchQuery;
                }

                let projection = {}
                fieldsArray.forEach(field => {
                    projection[field] = 1;
                });

                let aggregateData = [];
                aggregateData.push(matchQuery);
                aggregateData.push({
                    $sort : { "syncedAt" : -1 }
                })

                if (groupBy !== "") {
                    aggregateData.push({
                        $group: groupBy
                    });
                } else {
                    aggregateData.push({
                        $project: projection
                    });
                }

                aggregateData.push(
                    {
                        $facet: {
                            "totalCount": [
                                { "$count": "count" }
                            ],
                            "data": [
                                { $skip: pageSize * (pageNo - 1) },
                                { $limit: pageSize }
                            ],
                        }
                    },
                    {
                        $project: {
                            "data": 1,
                            "count": {
                                $arrayElemAt: ["$totalCount.count", 0]
                            }
                        }
                    }
                );

                let result =
                    await database.models.projects.aggregate(aggregateData);

                return resolve({
                    success: true,
                    message: CONSTANTS.apiResponses.PROJECTS_FETCHED,
                    data: {
                        data: result[0].data,
                        count: result[0].count ? result[0].count : 0
                    }
                })

            } catch (error) {
                return resolve({
                    success: false,
                    message: error.message,
                    data: {
                        data: [],
                        count: 0
                    }
                });
            }
        })
    }

    /**
    * Get tasks from a user project.
    * @method
    * @name tasks 
    * @param {String} projectId - Project id. 
    * @param {Array} taskIds - Array of tasks ids.
    * @returns {Object} - return tasks from a project. 
  */

    static tasks(projectId, taskIds) {
        return new Promise(async (resolve, reject) => {
            try {

                let aggregatedData = [{
                    $match: {
                        _id: ObjectId(projectId)
                    }
                }];

                if (taskIds.length > 0) {

                    let unwindData = {
                        "$unwind": "$tasks"
                    }

                    let matchData = {
                        "$match": {
                            "tasks._id": { $in: taskIds }
                        }
                    };

                    let groupData = {
                        "$group": {
                            "_id": "$_id",
                            "tasks": { "$push": "$tasks" }
                        }
                    }

                    aggregatedData.push(unwindData, matchData, groupData);
                }

                let projectData = {
                    "$project": { "tasks": 1 }
                }

                aggregatedData.push(projectData);

                let projects =
                    await database.models.projects.aggregate(aggregatedData);

                return resolve({
                    success: true,
                    data: projects
                });

            } catch (error) {
                return resolve({
                    success: false,
                    data: []
                });
            }
        })
    }

    /**
     * Status of tasks.
     * @method
     * @name tasksStatus 
     * @param {String} projectId - Project id.
     * @param {Array} taskIds - Tasks ids.
     * @returns {Object}
    */

    static tasksStatus(projectId, taskIds = []) {
        return new Promise(async (resolve, reject) => {
            try {

                let tasks = await this.tasks(projectId, taskIds);

                if (!tasks.success || !tasks.data.length > 0) {

                    throw {
                        status: HTTP_STATUS_CODE['bad_request'].status,
                        message: CONSTANTS.apiResponses.PROJECT_NOT_FOUND
                    };
                }

                let projectTasks = tasks.data[0].tasks;
                let result = [];

                for (let task = 0; task < projectTasks.length; task++) {

                    let currentTask = projectTasks[task];

                    let data = {
                        type: currentTask.type,
                        status: currentTask.status,
                        _id: currentTask._id
                    };

                    if (
                        currentTask.type === CONSTANTS.common.ASSESSMENT ||
                        currentTask.type === CONSTANTS.common.OBSERVATION
                    ) {

                        data["submissionDetails"] =
                            currentTask.submissionDetails ? currentTask.submissionDetails : {};
                    }

                    result.push(data);
                }

                return resolve({
                    success: true,
                    message: CONSTANTS.apiResponses.TASKS_STATUS_FETCHED,
                    data: result
                });

            } catch (error) {
                return reject({
                    success: false,
                    message: error.message,
                    status:
                        error.status ?
                            error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                    data: []
                });
            }
        })
    }

    /**
     * Update task.
     * @method
     * @name updateTask 
     * @param {String} projectId - Project id.
     * @param {String} taskId - Task id.
     * @param {Object} updatedData - Update data.
     * @returns {Object}
    */

    static updateTask(projectId, taskId, updatedData) {
        return new Promise(async (resolve, reject) => {
            try {

                let update = {};

                Object.keys(updatedData).forEach(taskData => {
                    update["tasks.$." + taskData] = updatedData[taskData];
                });

                const tasksUpdated =
                    await database.models.projects.findOneAndUpdate({
                        _id: projectId,
                        "tasks._id": taskId
                    }, { $set: update });

                return resolve(tasksUpdated);

            } catch (error) {
                return reject(error);
            }
        })
    }

    /**
     * Solutions details
     * @method
     * @name solutionDetails 
     * @param {String} userToken - Logged in user token.
     * @param {String} projectId - Project id.
     * @param {Array} taskId - Tasks id.
     * @returns {Object}
    */

    static solutionDetails(userToken, projectId, taskId) {
        return new Promise(async (resolve, reject) => {
            try {

                let project = await this.projectDocument(
                    {
                        "_id": projectId,
                        "tasks._id": taskId
                    }, [
                    "entityInformation._id",
                    "entityInformation.entityType",
                    "tasks.type",
                    "tasks._id",
                    "tasks.solutionDetails",
                    "tasks.submissionDetails",
                    "tasks.externalId",
                    "programInformation._id",
                    "projectTemplateId"
                ]
                );

                if (!project.length > 0) {
                    throw {
                        status: HTTP_STATUS_CODE['bad_request'].status,
                        message: CONSTANTS.apiResponses.USER_PROJECT_NOT_FOUND
                    };
                }

                let currentTask = project[0].tasks.find(task => task._id == taskId);

                let assessmentOrObservationData = {
                    entityId: project[0].entityInformation._id,
                    programId: project[0].programInformation._id
                }

                if (currentTask.submissionDetails) {
                    assessmentOrObservationData = currentTask.submissionDetails;
                } else {

                    let solutionDetails = currentTask.solutionDetails;

                    let assessmentOrObservation = {
                        token: userToken,
                        solutionDetails: solutionDetails,
                        entityId: assessmentOrObservationData.entityId,
                        programId: assessmentOrObservationData.programId,
                        project: {
                            "_id": project[0].projectTemplateId,
                            "taskId": currentTask.externalId
                        }
                    };

                    let assignedAssessmentOrObservation =
                        solutionDetails.type === CONSTANTS.common.ASSESSMENT ?
                            await _assessmentDetails(assessmentOrObservation) :
                            await _observationDetails(assessmentOrObservation);

                    if (!assignedAssessmentOrObservation.success) {
                        return resolve(assignedAssessmentOrObservation);
                    }

                    assessmentOrObservationData =
                        _.merge(assessmentOrObservationData, assignedAssessmentOrObservation.data);

                    if (!currentTask.solutionDetails.isReusable) {
                        assessmentOrObservationData["programId"] =
                            currentTask.solutionDetails.programId;
                    }

                    await database.models.projects.findOneAndUpdate({
                        "_id": projectId,
                        "tasks._id": taskId
                    }, {
                        $set: {
                            "tasks.$.submissionDetails": assessmentOrObservationData
                        }
                    });

                }

                assessmentOrObservationData["entityType"] = project[0].entityInformation.entityType;

                return resolve({
                    success: true,
                    message: CONSTANTS.apiResponses.SOLUTION_DETAILS_FETCHED,
                    data: assessmentOrObservationData
                });

            } catch (error) {
                return resolve({
                    status:
                        error.status ?
                            error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                    success: false,
                    message: error.message,
                    data: {}
                });
            }
        })
    }

    /**
      * Bulk create user projects By entityId and role.
      * @method
      * @name bulkCreateByUserRoleAndEntity - Bulk create user projects by entity and role.
      * @param {Object} userProjectData - user project data
      * @param {String} userToken - logged in user token.
      * @returns {Object}  Bulk create user projects.
     */

    static bulkCreateByUserRoleAndEntity(userProjectData, userToken) {
        return new Promise(async (resolve, reject) => {
            try {


                let userAndEntityList = await kendraService.getUsersByEntityAndRole
                    (
                        userProjectData.entityId,
                        userProjectData.role
                    )

                if (!userAndEntityList.success || !userAndEntityList.data) {
                    throw {
                        message: CONSTANTS.apiResponses.USERS_AND_ENTITIES_NOT_FOUND,
                        status: HTTP_STATUS_CODE['bad_request'].status
                    }
                }

                let userProjectBulkCreationData = [];

                await Promise.all(userAndEntityList.data.map(async user => {
                    userProjectBulkCreationData.push({
                        "templateId": userProjectData.templateId,
                        "keycloak-userId": user.userId,
                        "entityId": user.entityId
                    })
                }));

                let userProjects = await this.bulkCreate
                    (
                        userProjectBulkCreationData,
                        userToken
                    )

                return resolve(userProjects);

            } catch (error) {
                return reject(error);
            }
        })
    }

    /**
  * User assigned project creation data.
  * @method
  * @name userAssignedProjectCreation
  * @param {String} templateId - Project template id.
  * @param {String} userId - Logged in user id.
  * @param {String} userToken - Logged in user token.
  * @returns {String} - message.
  */

    static userAssignedProjectCreation(templateId, userId, userToken) {
        return new Promise(async (resolve, reject) => {
            try {

                const projectTemplateData =
                    await projectTemplatesHelper.templateDocument({
                        status: CONSTANTS.common.PUBLISHED,
                        _id: templateId,
                        isReusable: false
                    }, "all",
                        [
                            "ratings",
                            "noOfRatings",
                            "averageRating"
                        ]);

                if (!projectTemplateData.length > 0) {
                    throw {
                        message: CONSTANTS.apiResponses.SOLUTION_NOT_FOUND,
                        status: HTTP_STATUS_CODE['bad_request'].status
                    }
                }

                let result = { ...projectTemplateData[0] };

                result.projectTemplateId = result._id;
                result.projectTemplateExternalId = result.externalId;
                result.userId = userId;
                result.createdBy = userId;
                result.updatedBy = userId;

                let userOrganisations =
                    await kendraService.getUserOrganisationsAndRootOrganisations(
                        userToken,
                        userId
                    );

                if (!userOrganisations.success) {
                    throw {
                        message: CONSTANTS.apiResponses.USER_ORGANISATION_NOT_FOUND,
                        status: HTTP_STATUS_CODE['bad_request'].status
                    }
                }

                result.createdFor =
                    userOrganisations.data.createdFor;

                result.rootOrganisations =
                    userOrganisations.data.rootOrganisations;

                result.assesmentOrObservationTask = false;

                if (projectTemplateData[0].tasks && projectTemplateData[0].tasks.length > 0) {

                    const tasksAndSubTasks =
                        await projectTemplatesHelper.tasksAndSubTasks(
                            projectTemplateData[0]._id
                        );

                    if (tasksAndSubTasks.length > 0) {

                        result.tasks = _projectTask(tasksAndSubTasks);

                        result.tasks.forEach(task => {
                            if (
                                task.type === CONSTANTS.common.ASSESSMENT ||
                                task.type === CONSTANTS.common.OBSERVATION
                            ) {
                                result.assesmentOrObservationTask = true;
                            }
                        });


                        let taskReport = {
                            total: result.tasks.length
                        };

                        result.tasks.forEach(task => {
                            if (!taskReport[task.status]) {
                                taskReport[task.status] = 1;
                            } else {
                                taskReport[task.status] += 1;
                            }
                        });

                        result["taskReport"] = taskReport;

                    }
                }

                delete result._id;

                return resolve({
                    success: true,
                    message: CONSTANTS.apiResponses.UPDATED_DOCUMENT_SUCCESSFULLY,
                    data: result
                });

            } catch (error) {
                return resolve({
                    status:
                        error.status ?
                            error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                    success: false,
                    message: error.message,
                    data: {}
                });
            }
        });
    }

    /**
* Get list of user projects with the targetted ones.
* @method
* @name getProject 
* @param {String} userId - Logged in user id.
* @param {String} userToken - Logged in user token.
* @returns {Object}
*/

    static getProject(bodyData, userId, userToken, pageSize, pageNo, search, filter) {
        return new Promise(async (resolve, reject) => {
            try {

                let query = {
                    userId: userId,
                    isDeleted: false
                }

                let searchQuery = [];

                if (search !== "") {
                    searchQuery = [
                        { "title": new RegExp(search, 'i') },
                        { "description": new RegExp(search, 'i') }
                    ];
                }

                bodyData["filter"] = {};

                if (filter && filter !== "") {
                    if (filter === CONSTANTS.common.CREATED_BY_ME) {
                        query["isAPrivateProgram"] = bodyData["filter"]["isAPrivateProgram"] = {
                            "$ne": false
                        };
                    } else if (filter == CONSTANTS.common.ASSIGN_TO_ME) {
                        query["isAPrivateProgram"] = bodyData["filter"]["isAPrivateProgram"] = false;
                    }
                }

                let projects = await this.projects(
                    query,
                    CONSTANTS.common.DEFAULT_PAGE_SIZE,
                    CONSTANTS.common.DEFAULT_PAGE_NO,
                    searchQuery,
                    [
                        "title", 
                        "description", 
                        "solutionId", 
                        "programId", 
                        "programInformation.name", 
                        "projectTemplateId", 
                        "solutionExternalId",
                        "lastDownloadedAt",
                        "hasAcceptedTAndC"
                    ]
                );

                let solutionIds = [];

                let totalCount = 0;
                let mergedData = [];

                if (projects.success && projects.data) {

                    totalCount = projects.data.count;
                    mergedData = projects.data.data;

                    if (mergedData.length > 0) {
                        mergedData.forEach(projectData => {
                            projectData.name = projectData.title;

                            if (projectData.programInformation) {
                                projectData.programName = projectData.programInformation.name;
                                delete projectData.programInformation;
                            }

                            if (projectData.solutionExternalId) {
                                projectData.externalId = projectData.solutionExternalId;
                                delete projectData.solutionExternalId;
                            }

                            projectData.type = CONSTANTS.common.IMPROVEMENT_PROJECT;

                            if (projectData.solutionId) {
                                solutionIds.push(projectData.solutionId);
                            }

                            delete projectData.title;
                        });
                    }
                }

                if (solutionIds.length > 0) {
                    bodyData["filter"]["skipSolutions"] = solutionIds;
                }

                bodyData.filter["projectTemplateId"] = {
                    $exists: true
                };

                let targetedSolutions =
                    await kendraService.solutionBasedOnRoleAndLocation(
                        userToken,
                        bodyData,
                        CONSTANTS.common.IMPROVEMENT_PROJECT,
                        search
                    );

                if (targetedSolutions.success) {

                    if (targetedSolutions.data.data && targetedSolutions.data.data.length > 0) {
                        totalCount += targetedSolutions.data.count;

                        if (mergedData.length !== pageSize) {

                            targetedSolutions.data.data.forEach(targetedSolution => {
                                targetedSolution.solutionId = targetedSolution._id;
                                targetedSolution._id = "";
                                mergedData.push(targetedSolution);
                            })
                        }
                    }
                }

                if (mergedData.length > 0) {
                    let startIndex = pageSize * (pageNo - 1);
                    let endIndex = startIndex + pageSize;
                    mergedData = mergedData.slice(startIndex, endIndex);
                }

                return resolve({
                    success: true,
                    message: CONSTANTS.apiResponses.TARGETED_PROJECT_FETCHED,
                    data: {
                        description: CONSTANTS.common.PROJECT_DESCRIPTION,
                        data: mergedData,
                        count: totalCount
                    }
                });
            } catch (error) {
                return resolve({
                    success : false,
                    message : error.message,
                    status : 
                    error.status ? 
                    error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                    data : {
                        description : CONSTANTS.common.PROJECT_DESCRIPTION,
                        data : [],
                        count : 0
                    }
                });
            }
        })
      }

  /**
     * Creation of user targeted projects.
     * @method
     * @name detailsV2 
     * @param {String} projectId - project id.
     * @param {String} solutionId - solution id.
     * @param {String} userId - logged in user id.
     * @param {String} userToken - logged in user token.
     * @param {Object} bodyData - Requested body data.
     * @param {String} [appName = ""] - App name.
     * @param {String} [appVersion = ""] - App version.
     * @returns {Object} Project details.
    */

   static detailsV2( projectId,solutionId,userId,userToken,bodyData,appName = "",appVersion = "",templateId = "" ) {
    return new Promise(async (resolve, reject) => {
        try {
            
            let solutionExternalId = "";
            
            if( templateId !== "" ) {
                
                let templateDocuments = 
                await projectTemplatesHelper.templateDocument({
                    "externalId" : templateId,
                    "isReusable" : false,
                    "solutionId" : { $exists : true }
                },["solutionId","solutionExternalId"]);
            
                if( !templateDocuments.length > 0 ) {
                    throw {
                        message : CONSTANTS.apiResponses.PROJECT_TEMPLATE_NOT_FOUND,
                        status : HTTP_STATUS_CODE['bad_request'].status
                    }
                }

            
                solutionId = templateDocuments[0].solutionId;
                solutionExternalId = templateDocuments[0].solutionExternalId;
            }

            if (projectId === "") {
                
                const projectDetails = await this.projectDocument({
                    solutionId: solutionId,
                    userId: userId
                }, ["_id"]);

                if( projectDetails.length > 0 ) {
                    projectId = projectDetails[0]._id;
                } else {
                
                    let solutionDetails = {}

                    if( templateId === "" ) {
                    
                        solutionDetails = 
                        await kendraService.solutionDetailsBasedOnRoleAndLocation(
                            userToken,
                            bodyData,
                            solutionId
                        );

                        if( !solutionDetails.success || (solutionDetails.data.data && !solutionDetails.data.data.length > 0) ) {
                            throw {
                                status : HTTP_STATUS_CODE["bad_request"].status,
                                message : CONSTANTS.apiResponses.SOLUTION_DOES_NOT_EXISTS_IN_SCOPE
                            }
                        }

                        solutionDetails = solutionDetails.data;

                    } else {
                        solutionDetails =
                        await assessmentService.listSolutions([solutionExternalId]);

                        if( !solutionDetails.success ) {
                            throw {
                                message : CONSTANTS.apiResponses.SOLUTION_NOT_FOUND,
                                status : HTTP_STATUS_CODE['bad_request'].status
                            }
                        }
                        solutionDetails = solutionDetails.data[0];
                    }

                    let projectCreation = 
                    await this.userAssignedProjectCreation(
                        solutionDetails.projectTemplateId,
                        userId,
                        userToken
                    );
    
                    if( !projectCreation.success ) {
                        return resolve(projectCreation);
                    }
    
                    projectCreation.data["isAPrivateProgram"] = 
                    solutionDetails.isAPrivateProgram;
    
                    projectCreation.data.programInformation = {
                        _id : ObjectId(solutionDetails.programId),
                        externalId : solutionDetails.programExternalId,
                        description : 
                        solutionDetails.programDescription ? solutionDetails.programDescription : "",
                        name : solutionDetails.programName
                    }
    
                    projectCreation.data.solutionInformation = {
                        _id : ObjectId(solutionDetails._id),
                        externalId : solutionDetails.externalId,
                        description : 
                        solutionDetails.description ? 
                        solutionDetails.description : "",
                        name : solutionDetails.name
                    };
    
                    projectCreation.data["programId"] = 
                    projectCreation.data.programInformation._id;
    
                    projectCreation.data["programExternalId"] = 
                    projectCreation.data.programInformation.externalId;
    
                    projectCreation.data["solutionId"] = 
                    projectCreation.data.solutionInformation._id;
    
                    projectCreation.data["solutionExternalId"] = 
                    projectCreation.data.solutionInformation.externalId;
    
                    projectCreation.data["userRole"] = 
                    bodyData.role;
    
                    projectCreation.data["appInformation"] = {};
    
                    if( appName !== "" ) {
                        projectCreation.data["appInformation"]["appName"] = appName;
                    }
    
                    if( appVersion !== "" ) {
                        projectCreation.data["appInformation"]["appVersion"] = appVersion;
                    }
    
                    if( bodyData && Object.keys(bodyData).length > 0 ) {
    
                        if( bodyData.hasAcceptedTAndC ) {
                            projectCreation.data.hasAcceptedTAndC = bodyData.hasAcceptedTAndC;
                        }

                        if( bodyData.referenceFrom ) {
                            projectCreation.data.referenceFrom = bodyData.referenceFrom;
                        }
    
                        if( bodyData.role ) {
                            projectCreation.data["userRole"] = bodyData.role;
                        }
    
                        if( 
                            solutionDetails.entityType && bodyData[solutionDetails.entityType] 
                        ) {
                            let entityInformation = 
                            await assessmentService.listEntitiesByLocationIds(
                                userToken,
                                [bodyData[solutionDetails.entityType]] 
                            );
        
                            if( !entityInformation.success ) {
                                return resolve(entityInformation);
                            }
        
                            projectCreation.data["entityInformation"] = _entitiesMetaInformation(
                                entityInformation.data
                            )[0];
        
                            projectCreation.data.entityId = entityInformation.data[0]._id;
                        }
    
                    }
    
                    projectCreation.data.status = CONSTANTS.common.NOT_STARTED_STATUS;
                    projectCreation.data.lastDownloadedAt = new Date();
    
                    let project = await database.models.projects.create(projectCreation.data);
                    projectId = project._id;
                }
            }

            let projectDetails = await this.details(projectId, userId);
            
            return resolve({
                success: true,
                message: CONSTANTS.apiResponses.PROJECT_DETAILS_FETCHED,
                data: projectDetails.data
            });

        } catch (error) {
            return resolve({
                status:
                error.status ?
                error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                success: false,
                message: error.message,
                data: []
            });
        }
    })
}

    /**
   * User assigned project creation data.
   * @method
   * @name userAssignedProjectCreation
   * @param {String} templateId - Project template id.
   * @param {String} userId - Logged in user id.
   * @param {String} userToken - Logged in user token.
   * @returns {String} - message.
   */

    static userAssignedProjectCreation(templateId, userId, userToken) {
        return new Promise(async (resolve, reject) => {
            try {

                const projectTemplateData =
                    await projectTemplatesHelper.templateDocument({
                        status: CONSTANTS.common.PUBLISHED,
                        _id: templateId,
                        isReusable: false
                    }, "all",
                        [
                            "ratings",
                            "noOfRatings",
                            "averageRating"
                        ]);

                if (!projectTemplateData.length > 0) {
                    throw {
                        message: CONSTANTS.apiResponses.SOLUTION_NOT_FOUND,
                        status: HTTP_STATUS_CODE['bad_request'].status
                    }
                }

                let result = { ...projectTemplateData[0] };

                result.projectTemplateId = result._id;
                result.projectTemplateExternalId = result.externalId;
                result.userId = userId;
                result.createdBy = userId;
                result.updatedBy = userId;

                let userOrganisations =
                await kendraService.getUserOrganisationsAndRootOrganisations(
                    userToken,
                    userId
                );

                if (!userOrganisations.success) {
                    throw {
                        message: CONSTANTS.apiResponses.USER_ORGANISATION_NOT_FOUND,
                        status: HTTP_STATUS_CODE['bad_request'].status
                    }
                }

                result.createdFor =
                    userOrganisations.data.createdFor;

                result.rootOrganisations =
                    userOrganisations.data.rootOrganisations;

                result.createdAt = new Date();
                result.updatedAt = new Date();

                result.assesmentOrObservationTask = false;

                if (projectTemplateData[0].tasks && projectTemplateData[0].tasks.length > 0) {

                    const tasksAndSubTasks =
                        await projectTemplatesHelper.tasksAndSubTasks(
                            projectTemplateData[0]._id
                        );

                    if (tasksAndSubTasks.length > 0) {

                        result.tasks = _projectTask(tasksAndSubTasks);

                        result.tasks.forEach(task => {
                            if (
                                task.type === CONSTANTS.common.ASSESSMENT ||
                                task.type === CONSTANTS.common.OBSERVATION
                            ) {
                                result.assesmentOrObservationTask = true;
                            }
                        });


                        let taskReport = {
                            total: result.tasks.length
                        };

                        result.tasks.forEach(task => {
                            if (!taskReport[task.status]) {
                                taskReport[task.status] = 1;
                            } else {
                                taskReport[task.status] += 1;
                            }
                        });

                        result["taskReport"] = taskReport;

                    }
                }

                delete result._id;

                return resolve({
                    success: true,
                    message: CONSTANTS.apiResponses.UPDATED_DOCUMENT_SUCCESSFULLY,
                    data: result
                });

            } catch (error) {
                return resolve({
                    status:
                        error.status ?
                            error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                    success: false,
                    message: error.message,
                    data: {}
                });
            }
        });
    }

    /**
       * Add project.
       * @method
       * @name add 
       * @param {Object} data - body data.
       * @param {String} userId - Logged in user id.
       * @param {String} userToken - User token.
       * @param {String} [appName = ""] - App Name.
       * @param {String} [appVersion = ""] - App Version.
       * @returns {Object} Project created information.
     */

    static add(data, userId, userToken, appName = "", appVersion = "") {
        return new Promise(async (resolve, reject) => {
            try {

                const projectsModel = Object.keys(schemas["projects"].schema);
                let createProject = {};

                createProject["userId"] = createProject["createdBy"] = createProject["updatedBy"] = userId;

                let userOrganisations =
                    await kendraService.getUserOrganisationsAndRootOrganisations(
                        userToken
                    );

                if (!userOrganisations.success) {
                    throw {
                        message: CONSTANTS.apiResponses.USER_ORGANISATION_NOT_FOUND,
                        status: HTTP_STATUS_CODE['bad_request'].status
                    }
                }

                if (userOrganisations.data) {
                    createProject.createdFor = userOrganisations.data.createdFor;
                    createProject.rootOrganisations = userOrganisations.data.rootOrganisations;
                }

                let projectData = await _projectData(data);
                if (projectData && projectData.success == true) {
                    createProject = _.merge(createProject, projectData.data);
                }

                let createNewProgramAndSolution = false;

                if (data.programId && data.programId !== "") {
                    createNewProgramAndSolution = true;
                }
                else if (data.programName) {
                    createNewProgramAndSolution = true;
                }

                if (data.entityId) {
                    let entityInformation =
                        await _entitiesInformation([data.entityId]);

                    if (!entityInformation.success) {
                        return resolve(entityInformation);
                    }

                    createProject["entityInformation"] = entityInformation.data[0];
                    createProject.entityId = entityInformation.data[0]._id;
                }

                if (createNewProgramAndSolution) {

                    let programAndSolutionInformation =
                        await this.createProgramAndSolution(
                            data.programId,
                            data.programName,
                            createProject.entityId ? [createProject.entityId] : "",
                            userToken
                        );

                    if (!programAndSolutionInformation.success) {
                        return resolve(programAndSolutionInformation);
                    }

                    createProject =
                        _.merge(createProject, programAndSolutionInformation.data);
                }

                if (data.tasks) {

                    let taskReport = {};

                    createProject.tasks = await _projectTask(
                        data.tasks
                    );

                    taskReport.total = createProject.tasks.length;

                    createProject.tasks.forEach(task => {
                        if (!taskReport[task.status]) {
                            taskReport[task.status] = 1;
                        } else {
                            taskReport[task.status] += 1;
                        }
                    });

                    createProject["taskReport"] = taskReport;
                }

                let booleanData = this.booleanData(schemas["projects"].schema);
                let mongooseIdData = this.mongooseIdData(schemas["projects"].schema);



                Object.keys(data).forEach(updateData => {
                    if (
                        !createProject[updateData] &&
                        projectsModel.includes(updateData)
                    ) {

                        if (booleanData.includes(updateData)) {

                            createProject[updateData] =
                                UTILS.convertStringToBoolean(data[updateData]);

                        } else if (mongooseIdData.includes(updateData)) {
                            createProject[updateData] = ObjectId(data[updateData]);
                        } else {
                            createProject[updateData] = data[updateData];
                        }
                    }
                });

                createProject["appInformation"] = {};
                if (appName !== "") {
                    createProject["appInformation"]["appName"] = appName;
                }

                if (appVersion !== "") {
                    createProject["appInformation"]["appVersion"] = appVersion;
                }

                createProject["lastDownloadedAt"] = new Date();

                if (data.profileInformation) {
                    createProject.userRoleInformtion = data.profileInformation;
                }

                let userProject = await database.models.projects.create(
                    createProject
                );

                if (!userProject._id) {
                    throw {
                        message: CONSTANTS.apiResponses.USER_PROJECT_NOT_CREATED,
                        status: HTTP_STATUS_CODE['bad_request'].status
                    }
                }

                return resolve({
                    success: true,
                    message: CONSTANTS.apiResponses.PROJECT_CREATED,
                    data: {
                        programId:
                        userProject.programInformation && userProject.programInformation._id ?
                        userProject.programInformation._id : "",
                        projectId: userProject._id,
                        lastDownloadedAt: userProject.lastDownloadedAt,
                        hasAcceptedTAndC : userProject.hasAcceptedTAndC ? userProject.hasAcceptedTAndC : false
                    }
                });

            } catch (error) {
                return resolve({
                    status:
                        error.status ?
                            error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                    success: false,
                    message: error.message,
                    data: {}
                });
            }
        })
    }


     /**
       * share project and task pdf report.
       * @method
       * @name share 
       * @param {String} [projectId] - projectId.
       * @returns {Object} Downloadable pdf url.
     */

    static share(projectId = "", taskIds = [], userToken) {
        return new Promise(async (resolve, reject) => {
            try {

                let projectPdf = true;
                let projectDocument = [];

                let query = {
                    _id: projectId,
                    isDeleted: false
                }

                if (!taskIds.length ) {

                    projectDocument = await this.projectDocument
                    (
                        query,
                        [
                            "title",
                            "status",  
                            "metaInformation.goal",
                            "metaInformation.duration",
                            "startDate",
                            "endDate",
                            "tasks",
                            "categories",
                            "programInformation.name"
                        ]
                    );
                }
                else {
                    projectPdf = false;
                    
                    let aggregateData = [

                    { "$match": { _id: ObjectId(projectId), isDeleted: false } },

                    { "$project": {
                        "status": 1, "title": 1, "startDate": 1, "metaInformation.goal": 1, "metaInformation.duration":1,
                        "categories" : 1, "programInformation.name": 1,
                        tasks: { "$filter": {
                            input: '$tasks',
                            as: 'tasks',
                            cond: { "$in": ['$$tasks._id', taskIds]}
                        }}
                    }}]
                   
                    projectDocument = await database.models.projects.aggregate(aggregateData);
                }
              
                if (!projectDocument.length) {
                    throw {
                        message: CONSTANTS.apiResponses.PROJECT_NOT_FOUND,
                        status: HTTP_STATUS_CODE['bad_request'].status
                    }
                }

                projectDocument = projectDocument[0];
                projectDocument.goal = projectDocument.metaInformation ? projectDocument.metaInformation.goal : "";
                projectDocument.duration = projectDocument.metaInformation ? projectDocument.metaInformation.duration : "";
                projectDocument.programName = projectDocument.programInformation ? projectDocument.programInformation.name : "";
                projectDocument.category = [];

                if (projectDocument.categories && projectDocument.categories.length > 0) {
                    projectDocument.categories.forEach( category => {
                        projectDocument.category.push(category.name);
                    })
                }

                let tasks = [];
                if (projectDocument.tasks.length > 0) {
                    projectDocument.tasks.forEach( task => {
                        let subtasks = [];
                        if (!task.isDeleted) {
                           if (task.children.length > 0) {
                               task.children.forEach(children => {
                                   if (!children.isDeleted) {
                                       subtasks.push(children);
                                   }
                               })
                           }
                           task.children = subtasks;
                           tasks.push(task);
                        }
                    })
                    projectDocument.tasks = tasks;
                }

                delete projectDocument.categories;
                delete projectDocument.metaInformation;
                delete projectDocument.programInformation;
               
                let response = await dhitiService.projectAndTaskReport(userToken, projectDocument, projectPdf);

                if (response && response.success == true) {
                    return resolve({
                        success: true,
                        message: CONSTANTS.apiResponses.REPORT_GENERATED_SUCCESSFULLY,
                        data: {
                            data: {
                                downloadUrl: response.data.pdfUrl
                            }
                        }
                    });
                }

                else {
                    throw {
                        message: CONSTANTS.apiResponses.COULD_NOT_GENERATE_PDF_REPORT,
                    }
                }

            } catch (error) {
                return resolve({
                    status:
                        error.status ?
                            error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                    success: false,
                    message: error.message,
                    data: {}
                });
            }
        })
    }

    /**
    * Get list of user projects with the targetted ones.
    * @method
    * @name userAssigned 
    * @param {String} userId - Logged in user id.
    * @param {Number} pageSize - Page size.
    * @param {Number} pageNo - Page No.
    * @param {String} search - Search text.
    * @param {String} filter - filter text.
    * @returns {Object}
   */

  static userAssigned( userId,pageSize,pageNo,search, filter ) {
    return new Promise(async (resolve, reject) => {
        try {

            let query = {
                userId : userId,
                isDeleted : false
            }

            let searchQuery = [];

            if (search !== "") {
                searchQuery = [
                    { "title" : new RegExp(search, 'i') },
                    { "description" : new RegExp(search, 'i') }
                ];
            }

            if ( filter && filter !== "" ) {
                if( filter === CONSTANTS.common.CREATED_BY_ME ) {
                    query["isAPrivateProgram"] = {
                        $ne : false
                    };
                } else if( filter == CONSTANTS.common.ASSIGN_TO_ME ) {
                    query["isAPrivateProgram"] = false;
                }
            }

            let projects = await this.projects(
                query,
                pageSize,
                pageNo,
                searchQuery,    
                [
                    "title", 
                    "description",
                    "solutionId",
                    "programId",
                    "programInformation.name",
                    "projectTemplateId",
                    "solutionExternalId",
                    "lastDownloadedAt",
                    "hasAcceptedTAndC"
                ]
            );

            let totalCount = 0;
            let data = [];

            if( projects.success && projects.data ) {

                totalCount = projects.data.count;
                data = projects.data.data;

                if( data.length > 0 ) {
                    data.forEach( projectData => {
                        projectData.name = projectData.title;

                        if (projectData.programInformation) {
                            projectData.programName = projectData.programInformation.name;
                            delete projectData.programInformation;
                        }

                        if (projectData.solutionExternalId) {
                            projectData.externalId = projectData.solutionExternalId;
                            delete projectData.solutionExternalId;
                        }

                        projectData.type = CONSTANTS.common.IMPROVEMENT_PROJECT;

                        delete projectData.title;
                    });
                }
            }
            
            return resolve({
                success : true,
                message : CONSTANTS.apiResponses.USER_ASSIGNED_PROJECT_FETCHED,
                data : {
                    data: data,
                    count: totalCount
                }
            });

        } catch (error) {
            return resolve({
                success : false,
                message : error.message,
                status : 
                error.status ? 
                error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                data : {
                    description : CONSTANTS.common.PROJECT_DESCRIPTION,
                    data : [],
                    count : 0
                }
            });
        }
    })
  }
};

/**
 * Project information.
 * @method
 * @name _projectInformation 
 * @param {Object} project - Project data.
 * @returns {Object} Project information.
*/

function _projectInformation(project) {

    return new Promise(async (resolve, reject) => {
        try {

            if (project.entityInformation) {
                project.entityId = project.entityInformation._id;
                project.entityName = project.entityInformation.name;
            }

            if (project.programInformation) {
                project.programId = project.programInformation._id;
                project.programName = project.programInformation.name;
            }

            if (project.tasks && project.tasks.length > 0) {

                let attachments = [];
                let mapTaskIdToAttachment = {};

                for (let task = 0; task < project.tasks.length; task++) {

                    let currentTask = project.tasks[task];

                    if (currentTask.attachments && currentTask.attachments.length > 0) {
                        for (
                            let attachment = 0;
                            attachment < currentTask.attachments.length;
                            attachment++
                        ) {
                            let currentAttachment = currentTask.attachments[attachment];
                            attachments.push(currentAttachment.sourcePath);

                            if (!mapTaskIdToAttachment[currentAttachment.sourcePath]) {
                                mapTaskIdToAttachment[currentAttachment.sourcePath] = {
                                    taskId: currentTask._id
                                };

                            }
                        }
                    }

                }

                if (attachments.length > 0) {

                    let attachmentsUrl =
                        await kendraService.getDownloadableUrl(
                            {
                                filePaths: attachments
                            }
                        );

                    if (!attachmentsUrl.success) {
                        throw {
                            status: HTTP_STATUS_CODE['bad_request'].status,
                            message: CONSTANTS.apiResponses.ATTACHMENTS_URL_NOT_FOUND
                        }
                    }

                    if (attachmentsUrl.data.length > 0) {
                        attachmentsUrl.data.forEach(attachmentUrl => {

                            let taskIndex =
                                project.tasks.findIndex(task => task._id === mapTaskIdToAttachment[attachmentUrl.filePath].taskId);

                            if (taskIndex > -1) {
                                let attachmentIndex =
                                    project.tasks[taskIndex].attachments.findIndex(attachment => attachment.sourcePath === attachmentUrl.filePath);

                                if (attachmentIndex > -1) {
                                    project.tasks[taskIndex].attachments[attachmentIndex].url = attachmentUrl.url;
                                }
                            }
                        })
                    }

                }

            }

            project.status =
                project.status ? project.status : CONSTANTS.common.NOT_STARTED_STATUS;

            if (project.metaInformation) {
                Object.keys(project.metaInformation).forEach(projectMetaKey => {
                    project[projectMetaKey] = project.metaInformation[projectMetaKey];
                });
            }

            delete project.metaInformation;
            delete project.__v;
            delete project.entityInformation;
            delete project.solutionInformation;
            delete project.programInformation;

            return resolve({
                success: true,
                data: project
            });

        } catch (error) {
            return resolve({
                message: error.message,
                success: false,
                status:
                    error.status ?
                        error.status : HTTP_STATUS_CODE['internal_server_error'].status
            })
        }
    })
}

/**
  * Task of project.
  * @method
  * @name _projectTask
  * @param {Array} tasks - tasks data.
  * @param {String} userId - logged in user id.
  * @returns {Object} Project task.
*/

function _projectTask(tasks, isImportedFromLibrary = false, parentTaskId = "") {

    tasks.forEach(singleTask => {

        singleTask.externalId = singleTask.externalId ? singleTask.externalId : singleTask.name.toLowerCase();
        singleTask.type = singleTask.type ? singleTask.type : CONSTANTS.common.SIMPLE_TASK_TYPE;
        singleTask.status = singleTask.status ? singleTask.status : CONSTANTS.common.NOT_STARTED_STATUS;
        singleTask.isDeleted = singleTask.isDeleted ? singleTask.isDeleted : false;

        if (!singleTask.hasOwnProperty("isDeletable")) {
            singleTask.isDeletable = true;
        }

        singleTask.createdAt = singleTask.createdAt ? singleTask.createdAt : new Date();
        singleTask.updatedAt = new Date();
        singleTask._id = UTILS.isValidMongoId(singleTask._id.toString()) ? uuidv4() : singleTask._id;
        singleTask.isImportedFromLibrary = isImportedFromLibrary;
        singleTask.syncedAt = new Date();

        if (singleTask.startDate) {
            singleTask.startDate = singleTask.startDate;
        }

        if (singleTask.endDate) {
            singleTask.endDate = singleTask.endDate;
        }

        if (singleTask.visibleIf && singleTask.visibleIf.length > 0) {

            if (parentTaskId !== "") {
                singleTask.visibleIf.forEach(task => {
                    task._id = parentTaskId;
                });
            }
        }

        if (singleTask.children) {
            _projectTask(
                singleTask.children,
                isImportedFromLibrary,
                singleTask._id
            );
        } else {
            singleTask.children = [];
        }

    })

    return tasks;
}

/**
  * Project categories information.
  * @method
  * @name _projectCategories 
  * @param {Array} categories - Categories data.
  * @returns {Object} Project categories information.
*/

function _projectCategories(categories) {
    return new Promise(async (resolve, reject) => {
        try {

            let categoryIds = [];

            categories.forEach(category => {
                if (category.value && category.value !== "") {
                    categoryIds.push(category.value);
                }
            });

            let categoryData = [];

            if (categoryIds.length > 0) {

                categoryData =
                    await libraryCategoriesHelper.categoryDocuments({
                        _id: { $in: categoryIds }
                    }, ["name", "externalId"]);

                if (!categoryData.length > 0) {
                    throw {
                        status: HTTP_STATUS_CODE['bad_request'].status,
                        message: CONSTANTS.apiResponses.CATEGORY_NOT_FOUND
                    }
                }
            }

            let categoryInternalIdToData = {};

            if (categoryData.length > 0) {

                categoryData.forEach(category => {
                    categoryInternalIdToData[category._id.toString()] = category;
                });
            }

            const categoriesData = categories.map(category => {
                let categoryData = {};

                if (
                    category.value &&
                    category.value !== "" &&
                    categoryInternalIdToData[category.value]
                ) {
                    categoryData = categoryInternalIdToData[category.value];
                } else {
                    categoryData = {
                        name: category.label,
                        externalId: "",
                        _id: ""
                    }
                }

                return categoryData;
            });

            return resolve({
                success: true,
                data: categoriesData
            });

        } catch (error) {
            return resolve({
                message: error.message,
                status:
                    error.status ?
                        error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                success: false,
                data: {}
            });
        }
    })
}

/**
  * Entities information for project.
  * @method
  * @name _entitiesInformation 
  * @param {String} entityIds - entity id.
  * @returns {Object} Project entity information.
*/

function _entitiesInformation(entityIds) {
    return new Promise(async (resolve, reject) => {
        try {

            let entityData =
                await kendraService.entityDocuments(
                    entityIds,
                    ["metaInformation", "entityType", "entityTypeId", "registryDetails"]
                );

            if (!entityData.success) {
                throw {
                    status: HTTP_STATUS_CODE['bad_request'].status,
                    message: CONSTANTS.apiResponses.ENTITY_NOT_FOUND
                }
            }

            let entitiesData = [];

            if (entityData.success && entityData.data.length > 0) {

                entitiesData = _entitiesMetaInformation(entityData.data);
            }

            return resolve({
                success: true,
                data: entitiesData
            });

        } catch (error) {
            return resolve({
                status:
                    error.status ?
                        error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                success: false,
                message: error.message,
                data: []
            });
        }
    })
}

/**
   * Assessment details
   * @method
   * @name _assessmentDetails 
   * @param {Object} assessmentData - Assessment data.
   * @returns {Object} 
*/

function _assessmentDetails(assessmentData) {
    return new Promise(async (resolve, reject) => {
        try {

            let result = {};

            if (assessmentData.project) {

                let templateTasks =
                    await projectTemplateTasksHelper.taskDocuments({
                        externalId: assessmentData.project.taskId
                    }, ["_id"])

                if (templateTasks.length > 0) {
                    assessmentData.project.taskId = templateTasks[0]._id;
                }
            }

            if (assessmentData.solutionDetails.isReusable) {

                let createdAssessment =
                    await assessmentService.createAssessmentSolutionFromTemplate(
                        assessmentData.token,
                        assessmentData.solutionDetails._id,
                        {
                            name: assessmentData.solutionDetails.name + "-" + UTILS.epochTime(),
                            description: assessmentData.solutionDetails.name + "-" + UTILS.epochTime(),
                            program: {
                                _id: assessmentData.programId,
                                name: ""
                            },
                            entities: [assessmentData.entityId],
                            project: assessmentData.project
                        }
                    );

                if (!createdAssessment.success) {
                    throw {
                        status: HTTP_STATUS_CODE['bad_request'].status,
                        message: CONSTANTS.apiResponses.COULD_NOT_CREATE_ASSESSMENT_SOLUTION
                    }
                }

                result["solutionId"] = createdAssessment.data._id;

            } else {

                let assignedAssessmentToUser =
                    await assessmentService.createEntityAssessors(
                        assessmentData.token,
                        assessmentData.solutionDetails.programId,
                        assessmentData.solutionDetails._id,
                        [assessmentData.entityId]
                    );

                if (!assignedAssessmentToUser.success) {
                    throw {
                        status: HTTP_STATUS_CODE['bad_request'].status,
                        message: CONSTANTS.apiResponses.FAILED_TO_ASSIGNED_ASSESSMENT_TO_USER
                    }
                }

                let entitiesAddedToSolution =
                    await assessmentService.addEntitiesToSolution(
                        assessmentData.token,
                        assessmentData.solutionDetails._id,
                        [assessmentData.entityId.toString()]
                    );

                if (!entitiesAddedToSolution.success) {
                    throw {
                        status: HTTP_STATUS_CODE['bad_request'].status,
                        message: CONSTANTS.apiResponses.FAILED_TO_ADD_ENTITY_TO_SOLUTION
                    }
                }

                let solutionUpdated =
                    await assessmentService.updateSolution(
                        assessmentData.token,
                        {
                            "project": assessmentData.project,
                            referenceFrom: "project"
                        },
                        assessmentData.solutionDetails.externalId
                    );

                if (!solutionUpdated.success) {
                    throw {
                        status: HTTP_STATUS_CODE['bad_request'].status,
                        message: CONSTANTS.apiResponses.SOLUTION_NOT_UPDATED
                    }
                }

                result["solutionId"] = assessmentData.solutionDetails._id;
            }

            return resolve({
                success: true,
                data: result
            });

        } catch (error) {
            return resolve({
                message: error.message,
                success: false,
                status: error.status ?
                    error.status : HTTP_STATUS_CODE['internal_server_error'].status
            });
        }
    })
}

/**
   * Observation details
   * @method
   * @name _observationDetails 
   * @param {Object} observationData - Observation data.
   * @returns {Object} 
*/

function _observationDetails(observationData) {
    return new Promise(async (resolve, reject) => {
        try {

            let result = {};

            if (observationData.project) {

                let templateTasks =
                    await projectTemplateTasksHelper.taskDocuments({
                        externalId: observationData.project.taskId
                    }, ["_id"])

                if (templateTasks.length > 0) {
                    observationData.project.taskId = templateTasks[0]._id;
                }
            }

            if (observationData.solutionDetails.isReusable) {

                let observationCreatedFromTemplate =
                    await assessmentService.createObservationFromSolutionTemplate(
                        observationData.token,
                        observationData.solutionDetails._id,
                        {
                            name: observationData.solutionDetails.name + "-" + UTILS.epochTime(),
                            description: observationData.solutionDetails.name + "-" + UTILS.epochTime(),
                            program: {
                                _id: observationData.programId,
                                name: ""
                            },
                            status: CONSTANTS.common.PUBLISHED_STATUS,
                            entities: [observationData.entityId],
                            project: observationData.project
                        }
                    );

                if (!observationCreatedFromTemplate.success) {
                    throw {
                        status: HTTP_STATUS_CODE['bad_request'].status,
                        message: CONSTANTS.apiResponses.OBSERVATION_NOT_CREATED
                    }
                }

                result["solutionId"] = observationCreatedFromTemplate.data._id;
                result["observationId"] = observationCreatedFromTemplate.data.observationId;

            } else {

                let solutionUpdated =
                    await assessmentService.updateSolution(
                        observationData.token,
                        {
                            project: observationData.project,
                            referenceFrom: "project"
                        },
                        observationData.solutionDetails.externalId
                    );

                if (!solutionUpdated.success) {
                    throw {
                        status: HTTP_STATUS_CODE['bad_request'].status,
                        message: CONSTANTS.apiResponses.SOLUTION_NOT_UPDATED
                    }
                }

                let startDate = new Date();
                let endDate = new Date();
                endDate.setFullYear(endDate.getFullYear() + 1);

                let observation = {
                    name: observationData.solutionDetails.name,
                    description: observationData.solutionDetails.name,
                    status: CONSTANTS.common.PUBLISHED_STATUS,
                    startDate: startDate,
                    endDate: endDate,
                    entities: [observationData.entityId],
                    project: observationData.project
                };

                let observationCreated = await assessmentService.createObservation(
                    observationData.token,
                    observationData.solutionDetails._id,
                    observation
                );

                if (!observationCreated.success) {
                    throw {
                        status: HTTP_STATUS_CODE['bad_request'].status,
                        message: CONSTANTS.apiResponses.OBSERVATION_NOT_CREATED
                    }
                }

                result["solutionId"] = observationData.solutionDetails._id;
                result["observationId"] = observationCreated.data._id;
            }

            return resolve({
                success: true,
                data: result
            });

        } catch (error) {
            return resolve({
                message: error.message,
                success: false,
                status: error.status ?
                    error.status : HTTP_STATUS_CODE['internal_server_error'].status
            });
        }
    })
}

/**
   * Observation details
   * @method
   * @name _entitiesMetaInformation 
   * @param {Object} entitiesData - entities data.
   * @returns {Object} - entities metadata.
*/

function _entitiesMetaInformation(entitiesData) {

    entitiesData = entitiesData.map(entity => {
        entity.metaInformation._id = ObjectId(entity._id);
        entity.metaInformation.entityType = entity.entityType;
        entity.metaInformation.entityTypeId = ObjectId(entity.entityTypeId);
        entity.metaInformation.registryDetails = entity.registryDetails;
        return entity.metaInformation;
    });

    return entitiesData;
}


/**
  * Project Add And Sync Common information.
  * @method
  * @name _projectData 
  * @param {Array} data - Req data.
  * @returns {Object} Project Add information.
*/

function _projectData(data) {
    return new Promise(async (resolve, reject) => {
        try {

            let projectData = {};
            if (data.categories && data.categories.length > 0) {

                let categories =
                    await _projectCategories(data.categories);

                if (!categories.success) {
                    return resolve(categories);
                }

                projectData.categories = categories.data;
            }

            if (data.startDate) {
                projectData["startDate"] = data.startDate;
            }

            if (data.endDate) {
                projectData["endDate"] = data.endDate;
            }

            if (data.learningResources) {
                projectData.learningResources = data.learningResources;
            }

            projectData.syncedAt = new Date();

            return resolve({
                success: true,
                data: projectData
            });

        } catch (error) {
            return resolve({
                message: error.message,
                status:
                    error.status ?
                        error.status : HTTP_STATUS_CODE['internal_server_error'].status,
                success: false,
                data: {}
            });
        }
    })
}




