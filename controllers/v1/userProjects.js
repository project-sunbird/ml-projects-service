/**
 * name : userProjects.js
 * author : Aman
 * created-date : 20-July-2020
 * Description : User Projects related information.
 */

// Dependencies
const csv = require('csvtojson');
const userProjectsHelper = require(MODULES_BASE_PATH + "/userProjects/helper");

 /**
    * UserProjects
    * @class
*/

module.exports = class UserProjects extends Abstract {

    /**
     * @apiDefine errorBody
     * @apiError {String} status 4XX,5XX
     * @apiError {String} message Error
     */

    /**
     * @apiDefine successBody
     *  @apiSuccess {String} status 200
     * @apiSuccess {String} result Data
     */
    
    constructor() {
        super("projects");
    }

    static get name() {
        return "userProjects";
    }

    /**
    * @api {get} /improvement-project/api/v1/userProjects/list?updateLastDownloadedAt=:updateLastDownloadedAt
    * List of projects.
    * @apiVersion 1.0.0
    * @apiGroup User Projects
    * @apiSampleRequest /improvement-project/api/v1/userProjects/list?updateLastDownloadedAt=true
    * @apiParamExample {json} Response:
    * {
    "message": "Project lists fetched successfully",
    "status": 200,
    "result": [
        {
            "_id": "5f2449eb626a540f40817ef5",
            "userId": "e97b5582-471c-4649-8401-3cc4249359bb",
            "status": "pending",
            "createdAt": "2020-06-29T05:38:43.408Z",
            "lastDownloadedAt": "2020-06-29T05:38:43.408Z",
            "syncedAt": "2020-06-29T05:38:43.408Z",
            "isDeleted": false,
            "category": [
                "Community"
            ],
            "createdBy": "e97b5582-471c-4649-8401-3cc4249359bb",
            "startedAt": "2020-06-29T05:38:43.408Z",
            "completedAt": "2020-06-29T05:38:43.408Z",
            "tasks": [
                {
                    "_id": "5f24404784504944928b10bc",
                    "isDeleted": false,
                    "isDeletable": false,
                    "taskSequence": [],
                    "children": [
                        {
                            "_id": "5f24404784504944928b10bd",
                            "createdBy": "e97b5582-471c-4649-8401-3cc4249359bb",
                            "updatedBy": "e97b5582-471c-4649-8401-3cc4249359bb",
                            "isDeleted": false,
                            "isDeletable": false,
                            "taskSequence": [],
                            "children": [],
                            "deleted": false,
                            "type": "single",
                            "projectTemplateId": "5f2402f570d11462f8e9a591",
                            "name": "Sub-Task-4",
                            "externalId": "sub-task-4",
                            "description": "Sub Task-4-details",
                            "updatedAt": "2020-07-31T16:25:01.405Z",
                            "createdAt": "2020-07-31T16:01:11.286Z",
                            "__v": 0,
                            "parentId": "5f24404784504944928b10bc"
                        }
                    ],
                    "deleted": false,
                    "type": "multiple",
                    "projectTemplateId": "5f2402f570d11462f8e9a591",
                    "name": "Task-3",
                    "externalId": "Task-3",
                    "description": "Task-3details",
                    "updatedAt": "2020-07-31T16:25:01.430Z",
                    "createdAt": "2020-07-31T16:01:11.280Z"
                }
            ],
            "entityInformation": {
                "externalId": "1959076",
                "name": "Nigam Pratibha Vidyalaya (Girls), Jauna Pur, New Delhi"
            },
            "solutionInformation": {
                "externalId": "EF-DCPCR-2018-001",
                "name": "DCPCR Assessment Framework 2018",
                "description": "DCPCR Assessment Framework 2018",
                "_id": "5b98fa069f664f7e1ae7498c"
            },
            "entityTypeId": "5ce23d633c330302e720e65f",
            "programInformation": {
                "externalId": "PROGID01",
                "name": "DCPCR School Development Index 2018-19",
                "description": "DCPCR School Development Index 2018-19"
            },
            "title": "Improving Library",
            "goal": "Improving Library",
            "duration": "1 weeak"
        }
    ]
    } 
    * @apiUse successBody
    * @apiUse errorBody
    */

      /**
      * List of project
      * @method
      * @name list
      * @param {Object} req - request data.
      * @returns {JSON} returns a list of project.
     */

    async list(req) {
        return new Promise(async (resolve, reject) => {
            try {

                let projects = await userProjectsHelper.list(
                    req.userDetails.userInformation.userId,
                    req.query.updateLastDownloadedAt ? req.query.updateLastDownloadedAt : false
                );

                projects.result = projects.data;

                return resolve(projects);

            } catch (error) {
                return reject({
                    status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                    message: error.message || HTTP_STATUS_CODE.internal_server_error.message,
                    errorObject: error
                });
            }
        })
    }

      /**
    * @api {get} /improvement-project/api/v1/userProjects/metaForm 
    * Projects metaForm.
    * @apiVersion 1.0.0
    * @apiGroup User Projects
    * @apiSampleRequest /improvement-project/api/v1/userProjects/metaForm
    * @apiParamExample {json} Response:
    * {
    * "message": "Successfully fetched projects metaform",
    * "status": 200,
    * "result": [
        {
            "field": "title",
            "label": "Title",
            "value": "",
            "visible": true,
            "editable": true,
            "input": "text",
            "validation": {
                "required": true
            }
        },
        {
            "field": "description",
            "label": "Description",
            "value": "",
            "visible": true,
            "editable": true,
            "input": "textarea",
            "validation": {
                "required": true
            }
        },
        {
            "field": "categories",
            "label": "Categories",
            "value": "",
            "visible": true,
            "editable": true,
            "input": "select",
            "options": [
                {
                    "_id": "5fbb2bcc3e7f4958654e351e",
                    "label": "Teachers",
                    "value": "teachers"
                },
                {
                    "_id": "5fbb2bcc3e7f4958654e351f",
                    "label": "Students",
                    "value": "students"
                },
                {
                    "_id": "5fbb2bcc3e7f4958654e3520",
                    "label": "Infrastructure",
                    "value": "infrastructure"
                },
                {
                    "_id": "5fbb2bcc3e7f4958654e3521",
                    "label": "Community",
                    "value": "community"
                },
                {
                    "_id": "5fbb2bcc3e7f4958654e3522",
                    "label": "Education Leader",
                    "value": "educationLeader"
                },
                {
                    "_id": "",
                    "label": "Others",
                    "value": "others"
                }
            ],
            "validation": {
                "required": false
            }
        }]}
        * @apiUse successBody
        * @apiUse errorBody 
    */

      /**
      * Projects form
      * @method
      * @name metaForm
      * @param {Object} req - request data.
      * @returns {JSON} Projects form.
     */

    async metaForm(req) {
        return new Promise(async (resolve, reject) => {
            try {

                const formsData = 
                await userProjectsHelper.metaForm(req.userDetails.userToken);

                formsData.result = formsData.data;
                return resolve(formsData);

            } catch (error) {
                return reject({
                    status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                    message: error.message || HTTP_STATUS_CODE.internal_server_error.message,
                    errorObject: error
                });
            }
        })
    }

      /**
    * @api {get} /improvement-project/api/v1/userProjects/tasksMetaForm 
    * Projects task meta form.
    * @apiVersion 1.0.0
    * @apiGroup User Projects
    * @apiSampleRequest /improvement-project/api/v1/userProjects/tasksMetaForm
    * @apiParamExample {json} Response:
    * {
    "message": "Successfully fetched projects tasks metaform",
    "status": 200,
    "result": [
        {
            "field": "name",
            "label": "Name",
            "value": "",
            "visible": true,
            "editable": true,
            "input": "text",
            "validation": {
                "required": true
            }
        },
        {
            "field": "description",
            "label": "Description",
            "value": "",
            "visible": true,
            "editable": true,
            "input": "textarea",
            "validation": {
                "required": true
            }
        }
    ]
    }
    * @apiUse successBody
    * @apiUse errorBody
    */

      /**
      * Projects tasks meta form.
      * @method
      * @name tasksMetaForm
      * @param {Object} req - request data.
      * @returns {JSON} Projects tasks meta form.
     */

    async tasksMetaForm() {
        return new Promise(async (resolve, reject) => {
            try {

                let formsData = await userProjectsHelper.tasksMetaForm();

                formsData.result = formsData.data;
                return resolve(formsData);

            } catch (error) {
                return reject({
                    status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                    message: error.message || HTTP_STATUS_CODE.internal_server_error.message,
                    errorObject: error
                });
            }
        })
    }

     /**
    * @api {post} /improvement-project/api/v1/userProjects/bulkCreate 
    * Bulk create user projects.
    * @apiVersion 1.0.0
    * @apiGroup User Projects
    * @apiParam {File} projects Mandatory project file of type CSV.
    * @apiSampleRequest /improvement-project/api/v1/userProjects/bulkCreate
    * @apiUse successBody
    * @apiUse errorBody
    */

    /**
      * Bulk create user projects.
      * @method
      * @name bulkCreate
      * @param {Object} req - request data.
      * @returns {CSV} Assigned projects to user.
     */

    async bulkCreate(req) {
        return new Promise(async (resolve, reject) => {
            try {

                if ( !req.files || !req.files.projects ) {
                    return resolve(
                      {
                        status : HTTP_STATUS_CODE["bad_request"].status, 
                        message : CONSTANTS.apiResponses.PROJECTS_CSV_REQUIRED
                      }
                    )
                }

                const projectsData = await csv().fromString(
                    req.files.projects.data.toString()
                );

                const projects = await userProjectsHelper.bulkCreate(
                    projectsData,
                    req.userDetails.userToken
                );

                return resolve(projects);

            } catch (error) {
                return reject({
                    status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                    message: error.message || HTTP_STATUS_CODE.internal_server_error.message,
                    errorObject: error
                });
            }
        })
    }

     /**
    * @api {post} /improvement-project/api/v1/userProjects/importFromLibrary/:projectTemplateId 
    * Import project from library.
    * @apiVersion 1.0.0
    * @apiGroup User Projects
    * @apiSampleRequest /improvement-project/api/v1/userProjects/importFromLibrary/5f5b32cef16777642d51aaf0
    * @apiParamExample {json} Request:
    * {
    * "programId" : "",
    * "programName" : "My Program",
    * "rating" : 2
    * }
    * @apiParamExample {json} Response:
    * {
    "message": "Successfully fetched projects",
    "status": 200,
    "result": {
        "userId": "01c04166-a65e-4e92-a87b-a9e4194e771d",
        "createdFor": [],
        "isDeleted": false,
        "categories": [
            {
                "_id": "5f102331665bee6a740714eb",
                "externalId": "community",
                "name": "Community"
            }
        ],
        "createdBy": "01c04166-a65e-4e92-a87b-a9e4194e771d",
        "tasks": [
            {
                "_id": "61d6690d-82cb-4db2-8191-8dd945c5e742",
                "isDeleted": false,
                "isDeletable": false,
                "taskSequence": [],
                "children": [
                    {
                        "_id": "b5068cef-eefc-4f43-8a29-ab9c2268f451",
                        "isDeleted": false,
                        "isDeletable": false,
                        "taskSequence": [],
                        "children": [],
                        "visibleIf": [
                            {
                                "operator": "===",
                                "_id": "5f72f9998925ec7c60f79a91",
                                "value": "started"
                            }
                        ],
                        "deleted": false,
                        "type": "single",
                        "projectTemplateId": "5f5b32cef16777642d51aaf0",
                        "name": "Sub task 1",
                        "externalId": "Sub-task-1",
                        "description": "Sub-Task-1-Description",
                        "updatedAt": "2020-09-29T09:08:41.681Z",
                        "createdAt": "2020-09-29T09:08:41.675Z",
                        "__v": 0,
                        "status": "notStarted"
                    },
                    {
                        "_id": "988ef20f-267f-4bed-9a38-9d7dc6a320e9",
                        "isDeleted": false,
                        "isDeletable": false,
                        "taskSequence": [],
                        "children": [],
                        "visibleIf": [
                            {
                                "operator": "===",
                                "_id": "5f72f9998925ec7c60f79a91",
                                "value": "started"
                            }
                        ],
                        "deleted": false,
                        "type": "single",
                        "projectTemplateId": "5f5b32cef16777642d51aaf0",
                        "name": "Sub task 2",
                        "externalId": "Sub-task-2",
                        "description": "Sub-Task-2-Description",
                        "updatedAt": "2020-09-29T09:08:41.693Z",
                        "createdAt": "2020-09-29T09:08:41.689Z",
                        "__v": 0,
                        "status": "notStarted"
                    }
                ],
                "visibleIf": [],
                "deleted": false,
                "type": "multiple",
                "projectTemplateId": "5f5b32cef16777642d51aaf0",
                "name": "Task 1",
                "externalId": "task-1",
                "description": "Task-1 Description",
                "updatedAt": "2020-09-29T09:08:41.691Z",
                "createdAt": "2020-09-29T09:08:41.612Z",
                "__v": 0,
                "status": "notStarted"
            },
            {
                "_id": "289d9558-b98f-41cf-81d3-92486f114a49",
                "isDeleted": false,
                "isDeletable": false,
                "taskSequence": [],
                "children": [],
                "visibleIf": [],
                "deleted": false,
                "type": "single",
                "projectTemplateId": "5f5b32cef16777642d51aaf0",
                "name": "Task 12",
                "externalId": "Task-12",
                "description": "Task-1 Description",
                "updatedAt": "2020-09-29T09:08:41.667Z",
                "createdAt": "2020-09-29T09:08:41.667Z",
                "__v": 0,
                "status": "notStarted"
            }
        ],
        "updatedBy": "01c04166-a65e-4e92-a87b-a9e4194e771d",
        "rootOrganisations": [],
        "_id": "5f731d68920a8c3e092e6e4c",
        "deleted": false,
        "name": "Test-2",
        "description": "improving school library",
        "status": "notStarted",
        "updatedAt": "2020-09-29T11:41:28.656Z",
        "createdAt": "2020-09-11T08:18:22.077Z",
        "__v": 0,
        "solutionInformation": {
            "externalId": "01c04166-a65e-4e92-a87b-a9e4194e771d-1601379673400"
        },
        "programInformation": {
            "externalId": "My Program-1601379673400",
            "name": "My Program"
        },
        "taskReport": {},
        "entityInformation": {},
        "rationale": "sample",
        "primaryAudience": [
            "teachers",
            "head master"
        ]
    }}
    * @apiUse successBody
    * @apiUse errorBody
    */

    /**
      * Import project from library.
      * @method
      * @name importFromLibrary
      * @param {Object} req - request data.
      * @param {String} req.params._id - project Template Id.
      * @returns {JSON} import project from library.
     */

    async importFromLibrary(req) {
        return new Promise(async (resolve, reject) => {
            try {

                const createdProject = await userProjectsHelper.importFromLibrary(
                    req.params._id,
                    req.body,
                    req.userDetails.userToken,
                    req.userDetails.userInformation.userId
                );

                return resolve({
                    message: createdProject.message,
                    result: createdProject.data
                });

            } catch (error) {
                return reject({
                    status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                    message: error.message || HTTP_STATUS_CODE.internal_server_error.message,
                    errorObject: error
                });
            }
        })
    }

       /**
    * @api {get} /improvement-project/api/v1/userProjects/create
    * Create project.
    * @apiVersion 1.0.0
    * @apiGroup User Projects
    * @apiSampleRequest /improvement-project/api/v1/userProjects/create
    * @apiParamExample {json} Response:
    * {
    "message": "Created user project successfully",
    "status": 200,
    "result": {
        "_id": "5f97d2f6bf3a3b1c0116c80a",
        "lastDownloadedAt": "2020-10-27T07:57:41.003Z"
    }}
    * @apiUse successBody
    * @apiUse errorBody
    */

    /**
      * Create project.
      * @method
      * @name create
      * @param {Object} req - request data.
      * @param {String} req.params._id - Project id.
      * @returns {JSON} Create project.
     */

    async create(req) {
        return new Promise(async (resolve, reject) => {
            try {

                let createdProject = await userProjectsHelper.create(
                    req.userDetails.userInformation.userId,
                    req.userDetails.userToken
                );

                createdProject.result = createdProject.data;

                return resolve(createdProject);

            } catch (error) {
                return reject({
                    status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                    message: error.message || HTTP_STATUS_CODE.internal_server_error.message,
                    errorObject: error
                });
            }
        })
    }

     /**
    * @api {post} /improvement-project/api/v1/userProjects/sync/:projectId?lastDownloadedAt=:epochTime 
    * Sync project.
    * @apiVersion 1.0.0
    * @apiGroup User Projects
    * @apiSampleRequest /improvement-project/api/v1/userProjects/sync/5f731631e8d7cd3b88ac0659?lastDownloadedAt=0125747659358699520
    * @apiParamExample {json} Request:
    * {
    "title": "Project 1",
    "description": "Project 1 description",
    "tasks": [
        {
            "_id": "289d9558-b98f-41cf-81d3-92486f114a49",
            "name": "Task 1",
            "description": "Task 1 description",
            "status": "notStarted/inProgress/completed",
            "isACustomTask": false,
            "startDate": "2020-09-29T09:08:41.667Z",
            "endDate": "2020-09-29T09:08:41.667Z",
            "lastModifiedAt": "2020-09-29T09:08:41.667Z",
            "type": "single/multiple",
            “isDeleted” : false,
             “attachments” : [
               {
                 "name" : "download(2).jpeg",
                 "type" : "image/jpeg",
                  "sourcePath" : "projectId/userId/imageName"
               }
             ],
             “remarks” : “Tasks completed”,
             “assignee” : “Aman”,
            "children": [
                {
                    "_id": "289d9558-b98f-41cf-81d3-92486f114a50",
                    "name": "Task 2",
                    "description": "Task 2 description",
                    "status": "notStarted/inProgress/completed",
                    "children": [],
                    "isACustomTask": false,
                    "startDate": "2020-09-29T09:08:41.667Z",
                    "endDate": "2020-09-29T09:08:41.667Z",
                    "lastModifiedAt": "2020-09-29T09:08:41.667Z",
                    "type": "single/multiple”,
                    “isDeleted” : false
                }
            ]
        }
    ],
    "programId": "",
    "programName": "New Project Program",
    "entityId" : “5beaa888af0065f0e0a10515”,
    "categories": [
        {
            "value": "5f102331665bee6a740714e8",
            "label": "teacher"
        },
        {
            "value": "",
            "label": "other"
        }
    ],
    "status": "notStarted/inProgress/completed",
    “lastDownloadedAt” : "2020-09-29T09:08:41.667Z",
    "payload": {
        "_id": "289d9558-b98f-41cf-81d3-92486f114a51"
    }}
    * @apiParamExample {json} Response:
    * {
    * "message": "Project updated successfully",
    * "status": 200,
    * "result" : {
    *   "programId" : "5fb669f223575a2f0cef3b33"
    * }
    * }
    * @apiUse successBody
    * @apiUse errorBody
    */

    /**
      * Sync projects.
      * @method
      * @name sync
      * @param {Object} req - request data.
      * @param {String} req.params._id - Project id.
      * @returns {JSON} Create Self projects.
     */

    async sync(req) {
        return new Promise(async (resolve, reject) => {
            try {

                let createdProject = await userProjectsHelper.sync(
                    req.params._id,
                    req.query.lastDownloadedAt,
                    req.body,
                    req.userDetails.userInformation.userId,
                    req.userDetails.userToken,
                    req.headers["x-app-id"]  ? 
                    req.headers["x-app-id"]  : 
                    req.headers.appname ? req.headers.appname : "",
                    req.headers["x-app-ver"] ? 
                    req.headers["x-app-ver"] : 
                    req.headers.appversion ? req.headers.appversion : ""
                );

                createdProject.result = createdProject.data;

                return resolve(createdProject);

            } catch (error) {
                return reject({
                    status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                    message: error.message || HTTP_STATUS_CODE.internal_server_error.message,
                    errorObject: error
                });
            }
        })
    }

      /**
    * @api {get} /improvement-project/api/v1/userProjects/details/:projectId
    * Project Details.
    * @apiVersion 1.0.0
    * @apiGroup User Projects
    * @apiSampleRequest /improvement-project/api/v1/userProjects/details/5f731631e8d7cd3b88ac0659
    * @apiParamExample {json} Response:
    * {
    "message": "Successfully fetched project details",
    "status": 200,
    "result": {
        "_id": "5f97d2f6bf3a3b1c0116c80a",
        "status": "notStarted",
        "isDeleted": false,
        "categories": [
            {
                "_id": "5f102331665bee6a740714e8",
                "name": "Teachers",
                "externalId": "teachers"
            },
            {
                "name": "newCategory",
                "externalId": "",
                "_id": ""
            }
        ],
        "tasks": [
            {
                "_id": "289d9558-b98f-41cf-81d3-92486f114a49",
                "name": "Task 1",
                "description": "Task 1 description",
                "status": "notStarted",
                "isACustomTask": false,
                "startDate": "2020-09-29T09:08:41.667Z",
                "endDate": "2020-09-29T09:08:41.667Z",
                "lastModifiedAt": "2020-09-29T09:08:41.667Z",
                "type": "single",
                "isDeleted": false,
                "attachments": [
                    {
                        "name": "download(2).jpeg",
                        "type": "image/jpeg",
                        "sourcePath": "projectId/userId/imageName"
                    }
                ],
                "remarks": "Tasks completed",
                "assignee": "Aman",
                "children": [
                    {
                        "_id": "289d9558-b98f-41cf-81d3-92486f114a50",
                        "name": "Task 2",
                        "description": "Task 2 description",
                        "status": "notStarted",
                        "children": [],
                        "isACustomTask": false,
                        "startDate": "2020-09-29T09:08:41.667Z",
                        "endDate": "2020-09-29T09:08:41.667Z",
                        "lastModifiedAt": "2020-09-29T09:08:41.667Z",
                        "type": "single",
                        "isDeleted": false,
                        "externalId": "task 2",
                        "isDeletable": false,
                        "createdAt": "2020-10-28T05:58:24.907Z",
                        "updatedAt": "2020-10-28T05:58:24.907Z",
                        "isImportedFromLibrary": false
                    }
                ],
                "externalId": "task 1",
                "isDeletable": false,
                "createdAt": "2020-10-28T05:58:24.907Z",
                "updatedAt": "2020-10-28T05:58:24.907Z",
                "isImportedFromLibrary": false
            }
        ],
        "resources": [],
        "deleted": false,
        "lastDownloadedAt": "2020-09-29T09:08:41.667Z",
        "__v": 0,
        "description": "Project 1 description"
    }
    }
    * @apiUse successBody
    * @apiUse errorBody
    */

    /**
      * Project details
      * @method
      * @name details
      * @param {Object} req - request data.
      * @param {String} req.params._id - Project id.
      * @returns {JSON} Create Self projects.
     */

    async details(req) {
        return new Promise(async (resolve, reject) => {
            try {

                let projectDetails = await userProjectsHelper.details(
                    req.params._id,
                    req.userDetails.userInformation.userId
                );

                projectDetails.result = projectDetails.data;

                return resolve(projectDetails);

            } catch (error) {
                return reject({
                    status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                    message: error.message || HTTP_STATUS_CODE.internal_server_error.message,
                    errorObject: error
                });
            }
        })
    }

   /**
    * @api {post} /improvement-project/api/v1/userProjects/tasksStatus/:projectId
    * User Project tasks status
    * @apiVersion 1.0.0
    * @apiGroup User Projects
    * @apiSampleRequest /improvement-project/api/v1/userProjects/tasksStatus/5f731631e8d7cd3b88ac0659
    * @apiParamExample {json} Request:
    * {
    *   "taskIds" : [
           "2f2ef6dd-24e9-40ab-a681-3b3167fcd2c6",
           "a18ae088-fa11-4ff4-899f-213abefb30f6"
       ]
     }
    * @apiParamExample {json} Response:
    {
    "message": "Tasks status fetched successfully",
    "status": 200,
    "result": [
        {
            "type": "assessment",
            "status": "started",
            "_id": "2f2ef6dd-24e9-40ab-a681-3b3167fcd2c6"
        },
        {
            "type": "observation",
            "status": "started",
            "_id": "a18ae088-fa11-4ff4-899f-213abefb30f6",
            "submissionId": "5fbaa71d97ccef111cbb4ee0"
        }
    ]
    }
    * @apiUse successBody
    * @apiUse errorBody
    */

    /**
      * Tasks status
      * @method
      * @name tasksStatus
      * @param {Object} req - request data.
      * @param {String} req.params._id - Project id.
      * @returns {JSON} status of tasks
     */
    
    async tasksStatus(req) {
        return new Promise(async (resolve, reject) => {
            try {

                let taskStatus = await userProjectsHelper.tasksStatus(
                    req.params._id,
                    req.body.taskIds
                );

                taskStatus.result = taskStatus.data;
                
                return resolve(taskStatus);

            } catch (error) {
                return reject({
                    status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                    message: error.message || HTTP_STATUS_CODE.internal_server_error.message,
                    errorObject: error
                });
            }
        })
    }

     /**
    * @api {get} /improvement-project/api/v1/userProjects/solutionDetails/:projectId?taskId=:taskId
    * User project solution details
    * @apiVersion 1.0.0
    * @apiGroup User Projects
    * @apiSampleRequest /improvement-project/api/v1/userProjects/solutionDetails/5fba54dc5bf46b25a926bee5?taskId=347400e7-8a62-4dad-bc24-af7c5bd70ad1
    * @apiParamExample {json} Response:
    * {
    "message" : "Solutions details fetched successfully",
    "status": 200,
    "result": {
        "entityId": "5beaa888af0065f0e0a10515",
        "programId": "5fba54dc2a1f7b172f066597",
        "observationId": "5d1a002d2dfd8135bc8e1617",
        "solutionId": "5d15b0d7463d3a6961f91749"
    }
    }
    * @apiUse successBody
    * @apiUse errorBody
    */

    /**
      * Solutions details information.
      * @method
      * @name status
      * @param {Object} req - request data.
      * @param {String} req.params._id - Project id.
      * @param {String} req.query.taskId - task id.
      * @returns {JSON} Solutions details
     */
    
    async solutionDetails(req) {
        return new Promise(async (resolve, reject) => {
            try {

                let solutionDetails = await userProjectsHelper.solutionDetails(
                    req.userDetails.userToken,
                    req.params._id,
                    req.query.taskId
                );

                solutionDetails.result = solutionDetails.data;
                
                return resolve(solutionDetails);

            } catch (error) {
                return reject({
                    status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                    message: error.message || HTTP_STATUS_CODE.internal_server_error.message,
                    errorObject: error
                });
            }
        })
    }


      /**
    * @api {post} /improvement-project/api/v1/userProjects/bulkCreateByUserRoleAndEntity 
    * Bulk create user projects by entity and role.
    * @apiVersion 1.0.0
    * @apiGroup User Projects
    * @apiSampleRequest /improvement-project/api/v1/userProjects/bulkCreateByUserRoleAndEntity
    * @apiParamExample {json} Request:
    * {
    *  "templateId": "5f2449eb626a540f40817ef5",
    *  "entityId": "5f2449eb626a540f40817ef5",
    *  "role": "CRP",
    *  "programExternalId": "TAF-pgm",
    *  "solutionExternalId": "TAF-solution"
     }
    * @apiUse successBody
    * @apiUse errorBody
    */

    /**
      * Bulk create user projects by entity and role.
      * @method
      * @name bulkCreateByUserRoleAndEntity
      * @param {Object} req - request data.
      * @param {String} req.body.entityId - entityId 
      * @param {String} req.body.role - role 
      * @returns {CSV} Assigned projects to user.
     */

    async bulkCreateByUserRoleAndEntity(req) {
        return new Promise(async (resolve, reject) => {
            try {

                let projects = await userProjectsHelper.bulkCreateByUserRoleAndEntity(
                    req.body,
                    req.userDetails.userToken
                );

                return resolve(projects);

            } catch (error) {
                return reject({
                    status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                    message: error.message || HTTP_STATUS_CODE.internal_server_error.message,
                    errorObject: error
                });
            }
        })
    }

      /**
    * @api {post} /improvement-project/api/v1/userProjects/getProject?page=:page&limit=:limit&search=:search&filter=:assignedToMe
    * List of User projects and auto targeted.
    * @apiVersion 1.0.0
    * @apiGroup User Projects
    * @apiSampleRequest /improvement-project/api/v1/userProjects/getProject
    * @apiParamExample {json} Request:
    * {
    *   "role" : "HM",
   		"state" : "236f5cff-c9af-4366-b0b6-253a1789766a",
        "district" : "1dcbc362-ec4c-4559-9081-e0c2864c2931",
        "school" : "c5726207-4f9f-4f45-91f1-3e9e8e84d824"
    }
    * @apiParamExample {json} Response:
    {
    "message": " Targeted projects fetched successfully",
    "status": 200,
    "result": {
        "description": "Manage and track your school Improvement easily by creating tasks and planning timelines.",
        "data": [
            {
                "_id": "5fd6f3b6062df5269e6532f0",
                "description": "h bucks ",
                "programId": "5fd6f3b7ab86c4262564b83f",
                "solutionId": "5fd6f3b7ab86c4262564b840",
                "name": "gjk"
            },
            {
                "_id": "",
                "externalId": "TAMIL-NADU-AUTO-TARGETING-IMPROVEMENT-PROJECT",
                "programId": "5ffbf8909259097d48017bbf",
                "programName": "Tamil nadu AUTO TARGETING program",
                "description": "tamil nadu improvement project testing",
                "name": "tamil nadu improvement project testing",
                "solutionId": "5ffbf9629259097d48017bc0"
            }
        ],
        "count": 2
    }
    }
    * @apiUse successBody
    * @apiUse errorBody
    */

    /**
      * List of user projects and targetted ones.
      * @method
      * @name getProject
      * @param {Object} req - request data.
      * @returns {JSON} List of user project with targetted ones.
     */
    
    async getProject(req) {
        return new Promise(async (resolve, reject) => {
            try {

                let projects = await userProjectsHelper.getProject(
                    req.body,
                    req.userDetails.userInformation.userId,
                    req.userDetails.userToken,
                    req.pageSize,
                    req.pageNo,
                    req.searchText,
                    req.query.filter
                );

                projects.result = projects.data;
                
                return resolve(projects);

            } catch (error) {
                return reject({
                    status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                    message: error.message || HTTP_STATUS_CODE.internal_server_error.message,
                    errorObject: error
                });
            }
        })
    }
    
    /**
    * @api {post} /improvement-project/api/v1/userProjects/add
    * Add project.
    * @apiVersion 1.0.0
    * @apiGroup User Projects
    * @apiSampleRequest /improvement-project/api/v1/userProjects/add
    * @apiParamExample {json} Request:
    * {
    "title": "Project 1",
    "description": "Project 1 description",
    "tasks": [
        {
            "_id": "289d9558-b98f-41cf-81d3-92486f114a49",
            "name": "Task 1",
            "description": "Task 1 description",
            "status": "notStarted/inProgress/completed",
            "startDate": "2020-09-29T09:08:41.667Z",
            "endDate": "2020-09-29T09:08:41.667Z",
            "lastModifiedAt": "2020-09-29T09:08:41.667Z",
            "type": "single/multiple",
            “isDeleted” : false,
             “remarks” : “Tasks completed”,
             “assignee” : “Aman”,
            "children": [
                {
                    "_id": "289d9558-b98f-41cf-81d3-92486f114a50",
                    "name": "Task 2",
                    "description": "Task 2 description",
                    "status": "notStarted/inProgress/completed",
                    "children": [],
                    "startDate": "2020-09-29T09:08:41.667Z",
                    "endDate": "2020-09-29T09:08:41.667Z",
                    "lastModifiedAt": "2020-09-29T09:08:41.667Z",
                    "type": "single/multiple”,
                    “isDeleted” : false
                }
            ]
        }
    ],
    "programId": "",
    "programName": "New Project Program",
    "entityId" : “5beaa888af0065f0e0a10515”,
    "categories": [
        {
            "value": "5f102331665bee6a740714e8",
            "label": "teacher"
        },
        {
            "value": "",
            "label": "other"
        }
    ],
    "status": "notStarted/inProgress/completed",
    “lastDownloadedAt” : "2020-09-29T09:08:41.667Z",
    "payload": {
        "_id": "289d9558-b98f-41cf-81d3-92486f114a51"
    },
    "profileInformation" : {
        "role" : "HM",
   		"state" : "236f5cff-c9af-4366-b0b6-253a1789766a",
        "district" : "1dcbc362-ec4c-4559-9081-e0c2864c2931",
        "school" : "c5726207-4f9f-4f45-91f1-3e9e8e84d824"
    }}
    * @apiParamExample {json} Response:
    * {
    * "message": "Project created successfully",
    * "status": 200,
    * "result" : {
    *   "programId" : "5fb669f223575a2f0cef3b33"
    *   "projectId" : "5f102331665bee6a740714e8"
    * }
    * }
    * @apiUse successBody
    * @apiUse errorBody
    */

    /**
      * Add projects.
      * @method
      * @name add
      * @param {Object} req - request data.
      * @returns {JSON} Create Self projects.
     */

     async add(req) {
        return new Promise(async (resolve, reject) => {
            try {

                let createdProject = await userProjectsHelper.add(
                    req.body,
                    req.userDetails.userInformation.userId,
                    req.userDetails.userToken,
                    req.headers["x-app-id"]  ? 
                    req.headers["x-app-id"]  : 
                    req.headers.appname ? req.headers.appname : "",
                    req.headers["x-app-ver"] ? 
                    req.headers["x-app-ver"] : 
                    req.headers.appversion ? req.headers.appversion : ""
                );

                createdProject.result = createdProject.data;

                return resolve(createdProject);

            } catch (error) {
                return reject({
                    status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                    message: error.message || HTTP_STATUS_CODE.internal_server_error.message,
                    errorObject: error
                });
            }
        })
    }

      /**
    * @api {get} /improvement-project/api/v1/userProjects/userAssigned?page=:page&limit=:limit&search=:search&filter=:assignedToMe
    * List of user assigned project.
    * @apiVersion 1.0.0
    * @apiGroup User Projects
    * @apiSampleRequest /improvement-project/api/v1/userProjects/userAssigned?page=1&limit=10
    * @apiParamExample {json} Response:
    * {
    "message": "User project fetched successfully",
    "status": 200,
    "result": {
        "data": [
            {
                "_id": "6049c282348d1b060c6454b7",
                "solutionId": "6049c277f026c305dd471769",
                "programId": "6049c275f026c305dd471768",
                "name": "TEST TITLE",
                "programName": "NEW",
                "externalId": "01c04166-a65e-4e92-a87b-a9e4194e771d-1615446645973",
                "type": "improvementProject"
            }
        ],
        "count": 1
    }}

    /**
      * List of user assigned projects.
      * @method
      * @name userAssigned
      * @param {Object} req - request data.
      * @returns {JSON} List of user assigned projects.
     */
    
     async userAssigned(req) {
        return new Promise(async (resolve, reject) => {
            try {

                let projects = await userProjectsHelper.userAssigned(
                    req.userDetails.userInformation.userId,
                    req.pageSize,
                    req.pageNo,
                    req.searchText,
                    req.query.filter
                );

                projects.result = projects.data;
                
                return resolve(projects);
            } catch (error) {
                    return reject({
                        status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                        message: error.message || HTTP_STATUS_CODE.internal_server_error.message,
                        errorObject: error
                    });
                }
            })
    }

     /**
    * @api {get} /improvement-project/api/v1/userProjects/share/:projectId?tasks=:taskId1,:taskId2
    * Share project and task pdf report.
    * @apiVersion 1.0.0
    * @apiGroup User Projects
    * @apiSampleRequest /improvement-project/api/v1/userProjects/share/6065ced7e9259b7f0b1f5d66?tasks=4d074de7-7059-4d99-9da9-452b0d32e081
     * @apiParamExample {json} Response:
    * {
    * "message": "Report generated succesfully",
    * "status": 200,
    * "result" : {
    *   "data" : {
    *      "downloadUrl": "http://localhost:4700/dhiti/api/v1/observations/pdfReportsUrl?id=dG1wLzVhNzZjMTY5LTA5YjAtNGU3Zi04ZmNhLTg0NDc5ZmI2YTNiNC0tODUyOA=="
    * }
    * }
    * }
    * @apiUse successBody
    * @apiUse errorBody
    */

    /*
    * Share project and task pdf report.
      * @method
      * @name share
      * @param {Object} req - request data.
      * @param {String} req.params._id - projectId 
      * @returns {JSON} Downloadable pdf url.
     */

    async share(req) {
        return new Promise(async (resolve, reject) => {
            try {

                let taskIds = req.query.tasks ? req.query.tasks.split(",") : [];

                let report = await userProjectsHelper.share(
                    req.params._id,
                    taskIds,
                    req.userDetails.userToken
                );

                return resolve({
                    message: report.message,
                    result: report.data
                });

            } catch (error) {
                return reject({
                    status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                    message: error.message || HTTP_STATUS_CODE.internal_server_error.message,
                    errorObject: error
                });
            }
        })
    }
};