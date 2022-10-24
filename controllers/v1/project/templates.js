/**
 * name : templates.js
 * author : Aman
 * created-date : 22-July-2020
 * Description : Projects templates related information.
 */

// Dependencies

const csv = require('csvtojson');
const projectTemplatesHelper = require(MODULES_BASE_PATH + "/project/templates/helper");

 /**
    * ProjectTemplates
    * @class
*/

module.exports = class ProjectTemplates extends Abstract {

    /**
     * @apiDefine errorBody
     * @apiError {String} status 4XX,5XX
     * @apiError {String} message Error
     */

    /**
     * @apiDefine successBody
     * @apiSuccess {String} status 200
     * @apiSuccess {String} result Data
     */
    
    constructor() {
        super("project-templates");
    }
   

    /**
    * @api {post} /improvement-project/api/v1/project/templates/bulkCreate 
    * Bulk Create projects templates.
    * @apiVersion 1.0.0
    * @apiGroup Project Templates
    * @apiParam {File} projectTemplates Mandatory project templates file of type CSV.
    * @apiSampleRequest /improvement-project/api/v1/project/templates/bulkCreate
    * @apiUse successBody
    * @apiUse errorBody
    */

      /**
      * Bulk Create project templates
      * @method
      * @name bulkCreate
      * @returns {JSON} returns uploaded project templates.
     */

    async bulkCreate(req) {
        return new Promise(async (resolve, reject) => {
            try {
                
                if ( !req.files || !req.files.projectTemplates ) {
                    return resolve(
                      {
                        status : HTTP_STATUS_CODE["bad_request"].status, 
                        message : CONSTANTS.apiResponses.PROJECT_TEMPLATES_CSV
                      }
                    )
                }

                const templatesData = 
                await csv().fromString(req.files.projectTemplates.data.toString());

                const projectTemplates = await projectTemplatesHelper.bulkCreate(
                    templatesData,
                    req.userDetails.userInformation.userId
                );

                return resolve(projectTemplates);

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
    * @api {post} /improvement-project/api/v1/project/templates/bulkUpdate 
    * Bulk Update projects templates.
    * @apiVersion 1.0.0
    * @apiGroup Project Templates
    * @apiParam {File} projectTemplates Mandatory project templates file of type CSV.
    * @apiSampleRequest /improvement-project/api/v1/project/templates/bulkUpdate
    * @apiUse successBody
    * @apiUse errorBody
    */

      /**
      * Bulk Update project templates
      * @method
      * @name bulkUpdate
      * @returns {JSON} returns uploaded project templates.
     */

    async bulkUpdate(req) {
        return new Promise(async (resolve, reject) => {
            try {
                
                if ( !req.files || !req.files.projectTemplates ) {
                    return resolve(
                      {
                        status : HTTP_STATUS_CODE["bad_request"].status, 
                        message : CONSTANTS.apiResponses.PROJECT_TEMPLATES_CSV
                      }
                    )
                }
                
                const templatesData = 
                await csv().fromString(req.files.projectTemplates.data.toString());

                const projectTemplates = await projectTemplatesHelper.bulkUpdate(
                    templatesData,
                    req.userDetails.userInformation.userId
                );

                return resolve(projectTemplates);

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
    * @api {post} /improvement-project/api/v1/project/templates/importProjectTemplate/:projectTemplateExternalId 
    * Import templates from existsing project templates.
    * @apiVersion 1.0.0
    * @apiGroup Project Templates
    * @apiSampleRequest /improvement-project/api/v1/project/templates/importProjectTemplate/template-1
    * @apiParamExample {json} Request: 
    * {
    * "externalId" : "template1",
      "isReusable" : false,
      "rating" : 5
    * }
    * @apiParamExample {json} Response:
    * {
    "message": "Successfully created duplicate project templates",
    "status": 200,
    "result": {
        "_id": "5f2402f570d11462f8e9a591"
    }
    }
    * @apiUse successBody
    * @apiUse errorBody
    */

      /**
      * Import templates from existsing project templates.
      * @method
      * @name importProjectTemplate
      * @param {Object} req - request data.
      * @param {String} req.params._id - project Template ExternalId.
      * @returns {JSON} returns imported project templates.
     */

    async importProjectTemplate(req) {
        return new Promise(async (resolve, reject) => {
            try {

                let projectTemplates = 
                await projectTemplatesHelper.importProjectTemplate(
                    req.params._id,
                    req.userDetails.userInformation.userId,
                    req.userDetails.userToken,
                    req.query.solutionId ? req.query.solutionId : "",
                    req.body
                );

                projectTemplates.result = projectTemplates.data;

                return resolve(projectTemplates);

            } catch (error) {
                return reject({
                    status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                    message: error.message || HTTP_STATUS_CODE.internal_server_error.message
                });
            }
        })
    }

     /**
    * @api {post} /improvement-project/api/v1/project/templates/listByIds
    * List templates based on ids.
    * @apiVersion 1.0.0
    * @apiGroup Project Templates
    * @apiSampleRequest /improvement-project/api/v1/project/templates/listByIds
    * @apiParamExample {json} Request: 
    * {
    * "externalIds" : ["IDEAIMP 4"]
    * }
    * @apiParamExample {json} Response:
    * {
    "message": "List of project templates fetched successfully",
    "status": 200,
    "result": [
        {
            "_id": "5fd0c55d496c5a49b203e047",
            "title": "Keep Our Schools Alive! (Petition)",
            "externalId": "IDEAIMP 4",
            "goal": "Leveraging the huge number of private schools to show the significance of the financial problem by creating a petition and presenting to the authorities."
        }
    ]
    }
    * @apiUse successBody
    * @apiUse errorBody
    */

      /**
       * List templates based on ids.
      * @method
      * @name listByIds
      * @param {Object} req - request data.
      * @returns {Array} List of templates.
     */

    async listByIds(req) {
        return new Promise(async (resolve, reject) => {
            try {

                let projectTemplates = 
                await projectTemplatesHelper.listByIds(req.body.externalIds);

                projectTemplates.result = projectTemplates.data;

                return resolve(projectTemplates);

            } catch (error) {
                return reject({
                    status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                    message: error.message || HTTP_STATUS_CODE.internal_server_error.message
                });
            }
        })
    }

    /**
    * @api {get} /improvement-project/api/v1/project/templates/details/:templateId
    * Project template details.
    * @apiVersion 1.0.0
    * @apiGroup Project Templates
    * @apiSampleRequest /improvement-project/api/v1/project/templates/details/MAHARASTHA IMPROVEMENT PROJECT TEMPLATE
    * @apiParamExample {json} Response:
    * {
    "message": "Successfully fetched project template details",
    "status": 200,
    "result": {
        "_id": "5ffbd53f5fc92a7dbc972906",
        "description": "",
        "concepts": [
            ""
        ],
        "keywords": [
            "Community, Parent Mela"
        ],
        "isDeleted": false,
        "recommendedFor": [],
        "tasks": [
            {
                "_id": "5fd2447b1233354b094f15d5",
                "createdBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                "updatedBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                "isDeleted": false,
                "taskSequence": [],
                "children": [
                    {
                        "_id": "5fd2447b1233354b094f15db",
                        "createdBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                        "updatedBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                        "isDeleted": false,
                        "taskSequence": [],
                        "children": [],
                        "visibleIf": [
                            {
                                "operator": "===",
                                "_id": "5fd2447b1233354b094f15d5",
                                "value": "started"
                            }
                        ],
                        "hasSubTasks": false,
                        "learningResources": [],
                        "deleted": false,
                        "type": "simple",
                        "name": "Look for videos and case studies which capture the parent mela or parent meeting conducted in different schools. Focus on the ideas being used and themes being selected.",
                        "externalId": "IMP-3147aa-TASK7",
                        "description": "",
                        "updatedAt": "2020-12-10T15:53:31.478Z",
                        "createdAt": "2020-12-10T15:53:31.476Z",
                        "parentId": "5fd2447b1233354b094f15d5",
                        "isDeletable": true
                    }
                ],
                "visibleIf": [],
                "hasSubTasks": true,
                "learningResources": [],
                "deleted": false,
                "type": "simple",
                "name": "Look for samples of parent mela/ excitement building parent meeting from different schools",
                "externalId": "IMP-3147aa-TASK1",
                "description": "",
                "updatedAt": "2020-12-10T15:53:31.477Z",
                "createdAt": "2020-12-10T15:53:31.460Z",
                "isDeletable": true
            },
            {
                "_id": "5fd2447b1233354b094f15d6",
                "createdBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                "updatedBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                "isDeleted": false,
                "taskSequence": [],
                "children": [
                    {
                        "_id": "5fd2447b1233354b094f15dc",
                        "createdBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                        "updatedBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                        "isDeleted": false,
                        "taskSequence": [],
                        "children": [],
                        "visibleIf": [
                            {
                                "operator": "===",
                                "_id": "5fd2447b1233354b094f15d6",
                                "value": "started"
                            }
                        ],
                        "hasSubTasks": false,
                        "learningResources": [],
                        "deleted": false,
                        "type": "simple",
                        "name": "-Meeting with teachers to discuss the importance and role of parent mela in improving community engagement in the school.",
                        "externalId": "IMP-3147aa-TASK8",
                        "description": "",
                        "updatedAt": "2020-12-10T15:53:31.483Z",
                        "createdAt": "2020-12-10T15:53:31.481Z",
                        "parentId": "5fd2447b1233354b094f15d6",
                        "isDeletable": true
                    },
                    {
                        "_id": "5fd2447b1233354b094f15e1",
                        "createdBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                        "updatedBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                        "isDeleted": false,
                        "taskSequence": [],
                        "children": [],
                        "visibleIf": [
                            {
                                "operator": "===",
                                "_id": "5fd2447b1233354b094f15d6",
                                "value": "started"
                            }
                        ],
                        "hasSubTasks": false,
                        "learningResources": [],
                        "deleted": false,
                        "type": "simple",
                        "name": "-Discuss stories from other schools.",
                        "externalId": "IMP-3147aa-TASK13",
                        "description": "",
                        "updatedAt": "2020-12-10T15:53:31.511Z",
                        "createdAt": "2020-12-10T15:53:31.509Z",
                        "parentId": "5fd2447b1233354b094f15d6",
                        "isDeletable": true
                    },
                    {
                        "_id": "5fd2447b1233354b094f15e2",
                        "createdBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                        "updatedBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                        "isDeleted": false,
                        "taskSequence": [],
                        "children": [],
                        "visibleIf": [
                            {
                                "operator": "===",
                                "_id": "5fd2447b1233354b094f15d6",
                                "value": "started"
                            }
                        ],
                        "hasSubTasks": false,
                        "learningResources": [],
                        "deleted": false,
                        "type": "simple",
                        "name": "- Form a committee among teachers to plan and facilitate the mela along with the school leader.",
                        "externalId": "IMP-3147aa-TASK14",
                        "description": "",
                        "updatedAt": "2020-12-10T15:53:31.524Z",
                        "createdAt": "2020-12-10T15:53:31.521Z",
                        "parentId": "5fd2447b1233354b094f15d6",
                        "isDeletable": true
                    }
                ],
                "visibleIf": [],
                "hasSubTasks": true,
                "learningResources": [],
                "deleted": false,
                "type": "simple",
                "name": "Form a parent mela committee among the teachers",
                "externalId": "IMP-3147aa-TASK2",
                "description": "",
                "updatedAt": "2020-12-10T15:53:31.522Z",
                "createdAt": "2020-12-10T15:53:31.463Z",
                "isDeletable": true
            },
            {
                "_id": "5fd2447b1233354b094f15d7",
                "createdBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                "updatedBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                "isDeleted": false,
                "taskSequence": [],
                "children": [
                    {
                        "_id": "5fd2447b1233354b094f15dd",
                        "createdBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                        "updatedBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                        "isDeleted": false,
                        "taskSequence": [],
                        "children": [],
                        "visibleIf": [
                            {
                                "operator": "===",
                                "_id": "5fd2447b1233354b094f15d7",
                                "value": "started"
                            }
                        ],
                        "hasSubTasks": false,
                        "learningResources": [],
                        "deleted": false,
                        "type": "simple",
                        "name": "Finalize the themes and activities for the mela. This could be taken borrowed from other schools or decided by the teachers through brainstorming. \nFinalize the dates for the same",
                        "externalId": "IMP-3147aa-TASK9",
                        "description": "",
                        "updatedAt": "2020-12-10T15:53:31.489Z",
                        "createdAt": "2020-12-10T15:53:31.487Z",
                        "parentId": "5fd2447b1233354b094f15d7",
                        "isDeletable": true
                    }
                ],
                "visibleIf": [],
                "hasSubTasks": true,
                "learningResources": [],
                "deleted": false,
                "type": "simple",
                "name": "Planning for the mela",
                "externalId": "IMP-3147aa-TASK3",
                "description": "",
                "updatedAt": "2020-12-10T15:53:31.488Z",
                "createdAt": "2020-12-10T15:53:31.465Z",
                "isDeletable": true
            },
            {
                "_id": "5fd2447b1233354b094f15d8",
                "createdBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                "updatedBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                "isDeleted": false,
                "taskSequence": [],
                "children": [
                    {
                        "_id": "5fd2447b1233354b094f15de",
                        "createdBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                        "updatedBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                        "isDeleted": false,
                        "taskSequence": [],
                        "children": [],
                        "visibleIf": [
                            {
                                "operator": "===",
                                "_id": "5fd2447b1233354b094f15d8",
                                "value": "started"
                            }
                        ],
                        "hasSubTasks": false,
                        "learningResources": [],
                        "deleted": false,
                        "type": "simple",
                        "name": "Communicate the event with parents and invite them to the mela",
                        "externalId": "IMP-3147aa-TASK10",
                        "description": "",
                        "updatedAt": "2020-12-10T15:53:31.496Z",
                        "createdAt": "2020-12-10T15:53:31.493Z",
                        "parentId": "5fd2447b1233354b094f15d8",
                        "isDeletable": true
                    }
                ],
                "visibleIf": [],
                "hasSubTasks": true,
                "learningResources": [],
                "deleted": false,
                "type": "simple",
                "name": "Invite parents",
                "externalId": "IMP-3147aa-TASK4",
                "description": "",
                "updatedAt": "2020-12-10T15:53:31.494Z",
                "createdAt": "2020-12-10T15:53:31.468Z",
                "isDeletable": true
            },
            {
                "_id": "5fd2447b1233354b094f15d9",
                "createdBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                "updatedBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                "isDeleted": false,
                "taskSequence": [],
                "children": [
                    {
                        "_id": "5fd2447b1233354b094f15df",
                        "createdBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                        "updatedBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                        "isDeleted": false,
                        "taskSequence": [],
                        "children": [],
                        "visibleIf": [
                            {
                                "operator": "===",
                                "_id": "5fd2447b1233354b094f15d9",
                                "value": "started"
                            }
                        ],
                        "hasSubTasks": false,
                        "learningResources": [],
                        "deleted": false,
                        "type": "simple",
                        "name": "Do necessary arrangements for smooth functioning of the event and to make it attractive and interesting to parents",
                        "externalId": "IMP-3147aa-TASK11",
                        "description": "",
                        "updatedAt": "2020-12-10T15:53:31.501Z",
                        "createdAt": "2020-12-10T15:53:31.498Z",
                        "parentId": "5fd2447b1233354b094f15d9",
                        "isDeletable": true
                    }
                ],
                "visibleIf": [],
                "hasSubTasks": true,
                "learningResources": [],
                "deleted": false,
                "type": "simple",
                "name": "Make the arrangements",
                "externalId": "IMP-3147aa-TASK5",
                "description": "",
                "updatedAt": "2020-12-10T15:53:31.499Z",
                "createdAt": "2020-12-10T15:53:31.471Z",
                "isDeletable": true
            },
            {
                "_id": "5fd2447b1233354b094f15da",
                "createdBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                "updatedBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                "isDeleted": false,
                "taskSequence": [],
                "children": [
                    {
                        "_id": "5fd2447b1233354b094f15e0",
                        "createdBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                        "updatedBy": "140558b9-7df4-4993-be3c-31eb8b9ca368",
                        "isDeleted": false,
                        "taskSequence": [],
                        "children": [],
                        "visibleIf": [
                            {
                                "operator": "===",
                                "_id": "5fd2447b1233354b094f15da",
                                "value": "started"
                            }
                        ],
                        "hasSubTasks": false,
                        "learningResources": [],
                        "deleted": false,
                        "type": "simple",
                        "name": "Make sure you collect feedback from parents after the event and reflect among the teachers to see the areas of improvement for next year.",
                        "externalId": "IMP-3147aa-TASK12",
                        "description": "",
                        "updatedAt": "2020-12-10T15:53:31.506Z",
                        "createdAt": "2020-12-10T15:53:31.503Z",
                        "parentId": "5fd2447b1233354b094f15da",
                        "isDeletable": true
                    }
                ],
                "visibleIf": [],
                "hasSubTasks": true,
                "learningResources": [],
                "deleted": false,
                "type": "simple",
                "name": "Collect feedback after the event",
                "externalId": "IMP-3147aa-TASK6",
                "description": "",
                "updatedAt": "2020-12-10T15:53:31.505Z",
                "createdAt": "2020-12-10T15:53:31.473Z",
                "isDeletable": true
            }
        ],
        "learningResources": [
            {
                "name": "Examprep_10EM_ps_cha1_Q3",
                "id": "do_31268582767737241615189",
                "app": "diksha",
                "link": "https://staging.sunbirded.org/resources/play/content/do_31268582767737241615189"
            },
            {
                "name": "Examprep_10tm_ps_cha 11-Q2",
                "id": "do_31269107959395942417491",
                "app": "diksha",
                "link": "https://staging.sunbirded.org/resources/play/content/do_31269107959395942417491"
            },
            {
                "name": "Examprep_10tm_ps_cha 11-Q3",
                "id": "do_31269108472948326417493",
                "app": "diksha",
                "link": "https://staging.sunbirded.org/resources/play/content/do_31269108472948326417493"
            }
        ],
        "isReusable": false,
        "taskSequence": [],
        "deleted": false,
        "categories": [
            {
                "_id": "5fcfa9a2457d6055e33843f2",
                "externalId": "community",
                "name": "Community"
            }
        ],
        "title": "Come See Our School!- Parent Mela",
        "externalId": "MAHARASTHA IMPROVEMENT PROJECT TEMPLATE",
        "entityType": "",
        "taskCreationForm": "",
        "status": "published",
        "solutionId": "5ff9dc1b9259097d48017bbe",
        "solutionExternalId": "MAHARASTHA-IMPROVEMENT-PROJECT",
        "programInformation": {
            "programId": "5ff840ce383ae437eb02b96c",
            "programName": "MAHARASTHA AUTO TARGETING program"
        },
        "rationale": "",
        "primaryAudience": [
            "Community"
        ],
        "goal": "Organizing the Parent Mela in the school in order to make better community reach",
        "duration": "At the end of every quarter",
        "successIndicators": "",
        "risks": "",
        "approaches": "",
        "projectId": ""
    }}
    * @apiUse successBody
    * @apiUse errorBody
    */

      /**
       * Project templates details.
      * @method
      * @name details
      * @param {Object} req - request data.
      * @returns {Array} Details templates.
     */

       async details(req) {
        return new Promise(async (resolve, reject) => {
            
            try {
                if( !req.params._id && !req.query.link ) {
                    throw{
                        status:HTTP_STATUS_CODE.bad_request.status,
                        message:CONSTANTS.apiResponses.TEMPLATE_ID_OR_LINK_REQUIRED
                    }
                }
                
                let projectTemplatesDetails = 
                await projectTemplatesHelper.details(
                    req.params._id ? req.params._id : "",
                    req.query.link ? req.query.link : "",
                    req.userDetails && req.userDetails.userInformation && req.userDetails.userInformation.userId ? req.userDetails.userInformation.userId : ""
                );

                projectTemplatesDetails.result = projectTemplatesDetails.data;

                return resolve(projectTemplatesDetails);

            } catch (error) {
                return reject({
                    status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                    message: error.message || HTTP_STATUS_CODE.internal_server_error.message
                });
            }
        })
    }

    /**
    * @api {post} /improvement-project/api/v1/project/templates/update/:templateId 
    * Update projects template.
    * @apiVersion 1.0.0
    * @apiGroup Project Templates
    * @apiSampleRequest /improvement-project/api/v1/project/templates/update/6006b5cca1a95727dbcdf648
    * @apiHeader {String} internal-access-token internal access token 
    * @apiHeader {String} X-authenticated-user-token Authenticity token  
    * @apiUse successBody
    * @apiUse errorBody
    * @apiParamExample {json} Response:
    * {
    *  "status": 200,
        "message": "template updated successfully"
    }
    */

      /**
      * Update project templates
      * @method
      * @name update
      * @returns {JSON} returns uploaded project template.
     */

    async update(req) {
        return new Promise(async (resolve, reject) => {
            try {

                let projectTemplate = await projectTemplatesHelper.update(
                  req.params._id, 
                  req.body, 
                  req.userDetails.id
                );

                projectTemplate.result = projectTemplate.data;

                return resolve(projectTemplate);

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
