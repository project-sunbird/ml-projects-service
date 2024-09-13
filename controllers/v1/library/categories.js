/**
 * name : categories.js
 * author : Aman
 * created-date : 16-July-2020
 * Description : Library categories related information.
 */

// Dependencies

const libraryCategoriesHelper = require(MODULES_BASE_PATH + "/library/categories/helper");

 /**
    * LibraryCategories
    * @class
*/

module.exports = class LibraryCategories extends Abstract {

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
        super("project-categories");
    }

    static get name() {
        return "projectCategories";
    }

    /**
    * @api {get} /improvement-project/api/v1/library/categories/projects/:categoryExternalId?page=:page&limit=:limit&search=:search&sort=:sort 
    * List of library projects.
    * @apiVersion 1.0.0
    * @apiGroup Library Categories
    * @apiSampleRequest /improvement-project/api/v1/library/categories/projects/community?page=1&limit=1&search=t&sort=importantProject
    * @apiParamExample {json} Response:
    * {
    "message": "Successfully fetched projects",
    "status": 200,
    "result": {
        "data" : [
            {
                "_id": "5f4c91b0acae343a15c39357",
                "averageRating": 2.5,
                "noOfRatings": 4,
                "name": "Test-template",
                "externalId": "Test-template1",
                "description" : "Test template description",
                "createdAt": "2020-08-31T05:59:12.230Z"
            }
        ], 
        "count": 7
    }
    }
    * @apiUse successBody
    * @apiUse errorBody
    */

      /**
      * List of library categories projects.
      * @method
      * @name projects
      * @param {Object} req - requested data
      * @returns {Array} Library Categories project.
     */

    async projects(req) {
        return new Promise(async (resolve, reject) => {
            try {
                
                const libraryProjects = 
                await libraryCategoriesHelper.projects(
                    req.params._id ? req.params._id : "",
                    req.pageSize,
                    req.pageNo,
                    req.searchText,
                    req.query.sort
                );
                
                return resolve({
                    message : libraryProjects.message,
                    result : libraryProjects.data
                });

            } catch (error) {
                return reject(error);
            }
        })
    }

};
