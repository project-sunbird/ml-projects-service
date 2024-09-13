/**
 * name : projectCategories.js
 * author : Priyanka
 * created-date : 01-Sep-2021
 * Description : Project categories helper for DB interactions.
 */

// Dependencies 

/**
    * ProjectCategories
    * @class
*/

module.exports = class ProjectCategories {

      /**
   * Library project categories documents.
   * @method
   * @name categoryDocuments
   * @param {Object} [findQuery = "all"] - filtered data.
   * @param {Array} [fields = "all"] - projected data.
   * @param {Array} [skipFields = "none"] - fields to skip.
   * @returns {Array} - Library project categories data.
   */

  static categoryDocuments(
    findQuery = "all", 
    fields = "all",
    skipFields = "none"
  ) {
      return new Promise(async (resolve, reject) => {
        
        try {
          
          let queryObject = {};

          if (findQuery != "all") {
              queryObject = findQuery;
          }

          let projection = {};

          if (fields != "all") {
              fields.forEach(element => {
                  projection[element] = 1;
              });
          }

          if (skipFields != "none") {
              skipFields.forEach(element => {
                  projection[element] = 0;
              });
          }

          let projectCategoriesData = 
          await database.models.projectCategories.find(
            queryObject, 
            projection
          ).lean();
          
          return resolve(projectCategoriesData);

      } catch (error) {
          return reject(error);
        }
      });
    }

      /**
   * update Many project categories documents.
   * @method
   * @name updateMany
   * @param {Object} [filterQuery] - filtered Query.
   * @param {Object} [updateData] - update data.
   * @returns {Array} - Library project categories data.
   */

  static updateMany(filterQuery, updateData) {
      return new Promise(async (resolve, reject) => {
        
        try {
          
          let updatedCategories = 
                await database.models.projectCategories.updateMany(
                    filterQuery,
                    updateData
                );
          
          return resolve(updatedCategories);

      } catch (error) {
          return reject(error);
        }
      });
    }

};
