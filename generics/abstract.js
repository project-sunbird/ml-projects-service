/**
 * name : abstract.js
 * author : Aman Karki.
 * Date : 13-July-2020
 * Description : Abstract class.
 */

 /**
    * Abstract
    * @class
*/

let Abstract = class Abstract {
  
  constructor(schema) {
    database.createModel(schemas[schema]);
    if ( schemas[schema].compoundIndex && schemas[schema].compoundIndex.length > 0 ) {
      database.runCompoundIndex(schemas[schema].name,schemas[schema].compoundIndex);
    }
    
  }
};

module.exports = Abstract;
