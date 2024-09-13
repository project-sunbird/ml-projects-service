/**
 * name : certificateTemplates.js
 * author : Vishnu
 * created-date : 29-Sep-2022
 * Description : Certificate template related information.
*/

module.exports = class CertificateTemplates extends Abstract {
    constructor() {
        super("certificateTemplates");
      }
    
      static get name() {
        return "certificateTemplates";
      }
}