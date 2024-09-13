/**
 * name : v1.js
 * author : Aman
 * created-date : 31-July-2020
 * Description : Projects templates validation.
 */

module.exports = (req) => {

    let projectTemplateValidator = {

        importProjectTemplate : function () {
            req.checkParams('_id').exists().withMessage("required project template id");
            req.checkQuery('solutionId').exists().withMessage("required solution id");
        },
    }

    if (projectTemplateValidator[req.params.method]) {
        projectTemplateValidator[req.params.method]();
    }

};