/**
 * name : v1.js
 * author : Aman
 * created-date : 19-Jan-2021
 * Description : Solutions validation.
 */

module.exports = (req) => {

    let solutionsValidator = {

        create : function () {
            req.checkBody('programExternalId').exists().withMessage("required program externalId");
            req.checkBody('entityType').exists().withMessage("required entity type");
            req.checkBody('externalId').exists().withMessage("required solution externalId");
            req.checkBody('name').exists().withMessage("required solution name");
        }
    }

    if (solutionsValidator[req.params.method]) {
        solutionsValidator[req.params.method]();
    }

};