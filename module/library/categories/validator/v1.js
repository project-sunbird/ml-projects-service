/**
 * name : v1.js
 * author : Aman
 * created-date : 05-Aug-2020
 * Description : Projects categories validation.
 */

module.exports = (req) => {

    let projectsValidator = {
    }

    if (projectsValidator[req.params.method]) {
        projectsValidator[req.params.method]();
    }

};