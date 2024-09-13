/**
 * name : middleware/validator.js
 * author : Aman Karki
 * Date : 13-July-2020
 * Description : validation for endpoints.
 */


//dependencies
let fs = require("fs");

module.exports = (req, res, next) => {
    let validatorPath;
    if (req.params.file) {
        validatorPath =
         PROJECT_ROOT_DIRECTORY + `/module/${req.params.controller}/${req.params.file}/validator/${req.params.version}.js`;
    } else {
        validatorPath = 
        PROJECT_ROOT_DIRECTORY + `/module/${req.params.controller}/validator/${req.params.version}.js`;
    }

    if (fs.existsSync(validatorPath)) require(validatorPath.replace(/\.\.\//g, ''))(req);

    next();

    return

}