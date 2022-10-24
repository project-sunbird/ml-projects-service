/**
 * name : v1.js
 * author : Rakesh
 * created-date : 11-Jan-2020
 * Description : DataPipeline.
 */

 module.exports = (req) => {

    let dataPipeLineValidator = {

        userProject : function () {
            req.checkParams('_id').exists().withMessage("required project id")
            .isMongoId().withMessage("Invalid project id");
        }
    }

    if (dataPipeLineValidator[req.params.method]) {
        dataPipeLineValidator[req.params.method]();
    }

};