/**
 * name : index.js
 * author : Aman Karki
 * Date : 13-July-2020
 * Description : All routes.
 */

// Dependencies
const authenticator = require(PROJECT_ROOT_DIRECTORY + "/generics/middleware/authenticator");
const pagination = require(PROJECT_ROOT_DIRECTORY + "/generics/middleware/pagination");
const fs = require("fs");
const inputValidator = require(PROJECT_ROOT_DIRECTORY + "/generics/middleware/validator");

module.exports = function (app) {
  
  app.use(authenticator);
  app.use(pagination);

  var router = async function (req, res, next) {

    if (!req.params.version) {
      next();
    } else if (!controllers[req.params.version]) {
      next();
    } else if (!controllers[req.params.version][req.params.controller]) {
      next();
    }
    else if (!(controllers[req.params.version][req.params.controller][req.params.method] 
      || controllers[req.params.version][req.params.controller][req.params.file][req.params.method])) {
      next();
    }
    else if (req.params.method.startsWith("_")) {
      next();
    } else {

      try { 

        let validationError = req.validationErrors();

        if (validationError.length){
          throw { 
            status: HTTP_STATUS_CODE.bad_request.status, 
            message: validationError 
          };
        }

        let result;

        if (req.params.file) {
          result = 
          await controllers[req.params.version][req.params.controller][req.params.file][req.params.method](req);
        } else {
          result = 
          await controllers[req.params.version][req.params.controller][req.params.method](req);
        }

        if (result.isResponseAStream == true) {
          fs.exists(result.fileNameWithPath, function (exists) {

            if (exists) {

              res.setHeader(
                'Content-disposition', 
                'attachment; filename=' + result.fileNameWithPath.split('/').pop()
              );
              res.set('Content-Type', 'application/octet-stream');
              fs.createReadStream(result.fileNameWithPath.replace(/\.\.\//g, '')).pipe(res);

            } else {

              throw {
                status : HTTP_STATUS_CODE.internal_server_error.status,
                message : HTTP_STATUS_CODE.internal_server_error.message
              };

            }

          });

        } else {
          res.status(result.status ? result.status : HTTP_STATUS_CODE["ok"].status).json({
            message: result.message,
            status: result.status ? result.status : HTTP_STATUS_CODE["ok"].status,
            result: result.data,
            result: result.result,
            total: result.total,
            count : result.count
          });
        }

          console.log('-------------------Response log starts here-------------------');
          console.log(JSON.stringify(result));
          console.log('-------------------Response log ends here-------------------');
      }
      catch (error) {
        res.status(error.status ? error.status : HTTP_STATUS_CODE.bad_request.status).json({
          status: error.status ? error.status : HTTP_STATUS_CODE.bad_request.status,
          message: error.message,
          result : error.result
        });
        
      };
    }
  };

  app.all("/:version/:controller/:method", inputValidator, router);
  app.all("/:version/:controller/:file/:method", inputValidator, router);
  app.all("/:version/:controller/:method/:_id", inputValidator, router);
  app.all("/:version/:controller/:file/:method/:_id", inputValidator, router);

  app.use((req, res, next) => {
    res.status(HTTP_STATUS_CODE["not_found"].status).send(HTTP_STATUS_CODE["not_found"].message);
  });
};
