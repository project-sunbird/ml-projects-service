/**
 * name : globals.js
 * author : Aman Karki
 * created-date : 13-July-2020
 * Description : Globals data.
*/

// dependencies

const fs = require("fs");
const path = require("path");
const requireAll = require("require-all");

module.exports = function () {
  global.async = require("async");
  global.PROJECT_ROOT_DIRECTORY = path.join(__dirname, '..');
  global.MODULES_BASE_PATH = PROJECT_ROOT_DIRECTORY + "/module";
  global.DB_QUERY_BASE_PATH = PROJECT_ROOT_DIRECTORY + "/databaseQueries";
  global.GENERICS_FILES_PATH = PROJECT_ROOT_DIRECTORY + "/generics";
  global.GENERIC_HELPERS_PATH = GENERICS_FILES_PATH + "/helpers";
  global._ = require("lodash");
  global.UTILS = require(GENERIC_HELPERS_PATH + "/utils");

  global.CSV_FILE_STREAM = require(PROJECT_ROOT_DIRECTORY + "/generics/file-stream");
  require("./connections");

  global.HTTP_STATUS_CODE = 
  require(GENERICS_FILES_PATH + "/http-status-codes");

  // Load database models.
  global.models = requireAll({
    dirname: PROJECT_ROOT_DIRECTORY + "/models",
    filter: /(.+)\.js$/,
    resolve: function (Model) {
      return Model;
    }
  });

  //load base v1 controllers
  const pathToController = PROJECT_ROOT_DIRECTORY + "/controllers/v1/";

  fs.readdirSync(pathToController).forEach(function (file) {
    checkWhetherFolderExistsOrNot(pathToController, file);
  });

  /**
 * Check whether folder exists or Not.
 * @method
 * @name checkWhetherFolderExistsOrNot
 * @param {String} pathToFolder - path to folder.
 * @param {String} file - File name.
 */

  function checkWhetherFolderExistsOrNot(pathToFolder, file) {

    let folderExists = fs.lstatSync(pathToFolder + file).isDirectory();

    if (folderExists) {
      fs.readdirSync(pathToFolder + file).forEach(function (folderOrFile) {
        checkWhetherFolderExistsOrNot(pathToFolder + file + "/", folderOrFile);
      })

    } else {
      if (file.match(/\.js$/) !== null) {
        require(pathToFolder + file);
      }
    }

  }

  // Schema for db.
  global.schemas = new Array
  fs.readdirSync(PROJECT_ROOT_DIRECTORY + '/models/').forEach(function (file) {
    if (file.match(/\.js$/) !== null) {
      var name = file.replace('.js', '');
      global.schemas[name] = require(PROJECT_ROOT_DIRECTORY + '/models/' + file);
    }
  });
  

  // All controllers
  global.controllers = requireAll({
    dirname: PROJECT_ROOT_DIRECTORY + "/controllers",
    resolve: function (Controller) {
      return new Controller();
    }
  });

  // Message constants
  global.CONSTANTS = new Array
  fs.readdirSync(GENERICS_FILES_PATH + "/constants")
  .forEach(function (file) {
    if (file.match(/\.js$/) !== null) {
      let name = file.replace('.js', '');
      name = UTILS.hyphenCaseToCamelCase(name);
      global.CONSTANTS[name] = 
      require(GENERICS_FILES_PATH + "/constants/" + file);
    }
  });


  // KAFKA CONSUMERS

  fs.readdirSync(PROJECT_ROOT_DIRECTORY + "/generics/kafka/consumers")
  .forEach(function (file) {
    if (file.match(/\.js$/) !== null) {
      var name = file.replace('.js', '');
       global[name + 'Consumer'] = 
      require(PROJECT_ROOT_DIRECTORY + "/generics/kafka/consumers/" + file);
    }
  });

};
