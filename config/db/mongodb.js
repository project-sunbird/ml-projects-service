/**
 * name : mongodb.js.
 * author : Aman Karki.
 * created-date : 13-July-2020.
 * Description : mongodb configurations.
 */

// dependencies
const mongoose = require("mongoose");
const mongoose_delete = require("mongoose-delete");
const mongoose_autopopulate = require("mongoose-autopopulate");
const mongoose_timestamp = require("mongoose-timestamp");
const mongoose_ttl = require("mongoose-ttl");
const ObjectId = mongoose.Types.ObjectId;

/**
 * Mongodb setup.
 * @method
 * @name DB
*/

const DB = function() {
  mongoose.set('useCreateIndex', true)
  mongoose.set('useFindAndModify', false)
  mongoose.set('useUnifiedTopology', true)
  
  const db = mongoose.createConnection(
    process.env.MONGODB_URL,
    {
      useNewUrlParser: true
    }
  );
  
  db.on("error", console.error.bind(console, "connection error:"));
  db.once("open", function() {
    console.log("Connected to DB");
  });

  const createModel = function(opts) {
    if (typeof opts.schema.__proto__.instanceOfSchema === "undefined") {
      var schema = mongoose.Schema(opts.schema, opts.options);
    } else {
      var schema = opts.schema;
    }

    // apply Plugins
    schema.plugin(mongoose_timestamp, {
      createdAt: "createdAt",
      updatedAt: "updatedAt"
    });
    schema.plugin(mongoose_autopopulate);
    schema.plugin(mongoose_delete, { overrideMethods: true, deletedAt: true });

    // Expire at
    if (opts.options) {
      if (
        opts.options.expireAfterSeconds ||
        opts.options.expireAfterSeconds === 0
      ) {
        log.debug("Expire Configured for " + opts.name);
        schema.plugin(mongoose_ttl, {
          ttl: opts.options.expireAfterSeconds * 1000
        });
      }
    }

    var model = db.model(opts.name, schema, opts.name);
    return model;
  };

  const runCompoundIndex = function(modelName,opts) {
    if (opts && opts.length > 0) {
      for ( let indexPointer = 0 ; indexPointer < opts.length ; indexPointer++ ) {
        let currentIndex = opts[indexPointer];
        db.collection(modelName).createIndex(currentIndex.name, currentIndex.indexType);
      }
    }
  };

  return {
    database: db,
    createModel: createModel,
    ObjectId: ObjectId,
    models: db.models,
    runCompoundIndex: runCompoundIndex
  };
};

module.exports = DB;
