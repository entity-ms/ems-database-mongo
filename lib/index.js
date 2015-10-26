/**
 * Provides the Mongo Database component.
 *
 * @author Orgun109uk <orgun109uk@gmail.com>
 */

var util = require('util'),
    Database = require('ems-database');

/**
 * Provides the Mongo Database component.
 *
 * @class DatabaseMongoComponent
 * @extends DatabaseComponent
 * @constructor
 * @param {EntityMS} central The EntityMS object managing this component.
 * @param {String} name The name of this component.
 * @param {Object} [config] The components configuration object.
 */
function DatabaseMongoComponent(central, name, config) {
  'use strict';

  DatabaseMongoComponent.super_.call(this, central, name, config, {
    title: 'Database (Memory) Component',
    description: 'Provides a memory database component.',
    config: {
      host: '0.0.0.0',
      port: 27017,
      database: 'ems-database'
    }
  });

  var me = this,
      url = 'mongodb://';

  url += this.config.database.host + ':' + this.config.database.port + '/' +
    this.config.database.database;

  require('mongodb').MongoClient.connect(url, function (err, db) {
    if (err) {
      throw err;
    }

    Object.defineProperties(me, {
      /**
       * @todo
       */
      db: {
        get: function () {
          return db;
        }
      }
    });

    //db.open();
  });
}

util.inherits(DatabaseMongoComponent, Database);

/**
 * An internal callback for use with overriding database components to connect
 * to the respective database services.
 *
 * @method _connect
 * @private
 */
DatabaseMongoComponent.prototype._connect = function () {
  'use strict';

  // Does nothing.
};

/**
 * An internal callback for use with overriding database components to
 * disconnect from the respective database services.
 *
 * @method _disconnect
 * @private
 */
DatabaseMongoComponent.prototype._disconnect = function () {
  'use strict';

  this.db.close();
};

/**
 * An internal callback for use with overriding database components to perform a
 * query on the database service.
 *
 * @method _query
 * @param {String} collection The collection/table to perform the query on.
 * @param {RQL/Query} query An RQL Query object.
 * @param {Function} done The done callback.
 *   @param {Error} done.err Any raised errors.
 *   @param {Array} done.results An array of matching documents/results.
 * @private
 * @async
 */
DatabaseMongoComponent.prototype._query = function (collection, query, done) {
  'use strict';

  var qry = require('mongo-rql')(query);

  this.db.collection(collection).find(qry.criteria, {
    skip: qry.skip,
    limit: qry.limit,
    fields: qry.projection,
    sort: qry.sort
  }).toArray(function (err, docs) {
    console.info(docs);
    done(err ? err : null, err ? [] : docs);
  });
};

/**
 * An internal callback for use with overriding database components to perform
 * an insert on the database service.
 *
 * @method _insert
 * @param {String} collection The collection/table to perform the query on.
 * @param {Array} data An array of documents/rows to insert.
 * @param {Function} done The done callback.
 *   @param {Error} done.err Any raised errors.
 *   @param {Array} done.ids An array of IDs from the inserts.
 * @private
 * @async
 */
DatabaseMongoComponent.prototype._insert = function (collection, data, done) {
  'use strict';

  this.db.collection(collection).insert(data, {
    safe: true
  }, function (err, results) {
    if (err) {
      console.log(err);
    }

    done(null, results.insertedIds);
  });
};

/**
 * An internal callback for use with overriding database components to perform
 * an update on the database service.
 *
 * @method _update
 * @param {String} collection The collection/table to perform the query on.
 * @param {String} id The ID of the document to update.
 * @param {Object} data The data to update the document with.
 * @param {Function} done The done callback.
 *   @param {Error} done.err Any raised errors.
 *   @param {Boolean} done.success True if the document was successfully
 *     updated.
 * @private
 * @async
 */
DatabaseMongoComponent.prototype._update = function (
  collection, id, data, done
) {
  'use strict';

  this.db.collection(collection).update({
    _id: /* eslint-disable */require('mongodb').ObjectID(id)/* eslint-enable */
  }, data, function (err, doc) {
    done(err ? err : null);
  });
};

/**
 * An internal callback for use with overriding database components to perform a
 * select/load on the database service.
 *
 * @method _get
 * @param {String} collection The collection/table to perform the query on.
 * @param {String} id The ID of the document to load.
 * @param {Function} done The done callback.
 *   @param {Error} done.err Any raised errors.
 *   @param {Object} done.document The loaded document/result.
 * @private
 * @async
 */
DatabaseMongoComponent.prototype._get = function (collection, id, done) {
  'use strict';

  this.db.collection(collection).find({
    _id: /* eslint-disable */require('mongodb').ObjectID(id)/* eslint-enable */
  }).toArray(function (err, docs) {
    console.info(arguments);
    if (err) {
      return done(err);
    }

    if (docs.length === 0) {
      return done(new Error('Failed to find document with ID "' + id + '".'));
    }

    done(null, docs[0]);
  });
};

/**
 * An internal callback for use with overriding database components to perform a
 * delete on the database service.
 *
 * @method _delete
 * @param {String} collection The collection/table to perform the query on.
 * @param {String} id The ID of the document to delete.
 * @param {Function} done The done callback.
 *   @param {Error} done.err Any raised errors.
 *   @param {Boolean} done.success True if the document had been successfully
 *     deleted.
 * @private
 * @async
 */
DatabaseMongoComponent.prototype._delete = function (collection, id, done) {
  'use strict';

  this.db.collection(collection).remove({
    _id: /* eslint-disable */require('mongodb').ObjectID(id)/* eslint-enable */
  }, {
    safe: true,
    single: true
  }, function (err) {
    done(err ? err : null);
  });
};

/**
 * An internal callback for use with overriding database components to perform a
 * clear on the database service.
 *
 * @method _clear
 * @param {String} collection The collection/table to perform the query on.
 * @param {Function} done The done callback.
 *   @param {Error} done.err Any raised errors.
 * @private
 * @async
 */
DatabaseMongoComponent.prototype._clear = function (collection, done) {
  'use strict';

  this.db.collection(collection).remove({}, {
    safe: true
  }, function (err) {
    done(err ? err : null);
  });
};

/**
 * Exports the DatabaseMongoComponent class.
 */
module.exports = DatabaseMongoComponent;
