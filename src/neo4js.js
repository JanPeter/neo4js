import { v1 as neo4j } from 'neo4j-driver';
import Utils from './Utils';
import uuid from 'uuid';
import Debug from 'debug';
const debug = Debug('neo4js');

import neo4jsErrors from './errors';
import Model from './model';
import ModelManager from './model-manager';
import ModelObject from './model-object';
import Query from './query';
import { getSchemaInfo } from './schema-info';

class Neo4js {
  /**
   * Constructor to create a new instance of the Neo4js class
   * @param {String}  username The username which is used to authenticate against the database
   * @param {String}  password The password which is used to authenticate against the database
   * @param {Object}  [options={}] An object with options
   * @param {String}  [options.url=localhost] Bolt url
   * @param {Integer} [options.port=7687] Bolt port
   * @param {Object}  [options.rest={}]
   * @param {String}  [options.rest.url=localhost]
   * @param {Integer} [options.rest.port=7474]
   * @param {Integer} [options.connectionPoolSize=50]
   */
  constructor(username, password, options) {
    this.auth = { username, password };

    const defaultOptions = {
      url: `bolt://localhost`,
      port: 7687,
      rest: {
        url: `http://localhost`,
        port: 7474,
      },
      driver: {
        connectionPoolSize: 50,
      },
    };

    options = Utils._.extend({}, defaultOptions, options || {});
    options.rest = Utils._.extend({}, defaultOptions.rest, options.rest);
    options.driver = Utils._.extend({}, defaultOptions.driver, options.driver);

    // TODO: Maybe add here some more checks...
    const url = `${options.url}:${options.port}`;
    this.driver = neo4j.driver(url, neo4j.auth.basic(username, password), options.driver);

    this.options = options;

    this.models = {};
    this.modelManager = new ModelManager(this);
  }

  /**
   * @param {String | String[]} labels
   * @param {Object} schema
   * @param {Object} schema.{propertyName}
   * @param {Boolean} schema.{propertyName}.index
   * @param {Boolean} schema.{propertyName}.unique
   * @param {Boolean} schema.{propertyName}.exists
   * @param {Function} schema.{propertyName}.defaultValue
   */
  define(labels, schema) {
    const model = new Model(labels, schema, this);
    this.modelManager.add(model);
    return model;
  }

  /**
   * @param {String} label
   */
  getModel(label) {
    const model = this.modelManager.getModel(label);
    return model;
  }

  /**
   * @returns {Promise}
   */
  getSchemaInfo() {
    return getSchemaInfo(this.options.rest.url, this.options.rest.port, this.auth);
  }

  drop() {
    return this.getSchemaInfo()
      .then(result => {
        const cmds = [];
        for (const label in result) {
          for (const p in result[label]) {
            if (result[label][p].index) {
              cmds.push(new Query(this).dropIndex(label, p));
            }
            if (result[label][p].unique) {
              cmds.push(new Query(this).dropConstraint(label, p, 'unique'));
            }
          }
        }
        cmds.push(new Query(this).match('n').detach('n'));
        return Query.runSequence(cmds);
      });
  }

  /**
   * @param {Object} options
   * @param {Boolean} options.force
   */
  sync(options) {
    debug(`sync start`);
    options = options || {};

    return Promise.resolve()
      .then(() => {
        if (options.force) {
          debug(`cleanup database start`);
          return this.drop();
        }
      })
      .then(() => {
        if (options.force) {
          debug(`cleanup database end`);
        }
        const promises = this.modelManager.all.map(m => m.sync());
        return Promise.all(promises);
      })
      .then(syncResults => {
        const errors = [];
        for (const results of syncResults) {
          for (const result of results) {
            if (result.err) {
              errors.push(result.err);
            }
          }
        }
        return errors;
      })
      .then(result => {
        debug(`sync end`);
        return result;
      })
      .catch(err => {
        debug(`sync end with errors`, err);
        return err;
      });
  }

  /**
   * This method creates a new instance of a Query
   * @return {Query}
   */
  Query() {
    return new Query(this);
  }

  /**
   * This method runs a command with params
   * @param {String} cmd - Cypher statement to execute
   * @param {Object} params - Map with parameters to use in statement
   * @return {Promise} Resolves either with the result or the error message
   */
  run(cmd, params) {
    const session = this.driver.session();
    return session.run(cmd, params)
      .then(result => {
        session.close();
        return result;
      })
      .catch(err => {
        session.close();
        return err;
      });
  }

  /**
   * @returns {UUIDv4}
   */
  static uuid4() {
    return uuid.v4();
  }

  /**
   * @returns {UUIDv1}
   */
  static uuid1() {
    return uuid.v1();
  }
}

Neo4js.prototype.Error = Neo4js.Error = neo4jsErrors.BaseError;
Neo4js.prototype.Model = Neo4js.Model = Model;
Neo4js.prototype.ModelObject = Neo4js.ModelObject = ModelObject;

export default Neo4js;
