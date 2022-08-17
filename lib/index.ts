import Firebird from 'node-firebird';

//errorLog.level = 'debug';

export default class FirebirdTransaction {

  connOptions: any;
  db: any;
  transaction: any;

  constructor(_connOptions:any) {
    this.connOptions = _connOptions;
    this.db = undefined;
    this.transaction = undefined;
  }

  /**
   * Get the firebird connection
   * @returns connection
   */
  async getDbConnection() {
    var connOptions = this.connOptions;
    return new Promise(function (resolve, reject) {
        Firebird.attach(connOptions, function(err:any, db:any) {
            if (err) {
              //errorLog.error('...Reject: ', err);
              reject(err);
              return;
            } else {
              resolve(db);
            }
        });
    });
  }

  /**
   * Start a transaction in DB
   * @param isSequentialRead Define if the transaction is ISOLATION_REPEATABLE_READ, or ISOLATION_READ_COMMITED
   */
  async startTransaction(isSequentialRead:any = undefined) {
    var createTransaction = function (dbConn:any, _isSequentialRead:any) {
      var isolationLevel = Firebird.ISOLATION_READ_COMMITED;
      if (_isSequentialRead) {
          isolationLevel = Firebird.ISOLATION_REPEATABLE_READ;
      }

      return new Promise(function (resolve, reject) {
        //if (this._gerarLog) console.trace('FbTransaction() >> StartTransaction()...');
        dbConn.transaction(isolationLevel, function(err:any, transaction:any) {
          if (err) {
            dbConn.detach();
            reject(err);
            return;
          } else {
            resolve(transaction);
            return;
          }
        });
      });
    }

    if (!this.db) {
        this.db = await this.getDbConnection();
    }
    this.transaction = await createTransaction(this.db, isSequentialRead);
  }

  /**
   * Função para fazer a leitura dos blobs texto existentes no SQL
   * @param rows Linhas lidas do banco de dados para carregar os blobs
   * @param blobFields campos blobs a serem carregados
   * @returns Lista de linhas com os Blobs junto
   */
  async readBlobsFromRows(rows:any, blobFields:any) {
    for(var i = 0; i < rows.length; i++){ // lê a lista que retorna do BD
        for (let x = 0; x < blobFields.length; x++) {
        const field = blobFields[x];
        let blobValue = await this.readBlobPromise(rows[i], field);
        rows[i][field] = blobValue;
        }
    } 
    return rows; // retorna a lista da consulta
  }
  
  
   
  /**
   * Carrega do banco de dados o bloco
   * @param row 
   * @param field 
   * @returns 
   */
  async readBlobPromise(row:any, field:any):Promise<any> {
    return new Promise((resolve, reject) => {
        if (row[field] && row[field] != null && row[field] != undefined) {
            row[field](function(err:any, name:any, eventEmitter:any) {
                let buffers:any = [];
                eventEmitter.on('data', (chunk:any) => {
                buffers.push(chunk);
                });
                eventEmitter.once('end', () => {
                let buffer = buffers.join('');
                resolve(buffer);
                });
                eventEmitter.once('error', (err:any) => {
                reject(err);
                });
            });
        } else {
            resolve(null);
        } 
    });
  }

  /**
   * 
   * @param sql 
   * @param params 
   * @param blobFields 
   * @returns 
   */
  async execute(sql:any, params:any|undefined, blobFields:any|undefined) {
    var _this = this;

    var localExec = async function (transaction:any, _sql:any, _params:any, _blobFields:any) {
        return new Promise(async function (resolve, reject) {
            transaction.query(_sql, _params, async (err:any, result:any) => {
                if (err) {
                    transaction.rollback();
                    reject(err);
                    return;
                } else {
                    if (_blobFields) {
                        result = await _this.readBlobsFromRows(result, _blobFields);
                    } 
                    resolve(result); 
                }
            });
        });
    }
    if (!this.transaction) 
        this.startTransaction(false);
    return await localExec(this.transaction, sql, params, blobFields);
  } 

  /**
   * 
   */
  async commit() {
    var commit = function (dbConn:any, transaction:any) {
        return new Promise<void>(function (resolve, reject) {
            transaction.commit(function(err:any) {
                if (err) {
                    transaction.rollback();
                    reject(err);
                } else {
                    dbConn.detach();
                    resolve();
                }
            });
        });    
    };

    await commit(this.db, this.transaction);
    this.transaction = undefined;
    this.db = undefined;
  }

  /**
   * 
   */
  async rollback() {
    if (this.transaction) {
        this.transaction.rollback();
        this.db.detach();
    }
    this.transaction = undefined;
    this.db = undefined;
  }

}