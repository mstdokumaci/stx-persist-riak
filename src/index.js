const rocksdb = require('rocksdb')

const define = function (object, key, value) {
  Object.defineProperty(object, key, { value, configurable: true })
}

const PersistRocksDB = function (name) {
  this.db = rocksdb(name)
}

define(PersistRocksDB.prototype, 'start', function () {
  return new Promise(
    (resolve, reject) => this.db.open(error => error ? reject(error) : resolve())
  )
})

define(PersistRocksDB.prototype, 'store', function (key, value) {
  this.db.put(key, JSON.stringify(value), error => {
    if (error) {
      console.error(error)
    }
  })
})

define(PersistRocksDB.prototype, 'remove', function (key) {
  this.db.del(key, error => {
    if (error) {
      console.error(error)
    }
  })
})

define(PersistRocksDB.prototype, 'load', function (loadLeaf) {
  const iterator = this.db.iterator({
    keyAsBuffer: false,
    valueAsBuffer: false
  })

  return new Promise((resolve, reject) => {
    const consumeIterator = (error, key, value) => {
      if (error) {
        reject(error)
      } else if (key && value) {
        loadLeaf(key, JSON.parse(value))
        iterator.next(consumeIterator)
      } else {
        resolve()
      }
    }

    iterator.next(consumeIterator)
  })
})

define(PersistRocksDB.prototype, 'stop', function () {
  return new Promise((resolve, reject) => this.db.close(
    error => error ? reject(error) : resolve()
  ))
})

export { PersistRocksDB }
