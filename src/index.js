const Riak = require('basho-riak-client')

const define = function (object, key, value) {
  Object.defineProperty(object, key, { value, configurable: true })
}

const PersistRiak = function (nodes, bucket) {
  this.nodes = nodes
  this.bucket = bucket
  this.syncing = 0
  this.onSynced = null
}

define(PersistRiak.prototype, 'start', function () {
  return new Promise((resolve, reject) => {
    this.client = new Riak.Client(
      this.nodes, error => error ? reject(error) : resolve()
    )
  })
})

define(PersistRiak.prototype, 'store', function (key, value) {
  this.syncing++
  this.client.storeValue({ bucket: this.bucket, key, value }, error => {
    if (error) {
      console.error(error)
    }
    this.synced()
  })
})

define(PersistRiak.prototype, 'remove', function (key) {
  this.syncing++
  this.client.deleteValue({ bucket: this.bucket, key }, error => {
    if (error) {
      console.error(error)
    }
    this.synced()
  })
})

define(PersistRiak.prototype, 'synced', function () {
  if (--this.syncing === 0 && this.onSynced) {
    const onSynced = this.onSynced
    this.onSynced = null
    setImmediate(onSynced)
  }
})

define(PersistRiak.prototype, 'load', function (loadLeaf) {
  const allIds = []

  return new Promise((resolve, reject) => {
    this.client.listKeys(
      {
        bucket: this.bucket
      },
      (error, result) => {
        if (error) {
          reject(error)
        } else {
          allIds.push(...result.keys)

          if (result.done) {
            resolve()
          }
        }
      }
    )
  })
    .then(() => Promise.all(allIds.map(id => new Promise((resolve, reject) => {
      this.client.fetchValue(
        {
          bucket: this.bucket,
          key: id,
          convertToJs: true
        },
        (error, result) => {
          if (error) {
            reject(error)
          } else if (result.values.length) {
            loadLeaf(id, result.values.shift().value)
            resolve()
          } else {
            resolve()
          }
        }
      )
    }))))
})

define(PersistRiak.prototype, 'stop', function () {
  return new Promise((resolve, reject) => {
    if (this.syncing) {
      this.onSynced = () => this.client.stop(
        error => error ? reject(error) : resolve()
      )
    } else {
      this.client.stop(
        error => error ? reject(error) : resolve()
      )
    }
  })
})

export { PersistRiak }
