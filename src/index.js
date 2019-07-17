const Riak = require('basho-riak-client')

const define = function (object, key, value) {
  Object.defineProperty(object, key, { value, configurable: true })
}

const PersistRiak = function (nodes, bucket) {
  this.nodes = nodes
  this.bucket = bucket
  this.syncing = false
  this.onSynced = null
}

define(PersistRiak.prototype, 'start', function (state, addToStrings, getString) {
  this.branch = state.branch
  this.addToStrings = addToStrings
  this.getString = getString

  state.on('allData', (type, _, item) => {
    if (this.branch.leaves[item.id]) {
      const leaf = {}
      for (const key in this.branch.leaves[item.id]) {
        leaf[key] = this.branch.leaves[item.id][key]
      }
      leaf.keyString = this.getString(leaf.key)

      this.syncing = true
      if (type === 'remove') {
        this.client.deleteValue(
          {
            bucket: this.bucket,
            key: String(item.id)
          },
          error => {
            if (error) {
              console.error(error)
            }
            this.synced()
          }
        )
      } else {
        this.client.storeValue(
          {
            bucket: this.bucket,
            key: String(item.id),
            value: leaf
          },
          error => {
            if (error) {
              console.error(error)
            }
            this.synced()
          }
        )
      }
    }
  })

  return new Promise((resolve, reject) => {
    this.client = new Riak.Client(
      this.nodes, error => error ? reject(error) : resolve()
    )
  })
})

define(PersistRiak.prototype, 'synced', function () {
  setTimeout(() => {
    if (this.onSynced) {
      this.onSynced()
      this.onSynced = null
    }
    this.syncing = false
  })
})

define(PersistRiak.prototype, 'load', function () {
  const allIds = []

  return new Promise((resolve, reject) => {
    this.client.listKeys(
      {
        bucket: this.bucket
      },
      (error, { done, keys }) => {
        if (error) {
          reject(error)
        } else {
          allIds.push(...keys)

          if (done) {
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
        (error, { values }) => {
          if (error) {
            reject(error)
          } else {
            const leaf = values.shift().value
            this.addToStrings(leaf.key, leaf.keyString)
            delete leaf.keyString
            this.branch.leaves[id] = leaf
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
