const test = require('tape')
const { createPersist } = require('stx')
const { PersistRocksDB } = require('../dist')

test('test write', async t => {
  const persist = new PersistRocksDB('db/test')
  await createPersist({ item1: 'value1' }, persist)
  await persist.stop()

  t.end()
})

test('test read', async t => {
  const persist = new PersistRocksDB('db/test')
  const state = await createPersist({}, persist)

  t.same(
    state.serialize(),
    { item1: 'value1' },
    'state = { item1: value1 }'
  )

  state.set({ item1: null })

  await persist.stop()

  t.end()
})

test('test read after delete', async t => {
  const persist = new PersistRocksDB('db/test')
  const state = await createPersist({}, persist)

  t.same(
    state.serialize(),
    {},
    'state = {}'
  )

  await persist.stop()

  t.end()
})
