const test = require('tape')
const { createPersist } = require('stx')
const { PersistRiak } = require('../dist')

test('test write', async t => {
  const persist = new PersistRiak([ '127.0.0.1' ], 'state')
  await createPersist({ item1: 'value1' }, persist)
  await persist.stop()

  t.end()
})

test('test read', async t => {
  const persist = new PersistRiak([ '127.0.0.1' ], 'state')
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
