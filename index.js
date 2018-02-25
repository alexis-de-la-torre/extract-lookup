const { readdir, readFile } = require('fs')
const { Future, encaseN, parallel } = require('fluture')
const R = require('ramda')
const { connect, createTable, deleteTable, q, saveBatch } = require('postgres-future')
const { parse } = require('node-xlsx')

const folder = process.argv[2]

const data =
  encaseN(readdir, folder)
  .map(R.reject(R.propEq(0, '.'))) // remove dot files
  .map(R.map(file => ({ file, path: folder.concat(file) })))

const tablesQuery = `
SELECT table_schema,table_name
FROM information_schema.tables
ORDER BY table_schema,table_name
`

const db =
  connect({})
  .chain(
    db =>
    q(db, tablesQuery)
    .map(R.prop('rows'))
    .map(R.filter(x => x.table_name.includes('lookup_')))
    .map(R.pluck('table_name'))
    .map(R.map(table => deleteTable(db, table)))
    .map(futures => ({ db, futures }))
  )
  .chain(({ db, futures }) => parallel(Infinity, futures).map(_ => db))

const program = db => R.pipe(
  Future.of,
  R.chain(
    ({ file, path }) =>
    encaseN(readFile, path)
    .map(parse)
    .map(R.prop(0))
    .map(R.prop('data'))
    .map(x => x.slice(1).map(R.zipObj(x[0])))
    .map(data => ({ table: 'lookup_' + file.replace('.xlsx', ''), data }))
  ),
  R.chain(
    ({ table, data }) =>
    createTable(db, table, R.zipObj(R.keys(data[0]), R.repeat('TEXT', R.values(data[0]).length)))
    .chain(_ => saveBatch(db, table, data))
  )
)

parallel(2, [ db, data ])
  .map(R.zipObj([ 'db', 'data' ]))
  .map(({ db, data }) => data.map(program(db)))
  .chain(parallel(25))
  .fork(console.log, console.log)
