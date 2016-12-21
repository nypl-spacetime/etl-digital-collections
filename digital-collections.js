const fs = require('fs')
const path = require('path')
const R = require('ramda')
const H = require('highland')
const mkdirp = require('mkdirp')

const getFilename = (currentDir, dataset) =>
  path.join(currentDir, '..', '..', 'transform', dataset, `${dataset}.objects.ndjson`)

const datasets = {
  'oldnyc': {
    getUuid: (object) => object.data && object.data.uuid,
    transformToDc: (object) => ({
      id: object.id,
      url: object.data.url,
      geometry: object.geometry
    })
  },
  'mapwarper': {
    getUuid: (object) => object.data && object.data.uuid,
    transformToDc: (object) => ({
      id: object.id,
      url: `http://maps.nypl.org/warper/maps/${object.id}`,
      geometry: object.geometry
    })
  }
}

function getLines (obj) {
  return H(fs.createReadStream(obj.filename))
    .split()
    .compact()
    .map(JSON.parse)
    .map((object) => ({
      dataset: obj.dataset,
      object
    }))
}

function writeContents(dir, uuid, contents, callback) {
  // const filename = path.join(dir, ...uuid.split('-')) + '.json'
  // mkdirp.sync(path.dirname(filename))

  // const filename = path.join(dir, `${uuid}.json`)
  // fs.writeFileSync(filename, JSON.stringify(contents, null, 2))

  callback()
}

function writeLines (dir, uuid, lines, callback) {
  H(lines)
    .group('dataset')
    .map(R.toPairs)
    .sequence()
    .map(R.zipObj(['dataset', 'lines']))
    .map((data) => Object.assign(data, {
      lines: data.lines.map((line) => datasets[line.dataset].transformToDc(line.object))
    }))
    .errors(console.error)
    .map((data) => ([
      data.dataset, data.lines
    ]))
    .toArray((data) => {
      const contents = R.fromPairs(data)
      writeContents(dir, uuid, contents, callback)
    })
}

function aggregate (config, dirs, tools, callback) {
  H(Object.keys(datasets))
    .map((dataset) => ({
      dataset,
      filename: getFilename(dirs.current, dataset)
    }))
    .map(getLines)
    .sequence()
    .filter((line) => datasets[line.dataset].getUuid && datasets[line.dataset].transformToDc)
    .filter((line) => datasets[line.dataset].getUuid(line.object))
    .group((line) => datasets[line.dataset].getUuid(line.object))
    .map(R.toPairs)
    .sequence()
    .map(R.zipObj(['uuid', 'lines']))
    .map((uuidLines) => H.curry(writeLines, dirs.current)(uuidLines.uuid, uuidLines.lines))
    .nfcall([])
    .series()
    .done(callback)
}

// ==================================== API ====================================

module.exports.steps = [
  aggregate
]