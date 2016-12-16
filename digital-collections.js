const fs = require('fs')
const path = require('path')
const R = require('ramda')
const H = require('highland')
const mkdirp = require('mkdirp')

const oldnycFile = '/Users/bertspaan/data/spacetime/etl/transform/oldnyc/oldnyc.objects.ndjson'

function createFile (dir, data) {
  const filename = path.join(dir, ...data.uuid.split('-')) + '.json'
  const contents = {
    oldnyc: {
      locations: data.locations.map((location) => ({
        id: location.id,
        url: location.data.url,
        geometry: location.geometry
      }))
    }
  }

  mkdirp.sync(path.dirname(filename))
  fs.writeFileSync(filename, JSON.stringify(contents, null, 2))
}

function aggregate (config, dirs, tools, callback) {
  H(fs.createReadStream(oldnycFile))
    .split()
    .compact()
    .map(JSON.parse)
    .filter((object) => object.data.uuid)
    .group((object) => object.data.uuid)
    .map(R.toPairs)
    .sequence()
    .map(R.zipObj(['uuid', 'locations']))
    .each(H.curry(createFile, dirs.current))
    .done(callback)
}

// ==================================== API ====================================

module.exports.steps = [
  aggregate
]