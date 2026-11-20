#!/usr/bin/env node
// yarn start:go-debug <service>: the project's own "yarn start:dev <service>" with GoDebug loaded.
process.argv.splice(2, 0, 'yarn', 'start:dev');
require('./go-debug.cjs');
