#!/usr/bin/env node

'use strict';

const { Client } = require('pg');
const copyFrom = require('pg-copy-streams').from;
const program = require('commander');
const { Readable, Transform } = require('stream');
const csv = require('fast-csv');

const TABLE_NAME = 'test_epv';
const COLUMNS = [ 'first', 'last', 'amount', 'date' ];

class LogStream extends Transform {
    constructor() {
        super({ objectMode: true });
        this.cnt = 0;
    }

    _transform(record, enc, done) {
        if (++this.cnt % 100 === 0) console.log(`${this.cnt} records written`);
        done(null, record);
    }
}

class EPVReadStream extends Readable {
    constructor(size) {
        super({ objectMode: true });
        this.size = size;
        this.index = 0;
    }

    _read(size) {
        this.push({ first: 'Test', last: `User ${this.index++}`, amount: 200000, date: new Date().toISOString() });
        if (this.index >= this.size) this.push(null);
    }
}

program
    .option('-U, --user <user>', 'user', 'postgres')
    .option('-p, --password <password>', 'password')
    .option('-h, --host <host>', 'host', 'localhost')
    .option('-p, --port <port>', 'port', v => parseInt(v), 5432)
    .option('-d, --database <database>', 'database', 'informer')
    .option('-n, --records <records>', 'number of records', v => parseInt(v), 100);

function createTable(client) {
    return client.query(`CREATE TABLE IF NOT EXISTS "${TABLE_NAME}" (first text, last text, amount decimal, date timestamptz)`);
}

function createCsvStream() {
    return csv.createWriteStream({ readableObjectMode: false, headers: [ 'first', 'last', 'amount', 'date' ] });
}

function createCopyStream(client) {
    return client.query(copyFrom(`COPY "${TABLE_NAME}" (${COLUMNS.map(c => `${c}`).join(',')}) FROM STDIN WITH (FORMAT csv, HEADER true)`))
}

async function stream(client, size) {
    return new Promise((resolve, reject) => {
        new EPVReadStream(size)
            .pipe(new LogStream())
            .pipe(createCsvStream())
            .pipe(createCopyStream(client))
            .on('error', reject)
            .on('end', resolve);
    });
}

async function run() {
    program.parse(process.argv);

    const client = new Client(program);

    await client.connect();

    try {
        await createTable(client);
        await stream(client, program.records);
    } finally {
        client.end();
    }
}

run().catch(err => console.error(err));
