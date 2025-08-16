const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

function makeFastDB({ dataDir, walFilename = 'wal.log', snapshotFilename = 'snapshot.json.gz', snapshotIntervalMs = 10_000, flushIntervalMs = 2 }) {
    fs.mkdirSync(dataDir, { recursive: true });
    const WAL_FILE = path.join(dataDir, walFilename);
    const SNAPSHOT_FILE = path.join(dataDir, snapshotFilename);

    const memTable = new Map();
    let walFd = fs.openSync(WAL_FILE, 'a+');
    const walQueue = [];
    let closed = false;

    function serializeRecord(op, key, value) {
        const keyBuf = Buffer.from(String(key));
        const serializedValue = typeof value === 'object' ? JSON.stringify(value) : String(value);
        const valBuf = Buffer.from(serializedValue);
        const buf = Buffer.alloc(1 + 4 + 4 + keyBuf.length + valBuf.length);
        buf.writeUInt8(op === 'put' ? 1 : 2, 0);
        buf.writeUInt32BE(keyBuf.length, 1);
        buf.writeUInt32BE(valBuf.length, 5);
        keyBuf.copy(buf, 9);
        valBuf.copy(buf, 9 + keyBuf.length);
        return buf;
    }

    function applyRecord(op, key, value) {
        if (op === 'put') {
            memTable.set(key, { value, ts: Date.now() });
        } else if (op === 'del') {
            memTable.delete(key);
        }
    }

    function put(key, value) {
        if (closed) throw new Error('DB closed');
        const buf = serializeRecord('put', key, value);
        walQueue.push(buf);
        applyRecord('put', key, value);
    }

    function get(key) {
        return memTable.get(key) || null;
    }

    function del(key) {
        if (closed) throw new Error('DB closed');
        const buf = serializeRecord('del', key, '');
        walQueue.push(buf);
        applyRecord('del', key);
    }

    function takeSnapshot() {
        const json = JSON.stringify(Array.from(memTable.entries()));
        const gz = zlib.gzipSync(json);
        fs.writeFileSync(SNAPSHOT_FILE, gz);
        return { keys: memTable.size };
    }

    function recover() {
        if (fs.existsSync(SNAPSHOT_FILE)) {
            try {
                const gz = fs.readFileSync(SNAPSHOT_FILE);
                const json = zlib.gunzipSync(gz).toString();
                const entries = JSON.parse(json);
                memTable.clear();
                for (const [k, v] of entries) memTable.set(k, v);
            } catch (e) {
                console.error('[recovery] snapshot failed:', e);
            }
        }

        if (fs.existsSync(WAL_FILE)) {
            const walData = fs.readFileSync(WAL_FILE);
            let offset = 0;
            while (offset < walData.length) {
                const opCode = walData.readUInt8(offset); offset += 1;
                const keyLen = walData.readUInt32BE(offset); offset += 4;
                const valLen = walData.readUInt32BE(offset); offset += 4;
                const key = walData.slice(offset, offset + keyLen).toString(); offset += keyLen;
                let value = walData.slice(offset, offset + valLen).toString(); offset += valLen;

                try {
                    const parsed = JSON.parse(value);
                    if (typeof parsed === 'object' && parsed !== null) {
                        value = parsed;
                    }
                } catch (e) {
                }

                applyRecord(opCode === 1 ? 'put' : 'del', key, value);
            }
        }
    }

    const flushTimer = setInterval(() => {
        if (!walQueue.length || closed) return;
        const data = Buffer.concat(walQueue.splice(0));
        fs.writeSync(walFd, data);
        fs.fsyncSync(walFd);
    }, flushIntervalMs);

    const snapshotTimer = setInterval(() => {
        if (!closed) takeSnapshot();
    }, snapshotIntervalMs);

    function close() {
        if (closed) return;
        closed = true;
        clearInterval(flushTimer);
        clearInterval(snapshotTimer);
        if (walQueue.length) {
            const data = Buffer.concat(walQueue.splice(0));
            fs.writeSync(walFd, data);
            fs.fsyncSync(walFd);
        }
        try {
            fs.closeSync(walFd);
        } catch (e) {
        }
    }

    recover();

    return { put, get, del, takeSnapshot, close, stats: () => ({ keys: memTable.size }) };
}

module.exports.makeFastDB = makeFastDB;
