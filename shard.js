
const http = require('http');
const url = require('url');
const { makeFastDB } = require('./fastdb.js');

const args = require('node:util').parseArgs({
        options: {
            port: { type: 'string', default: '4101' },
            data: { type: 'string', default: './data' },
            replicas: { type: 'string', default: '' },
            quorum: { type: 'string', default: '1' },
            id: { type: 'string', default: '' },
        }
    }).values;

    const PORT = Number(args.port);
    const DB = makeFastDB({ dataDir: args.data });
    const REPLICAS = (args.replicas || '').split(',').map(s => s.trim()).filter(Boolean);
    const quorumSize = Math.max(1, Number(args.quorum || 1));
    const SHARD_ID = args.id || `shard-${PORT}`;

    function json(res, code, obj) {
        res.writeHead(code, { 'content-type': 'application/json' });
        res.end(JSON.stringify(obj));
    }

    async function forwardWrite(method, baseUrl, key, value, abortSignal) {
        const target = new URL(`/kv?key=${encodeURIComponent(key)}`, baseUrl).toString();
        
        let body, contentType;
        if (typeof value === 'object' && value !== null) {
            body = JSON.stringify(value);
            contentType = 'application/json';
        } else {
            body = String(value);
            contentType = 'text/plain';
        }
        
        const resp = await fetch(target, {
            method,
            headers: { 'content-type': contentType, 'x-forwarded-by': SHARD_ID },
            body: method === 'PUT' ? body : undefined,
            signal: abortSignal,
        });
        if (!resp.ok) throw new Error(`replica ${baseUrl} status ${resp.status}`);
        return true;
    }

    async function replicate(method, key, value) {
        if (!REPLICAS.length) return { acks: 1, quorum: 1, ok: true };
        let acks = 1; // self
        const ac = new AbortController();
        const promises = REPLICAS.map(base => forwardWrite(method, base, key, value, ac.signal)
            .then(() => { acks++; })
            .catch(() => {}));

        while (acks < Math.min(quorumSize, REPLICAS.length + 1)) {
            await new Promise(r => setTimeout(r, 1));
            if (promises.every(p => p.status === 'fulfilled' || p.status === 'rejected')) break;
        }
        if (acks >= quorumSize) ac.abort();
        await Promise.allSettled(promises);
        return { acks, quorum: quorumSize, ok: acks >= quorumSize };
    }

    const server = http.createServer(async (req, res) => {
        const parsed = url.parse(req.url, true);

        if (req.method === 'GET' && parsed.pathname === '/health') {
            return json(res, 200, { status: 'ok', shard: SHARD_ID, port: PORT });
        }

        if (req.method === 'GET' && parsed.pathname === '/stats') {
            return json(res, 200, { shard: SHARD_ID, ...DB.stats() });
        }

        if (parsed.pathname === '/kv') {
            const key = parsed.query.key;
            if (!key) return json(res, 400, { error: 'missing key' });

            if (req.method === 'GET') {
                const v = DB.get(key);
                if (!v) return json(res, 404, { found: false });
                res.writeHead(200, { 'content-type': 'application/json' });
                res.end(JSON.stringify({ found: true, value: v.value, ts: v.ts }));
                return;
            }

            if (req.method === 'PUT') {
                let body = '';
                req.on('data', chunk => (body += chunk));
                req.on('end', async () => {
                    let value = body;
                    
                    const contentType = req.headers['content-type'];
                    if (contentType && contentType.includes('application/json')) {
                        try {
                            value = JSON.parse(body);
                        } catch (e) {
                            return json(res, 400, { error: 'Invalid JSON' });
                        }
                    }
                    
                    DB.put(key, value);
                    const isForwarded = req.headers['x-forwarded-by'];
                    let repl = { acks: 1, quorum: 1, ok: true };
                    if (!isForwarded) repl = await replicate('PUT', key, value);
                    return json(res, repl.ok ? 200 : 500, { ok: repl.ok, acks: repl.acks, quorum: repl.quorum });
                });
                return;
            }

            if (req.method === 'DELETE') {
                DB.del(key);
                const isForwarded = req.headers['x-forwarded-by'];
                const repl = isForwarded ? { acks: 1, quorum: 1, ok: true } : await replicate('DELETE', key, '');
                return json(res, repl.ok ? 200 : 500, { ok: repl.ok, acks: repl.acks, quorum: repl.quorum });
            }

            return json(res, 405, { error: 'method not allowed' });
        }

        json(res, 404, { error: 'not found' });
    });

    server.listen(PORT, () => console.log(`[shard] ${SHARD_ID} listening on ${PORT} replicas=${REPLICAS.length} quorum=${quorumSize}`));

process.on('SIGINT', () => { DB.close(); process.exit(0); });
process.on('SIGTERM', () => { DB.close(); process.exit(0); });
