const {buildRing, pickNode} = require("./hash.js");
const http = require("http");
const url = require("url");
const { parseArgs } = require("node:util");

const args = parseArgs({
        options: {
            port: { type: 'string', default: '4000' },
            shards: { type: 'string', default: '' },
            vnodes: { type: 'string', default: '100' },
        }
    }).values;

    const PORT = Number(args.port);
    const SHARDS = (args.shards || '').split(',').map(s => s.trim()).filter(Boolean);
    const vnodeCount = Math.max(10, Number(args.vnodes || 100));
    if (!SHARDS.length) throw new Error('Provide --shards list');

    const ring = buildRing(SHARDS, vnodeCount);

    function json(res, code, obj) {
        res.writeHead(code, { 'content-type': 'application/json' });
        res.end(JSON.stringify(obj));
    }

    async function proxy(method, shardBase, key, bodyStr) {
        const target = new URL(`/kv?key=${encodeURIComponent(key)}`, shardBase).toString();
        const resp = await fetch(target, {
            method,
            headers: { 'content-type': 'text/plain' },
            body: method === 'PUT' ? bodyStr : undefined,
        });
        const text = await resp.text();
        return { status: resp.status, body: text };
    }

    const server = http.createServer(async (req, res) => {
        const parsed = url.parse(req.url, true);

        if (parsed.pathname === '/route' && req.method === 'GET') {
            const key = parsed.query.key;
            if (!key) return json(res, 400, { error: 'missing key' });
            const node = pickNode(ring, key);
            return json(res, 200, { key, shard: node });
        }

        if (parsed.pathname === '/kv') {
            const key = parsed.query.key;
            if (!key) return json(res, 400, { error: 'missing key' });
            const node = pickNode(ring, key);

            if (req.method === 'GET') {
                const r = await proxy('GET', node, key);
                res.writeHead(r.status, { 'content-type': 'application/json' });
                return res.end(r.body);
            }

            if (req.method === 'PUT') {
                let body = '';
                req.on('data', c => (body += c));
                req.on('end', async () => {
                    const r = await proxy('PUT', node, key, body);
                    res.writeHead(r.status, { 'content-type': 'application/json' });
                    res.end(r.body);
                });
                return;
            }

            if (req.method === 'DELETE') {
                const r = await proxy('DELETE', node, key);
                res.writeHead(r.status, { 'content-type': 'application/json' });
                return res.end(r.body);
            }

            return json(res, 405, { error: 'method not allowed' });
        }

        if (parsed.pathname === '/health' && req.method === 'GET') {
            return json(res, 200, { status: 'ok', shards: SHARDS.length, vnodes: vnodeCount });
        }

        json(res, 404, { error: 'not found' });
    });

    server.listen(PORT, () => console.log(`[coord] listening on ${PORT} shards=${SHARDS.length} vnodes=${vnodeCount}`));
