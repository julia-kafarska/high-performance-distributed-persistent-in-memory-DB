const crypto = require("crypto");

if (require.main === module) {
    const ring = buildRing(['A','B','C'], 10);
    console.log('A few picks:', pickNode(ring, 'foo'), pickNode(ring, 'bar'), pickNode(ring, 'baz'));
}

function buildRing(nodes, vnodes = 100) {
    const ring = [];
    for (const n of nodes) {
        for (let i = 0; i < vnodes; i++) {
            const h = crypto.createHash('sha1').update(n + '#' + i).digest();
            const hashInt = h.readUInt32BE(0);
            ring.push({ hash: hashInt, node: n });
        }
    }
    ring.sort((a, b) => a.hash - b.hash);
    return ring;
}

function pickNode(ring, key) {
    const h = crypto.createHash('sha1').update(String(key)).digest();
    const hashInt = h.readUInt32BE(0);
    let lo = 0, hi = ring.length - 1;
    while (lo <= hi) {
        const mid = (lo + hi) >>> 1;
        if (ring[mid].hash < hashInt) lo = mid + 1; else hi = mid - 1;
    }
    return (ring[lo] || ring[0]).node;
}

module.exports = { buildRing, pickNode };
