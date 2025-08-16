
if (require.main === module) {
    (async () => {
        const base = 'http://127.0.0.1:4000';
        const rnd = Math.random().toString(36).slice(2, 7);
        const k1 = `user:${rnd}:1`;
        const k2 = `user:${rnd}:2`;

        let r = await fetch(`${base}/kv?key=${encodeURIComponent(k1)}`, { method: 'PUT', body: 'Alice' });
        console.log('PUT string:', r.status, await r.text());
        
        const userData = { name: 'Bob', age: 30, tags: ['developer', 'nodejs'] };
        r = await fetch(`${base}/kv?key=${encodeURIComponent(k2)}`, { 
            method: 'PUT', 
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify(userData) 
        });
        console.log('PUT object:', r.status, await r.text());

        r = await fetch(`${base}/kv?key=${encodeURIComponent(k1)}`);
        console.log('GET string:', r.status, await r.text());
        
        r = await fetch(`${base}/kv?key=${encodeURIComponent(k2)}`);
        console.log('GET object:', r.status, await r.text());

        r = await fetch(`${base}/kv?key=${encodeURIComponent(k2)}`, { method: 'DELETE' });
        console.log('DELETE:', r.status, await r.text());

        r = await fetch(`${base}/kv?key=${encodeURIComponent(k2)}`);
        console.log('GET deleted:', r.status, await r.text());
    })().catch(e => console.error(e));
}
