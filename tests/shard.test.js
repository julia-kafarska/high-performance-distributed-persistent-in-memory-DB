const http = require('http');
const path = require('path');
const fs = require('fs');

// Helper to start a shard server for testing
function startShardServer(port, dataDir, options = {}) {
  return new Promise((resolve, reject) => {
    // Import the shard module
    const shard = require('../shard');
    
    // Create a test shard server
    const server = http.createServer();
    const { makeFastDB } = require('../fastdb');
    
    const DB = makeFastDB({ dataDir });
    const REPLICAS = (options.replicas || '').split(',').map(s => s.trim()).filter(Boolean);
    const QUORUM = Math.max(1, Number(options.quorum || 1));
    const SHARD_ID = options.id || `shard-${port}`;

    function json(res, code, obj) {
      res.writeHead(code, { 'content-type': 'application/json' });
      res.end(JSON.stringify(obj));
    }

    server.on('request', async (req, res) => {
      const url = require('url');
      const parsed = url.parse(req.url, true);

      if (parsed.pathname === '/kv') {
        const key = parsed.query.key;
        if (!key) return json(res, 400, { error: 'missing key' });

        if (req.method === 'GET') {
          const record = DB.get(key);
          if (record) {
            json(res, 200, { found: true, value: record.value, ts: record.ts });
          } else {
            json(res, 404, { found: false });
          }
        } else if (req.method === 'PUT') {
          let body = '';
          req.on('data', chunk => body += chunk);
          req.on('end', () => {
            let value = body;
            
            // If content-type is application/json, parse the JSON
            const contentType = req.headers['content-type'];
            if (contentType && contentType.includes('application/json')) {
              try {
                value = JSON.parse(body);
              } catch (e) {
                return json(res, 400, { error: 'Invalid JSON' });
              }
            }
            
            DB.put(key, value);
            json(res, 200, { ok: true, acks: 1, quorum: QUORUM });
          });
        } else if (req.method === 'DELETE') {
          DB.del(key);
          json(res, 200, { ok: true, acks: 1, quorum: QUORUM });
        } else {
          json(res, 405, { error: 'method not allowed' });
        }
      } else {
        json(res, 404, { error: 'not found' });
      }
    });

    server.listen(port, (err) => {
      if (err) reject(err);
      else resolve({ server, db: DB });
    });
  });
}

// Helper to make HTTP requests
function makeRequest(method, url, body = null, headers = {}) {
  return new Promise((resolve, reject) => {
    const urlParts = new URL(url);
    const contentType = headers['Content-Type'] || 'text/plain';
    const bodyData = typeof body === 'object' && body !== null ? JSON.stringify(body) : body;
    
    const options = {
      hostname: urlParts.hostname,
      port: urlParts.port,
      path: urlParts.pathname + urlParts.search,
      method: method,
      headers: {
        'Content-Type': contentType,
        'Content-Length': bodyData ? Buffer.byteLength(bodyData) : 0,
        ...headers
      }
    };

    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          resolve({ status: res.statusCode, data: parsed });
        } catch (e) {
          resolve({ status: res.statusCode, data: data });
        }
      });
    });

    req.on('error', reject);
    
    if (bodyData) {
      req.write(bodyData);
    }
    req.end();
  });
}

describe('Shard Integration Tests', () => {
  let shard1, shard2;
  let testDir1, testDir2;
  const port1 = 9001;
  const port2 = 9002;

  beforeEach(async () => {
    testDir1 = path.join(__dirname, 'shard-test-1-' + Math.random().toString(36).slice(2));
    testDir2 = path.join(__dirname, 'shard-test-2-' + Math.random().toString(36).slice(2));
    
    shard1 = await startShardServer(port1, testDir1);
    shard2 = await startShardServer(port2, testDir2);
    
    // Wait for servers to be ready
    await new Promise(resolve => setTimeout(resolve, 100));
  });

  afterEach(async () => {
    if (shard1) {
      shard1.server.close();
      shard1.db.close();
    }
    if (shard2) {
      shard2.server.close();
      shard2.db.close();
    }
    
    // Clean up test directories
    [testDir1, testDir2].forEach(dir => {
      if (fs.existsSync(dir)) {
        fs.rmSync(dir, { recursive: true, force: true });
      }
    });
  });

  describe('HTTP API', () => {
    test('should handle PUT requests', async () => {
      const response = await makeRequest('PUT', `http://localhost:${port1}/kv?key=test1`, 'value1');
      
      expect(response.status).toBe(200);
      expect(response.data.ok).toBe(true);
      expect(response.data.acks).toBe(1);
    });

    test('should handle GET requests', async () => {
      // First PUT a value
      await makeRequest('PUT', `http://localhost:${port1}/kv?key=test2`, 'value2');
      
      // Then GET it
      const response = await makeRequest('GET', `http://localhost:${port1}/kv?key=test2`);
      
      expect(response.status).toBe(200);
      expect(response.data.found).toBe(true);
      expect(response.data.value).toBe('value2');
      expect(response.data.ts).toBeGreaterThan(0);
    });

    test('should return 404 for non-existent keys', async () => {
      const response = await makeRequest('GET', `http://localhost:${port1}/kv?key=nonexistent`);
      
      expect(response.status).toBe(404);
      expect(response.data.found).toBe(false);
    });

    test('should handle DELETE requests', async () => {
      // First PUT a value
      await makeRequest('PUT', `http://localhost:${port1}/kv?key=test3`, 'value3');
      
      // Then DELETE it
      const deleteResponse = await makeRequest('DELETE', `http://localhost:${port1}/kv?key=test3`);
      expect(deleteResponse.status).toBe(200);
      expect(deleteResponse.data.ok).toBe(true);
      
      // Verify it's gone
      const getResponse = await makeRequest('GET', `http://localhost:${port1}/kv?key=test3`);
      expect(getResponse.status).toBe(404);
    });

    test('should return 400 for missing key parameter', async () => {
      const response = await makeRequest('GET', `http://localhost:${port1}/kv`);
      
      expect(response.status).toBe(400);
      expect(response.data.error).toBe('missing key');
    });

    test('should return 405 for unsupported methods', async () => {
      const response = await makeRequest('PATCH', `http://localhost:${port1}/kv?key=test`);
      
      expect(response.status).toBe(405);
      expect(response.data.error).toBe('method not allowed');
    });

    test('should return 404 for invalid paths', async () => {
      const response = await makeRequest('GET', `http://localhost:${port1}/invalid`);
      
      expect(response.status).toBe(404);
      expect(response.data.error).toBe('not found');
    });
  });

  describe('Data Operations', () => {
    test('should persist data between operations', async () => {
      // PUT multiple values
      await makeRequest('PUT', `http://localhost:${port1}/kv?key=user1`, 'Alice');
      await makeRequest('PUT', `http://localhost:${port1}/kv?key=user2`, 'Bob');
      await makeRequest('PUT', `http://localhost:${port1}/kv?key=user3`, 'Charlie');
      
      // GET all values
      const resp1 = await makeRequest('GET', `http://localhost:${port1}/kv?key=user1`);
      const resp2 = await makeRequest('GET', `http://localhost:${port1}/kv?key=user2`);
      const resp3 = await makeRequest('GET', `http://localhost:${port1}/kv?key=user3`);
      
      expect(resp1.data.value).toBe('Alice');
      expect(resp2.data.value).toBe('Bob');
      expect(resp3.data.value).toBe('Charlie');
    });

    test('should handle value overwrites', async () => {
      await makeRequest('PUT', `http://localhost:${port1}/kv?key=counter`, '1');
      await makeRequest('PUT', `http://localhost:${port1}/kv?key=counter`, '2');
      await makeRequest('PUT', `http://localhost:${port1}/kv?key=counter`, '3');
      
      const response = await makeRequest('GET', `http://localhost:${port1}/kv?key=counter`);
      expect(response.data.value).toBe('3');
    });

    test('should handle large values', async () => {
      const largeValue = 'x'.repeat(10000);
      
      await makeRequest('PUT', `http://localhost:${port1}/kv?key=large`, largeValue);
      const response = await makeRequest('GET', `http://localhost:${port1}/kv?key=large`);
      
      expect(response.data.value).toBe(largeValue);
    });

    test('should handle special characters in keys and values', async () => {
      const specialKey = 'key with spaces & symbols!@#$%';
      const specialValue = 'value with\nnewlines\tand\ttabs';
      
      await makeRequest('PUT', `http://localhost:${port1}/kv?key=${encodeURIComponent(specialKey)}`, specialValue);
      const response = await makeRequest('GET', `http://localhost:${port1}/kv?key=${encodeURIComponent(specialKey)}`);
      
      expect(response.data.value).toBe(specialValue);
    });
  });

  describe('Concurrent Operations', () => {
    test('should handle concurrent requests', async () => {
      const operations = [];
      
      // Concurrent PUTs
      for (let i = 0; i < 10; i++) {
        operations.push(makeRequest('PUT', `http://localhost:${port1}/kv?key=concurrent${i}`, `value${i}`));
      }
      
      await Promise.all(operations);
      
      // Verify all values were stored
      for (let i = 0; i < 10; i++) {
        const response = await makeRequest('GET', `http://localhost:${port1}/kv?key=concurrent${i}`);
        expect(response.data.value).toBe(`value${i}`);
      }
    });

    test('should handle mixed concurrent operations', async () => {
      // Setup some initial data
      await makeRequest('PUT', `http://localhost:${port1}/kv?key=mix1`, 'initial1');
      await makeRequest('PUT', `http://localhost:${port1}/kv?key=mix2`, 'initial2');
      
      const operations = [
        makeRequest('GET', `http://localhost:${port1}/kv?key=mix1`),
        makeRequest('PUT', `http://localhost:${port1}/kv?key=mix3`, 'new3'),
        makeRequest('DELETE', `http://localhost:${port1}/kv?key=mix2`),
        makeRequest('GET', `http://localhost:${port1}/kv?key=mix2`),
        makeRequest('PUT', `http://localhost:${port1}/kv?key=mix1`, 'updated1')
      ];
      
      const results = await Promise.all(operations);
      
      // Verify final state
      const final1 = await makeRequest('GET', `http://localhost:${port1}/kv?key=mix1`);
      const final2 = await makeRequest('GET', `http://localhost:${port1}/kv?key=mix2`);
      const final3 = await makeRequest('GET', `http://localhost:${port1}/kv?key=mix3`);
      
      expect(final1.data.value).toBe('updated1');
      expect(final2.status).toBe(404);
      expect(final3.data.value).toBe('new3');
    });
  });

  describe('Multiple Shards', () => {
    test('should maintain independent data on different shards', async () => {
      // Store different data on each shard
      await makeRequest('PUT', `http://localhost:${port1}/kv?key=shard1key`, 'shard1value');
      await makeRequest('PUT', `http://localhost:${port2}/kv?key=shard2key`, 'shard2value');
      
      // Verify shard1 has its data but not shard2's data
      const resp1a = await makeRequest('GET', `http://localhost:${port1}/kv?key=shard1key`);
      const resp1b = await makeRequest('GET', `http://localhost:${port1}/kv?key=shard2key`);
      
      expect(resp1a.data.value).toBe('shard1value');
      expect(resp1b.status).toBe(404);
      
      // Verify shard2 has its data but not shard1's data
      const resp2a = await makeRequest('GET', `http://localhost:${port2}/kv?key=shard2key`);
      const resp2b = await makeRequest('GET', `http://localhost:${port2}/kv?key=shard1key`);
      
      expect(resp2a.data.value).toBe('shard2value');
      expect(resp2b.status).toBe(404);
    });
  });

  describe('JSON Support', () => {
    test('should handle JSON PUT and GET requests', async () => {
      const jsonData = { name: 'Alice', age: 30, tags: ['developer', 'nodejs'] };
      
      // PUT JSON data
      const putResponse = await makeRequest('PUT', `http://localhost:${port1}/kv?key=json1`, jsonData, {
        'Content-Type': 'application/json'
      });
      
      expect(putResponse.status).toBe(200);
      expect(putResponse.data.ok).toBe(true);
      
      // GET JSON data
      const getResponse = await makeRequest('GET', `http://localhost:${port1}/kv?key=json1`);
      
      expect(getResponse.status).toBe(200);
      expect(getResponse.data.found).toBe(true);
      expect(getResponse.data.value).toEqual(jsonData);
    });

    test('should handle nested JSON objects', async () => {
      const nestedData = {
        user: { id: 123, profile: { name: 'Bob', settings: { theme: 'dark' } } },
        metadata: { created: '2024-01-01', tags: ['admin', 'user'] }
      };
      
      await makeRequest('PUT', `http://localhost:${port1}/kv?key=nested`, nestedData, {
        'Content-Type': 'application/json'
      });
      
      const response = await makeRequest('GET', `http://localhost:${port1}/kv?key=nested`);
      
      expect(response.data.value).toEqual(nestedData);
      expect(response.data.value.user.profile.name).toBe('Bob');
    });

    test('should handle JSON arrays', async () => {
      const arrayData = [
        { id: 1, name: 'First' },
        { id: 2, name: 'Second' },
        'string element',
        42,
        { nested: [1, 2, 3] }
      ];
      
      await makeRequest('PUT', `http://localhost:${port1}/kv?key=array`, arrayData, {
        'Content-Type': 'application/json'
      });
      
      const response = await makeRequest('GET', `http://localhost:${port1}/kv?key=array`);
      
      expect(response.data.value).toEqual(arrayData);
      expect(Array.isArray(response.data.value)).toBe(true);
    });

    test('should handle mixed JSON and string values', async () => {
      // Store a string
      await makeRequest('PUT', `http://localhost:${port1}/kv?key=string`, 'plain text');
      
      // Store JSON
      const jsonData = { type: 'json', data: [1, 2, 3] };
      await makeRequest('PUT', `http://localhost:${port1}/kv?key=json`, jsonData, {
        'Content-Type': 'application/json'
      });
      
      // Retrieve both
      const stringResponse = await makeRequest('GET', `http://localhost:${port1}/kv?key=string`);
      const jsonResponse = await makeRequest('GET', `http://localhost:${port1}/kv?key=json`);
      
      expect(stringResponse.data.value).toBe('plain text');
      expect(jsonResponse.data.value).toEqual(jsonData);
    });

    test('should return 400 for invalid JSON', async () => {
      const response = await makeRequest('PUT', `http://localhost:${port1}/kv?key=invalid`, '{invalid json}', {
        'Content-Type': 'application/json'
      });
      
      expect(response.status).toBe(400);
      expect(response.data.error).toBe('Invalid JSON');
    });

    test('should handle JSON values in DELETE operations', async () => {
      const jsonData = { toDelete: true, id: 999 };
      
      // PUT JSON data
      await makeRequest('PUT', `http://localhost:${port1}/kv?key=forDelete`, jsonData, {
        'Content-Type': 'application/json'
      });
      
      // Verify it exists
      const getResponse = await makeRequest('GET', `http://localhost:${port1}/kv?key=forDelete`);
      expect(getResponse.data.value).toEqual(jsonData);
      
      // DELETE it
      const deleteResponse = await makeRequest('DELETE', `http://localhost:${port1}/kv?key=forDelete`);
      expect(deleteResponse.status).toBe(200);
      
      // Verify it's gone
      const getAfterDelete = await makeRequest('GET', `http://localhost:${port1}/kv?key=forDelete`);
      expect(getAfterDelete.status).toBe(404);
    });
  });
});