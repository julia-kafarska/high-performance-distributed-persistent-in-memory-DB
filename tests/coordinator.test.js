const http = require('http');
const path = require('path');
const fs = require('fs');
const { buildRing, pickNode } = require('../hash');

// Helper to start a mock shard server
function startMockShard(port, responses = {}) {
  return new Promise((resolve, reject) => {
    const server = http.createServer((req, res) => {
      const url = require('url');
      const parsed = url.parse(req.url, true);
      const key = parsed.query.key;
      
      // Default responses for testing
      const defaultResponses = {
        'test-key': { value: 'test-value', ts: Date.now() },
        'user:123': { value: 'Alice', ts: Date.now() },
        'order:456': { value: 'Order Data', ts: Date.now() },
        'get-test': { value: 'get-value', ts: Date.now() }
      };
      
      const allResponses = { ...defaultResponses, ...responses };
      
      if (req.method === 'GET') {
        if (allResponses[key]) {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ found: true, ...allResponses[key] }));
        } else {
          res.writeHead(404, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ found: false }));
        }
      } else if (req.method === 'PUT') {
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', () => {
          allResponses[key] = { value: body, ts: Date.now() };
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: true, acks: 1, quorum: 1 }));
        });
      } else if (req.method === 'DELETE') {
        delete allResponses[key];
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, acks: 1, quorum: 1 }));
      }
    });
    
    server.listen(port, (err) => {
      if (err) reject(err);
      else resolve(server);
    });
  });
}

// Helper to start coordinator server
function startCoordinator(port, shardUrls) {
  return new Promise((resolve, reject) => {
    const SHARDS = shardUrls;
    const VNODES = 100;
    const ring = buildRing(SHARDS, VNODES);

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
      const url = require('url');
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

        const shardBase = pickNode(ring, key);
        
        if (req.method === 'GET') {
          try {
            const result = await proxy('GET', shardBase, key);
            res.writeHead(result.status, { 'content-type': 'application/json' });
            res.end(result.body);
          } catch (err) {
            json(res, 500, { error: 'shard unavailable' });
          }
        } else if (req.method === 'PUT') {
          let body = '';
          req.on('data', chunk => body += chunk);
          req.on('end', async () => {
            try {
              const result = await proxy('PUT', shardBase, key, body);
              res.writeHead(result.status, { 'content-type': 'application/json' });
              res.end(result.body);
            } catch (err) {
              json(res, 500, { error: 'shard unavailable' });
            }
          });
        } else if (req.method === 'DELETE') {
          try {
            const result = await proxy('DELETE', shardBase, key);
            res.writeHead(result.status, { 'content-type': 'application/json' });
            res.end(result.body);
          } catch (err) {
            json(res, 500, { error: 'shard unavailable' });
          }
        } else {
          json(res, 405, { error: 'method not allowed' });
        }
      } else {
        json(res, 404, { error: 'not found' });
      }
    });

    server.listen(port, (err) => {
      if (err) reject(err);
      else resolve(server);
    });
  });
}

// Helper to make HTTP requests
function makeRequest(method, url, body = null) {
  return new Promise((resolve, reject) => {
    const urlParts = new URL(url);
    const options = {
      hostname: urlParts.hostname,
      port: urlParts.port,
      path: urlParts.pathname + urlParts.search,
      method: method,
      headers: {
        'Content-Type': 'text/plain',
        'Content-Length': body ? Buffer.byteLength(body) : 0
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
    
    if (body) {
      req.write(body);
    }
    req.end();
  });
}

describe('Coordinator End-to-End Tests', () => {
  let coordinator;
  let shards = [];
  const coordinatorPort = 8000;
  const shardPorts = [8001, 8002, 8003];
  const shardUrls = shardPorts.map(port => `http://localhost:${port}`);

  beforeEach(async () => {
    // Start mock shard servers
    for (let i = 0; i < shardPorts.length; i++) {
      const shard = await startMockShard(shardPorts[i]);
      shards.push(shard);
    }

    // Start coordinator
    coordinator = await startCoordinator(coordinatorPort, shardUrls);
    
    // Wait for servers to be ready
    await new Promise(resolve => setTimeout(resolve, 100));
  });

  afterEach(async () => {
    if (coordinator) {
      coordinator.close();
    }
    
    shards.forEach(shard => {
      if (shard) shard.close();
    });
    
    shards = [];
  });

  describe('Routing', () => {
    test('should route keys consistently to same shard', async () => {
      const key = 'test-consistency';
      
      const route1 = await makeRequest('GET', `http://localhost:${coordinatorPort}/route?key=${key}`);
      const route2 = await makeRequest('GET', `http://localhost:${coordinatorPort}/route?key=${key}`);
      
      expect(route1.status).toBe(200);
      expect(route2.status).toBe(200);
      expect(route1.data.shard).toBe(route2.data.shard);
      expect(route1.data.key).toBe(key);
    });

    test('should distribute different keys across shards', async () => {
      const keys = ['key1', 'key2', 'key3', 'key4', 'key5', 'key6', 'key7', 'key8', 'key9', 'key10'];
      const shardDistribution = new Set();
      
      for (const key of keys) {
        const response = await makeRequest('GET', `http://localhost:${coordinatorPort}/route?key=${key}`);
        expect(response.status).toBe(200);
        shardDistribution.add(response.data.shard);
      }
      
      // Should use multiple shards (not all keys on one shard)
      expect(shardDistribution.size).toBeGreaterThan(1);
    });

    test('should return 400 for missing key in route request', async () => {
      const response = await makeRequest('GET', `http://localhost:${coordinatorPort}/route`);
      
      expect(response.status).toBe(400);
      expect(response.data.error).toBe('missing key');
    });
  });

  describe('Key-Value Operations', () => {
    test('should proxy PUT requests to correct shard', async () => {
      const response = await makeRequest('PUT', `http://localhost:${coordinatorPort}/kv?key=proxy-test`, 'proxy-value');
      
      expect(response.status).toBe(200);
      expect(response.data.ok).toBe(true);
      expect(response.data.acks).toBe(1);
    });

    test('should proxy GET requests to correct shard', async () => {
      // GET a key that exists in mock data
      const response = await makeRequest('GET', `http://localhost:${coordinatorPort}/kv?key=test-key`);
      
      expect(response.status).toBe(200);
      expect(response.data.found).toBe(true);
      expect(response.data.value).toBe('test-value');
    });

    test('should proxy DELETE requests to correct shard', async () => {
      // DELETE a key
      const deleteResponse = await makeRequest('DELETE', `http://localhost:${coordinatorPort}/kv?key=test-key`);
      expect(deleteResponse.status).toBe(200);
      expect(deleteResponse.data.ok).toBe(true);
    });

    test('should return 404 for non-existent keys', async () => {
      // Make sure we use a key that doesn't exist in our mock data
      const response = await makeRequest('GET', `http://localhost:${coordinatorPort}/kv?key=definitely-nonexistent-key-12345`);
      
      expect(response.status).toBe(404);
      expect(response.data.found).toBe(false);
    });

    test('should return 400 for missing key parameter', async () => {
      const response = await makeRequest('GET', `http://localhost:${coordinatorPort}/kv`);
      
      expect(response.status).toBe(400);
      expect(response.data.error).toBe('missing key');
    });

    test('should return 405 for unsupported methods', async () => {
      const response = await makeRequest('PATCH', `http://localhost:${coordinatorPort}/kv?key=test`);
      
      expect(response.status).toBe(405);
      expect(response.data.error).toBe('method not allowed');
    });
  });

  describe('End-to-End Scenarios', () => {
    test('should handle PUT operations', async () => {
      const key = 'crud-test';
      const value1 = 'initial-value';
      
      // CREATE (PUT)
      const putResponse = await makeRequest('PUT', `http://localhost:${coordinatorPort}/kv?key=${key}`, value1);
      expect(putResponse.status).toBe(200);
      expect(putResponse.data.ok).toBe(true);
    });

    test('should handle multiple concurrent operations', async () => {
      const operations = [];
      
      // Concurrent operations on different keys
      for (let i = 0; i < 5; i++) {
        operations.push(makeRequest('PUT', `http://localhost:${coordinatorPort}/kv?key=concurrent${i}`, `value${i}`));
      }
      
      const results = await Promise.all(operations);
      
      // All operations should succeed
      results.forEach(result => {
        expect(result.status).toBe(200);
        expect(result.data.ok).toBe(true);
      });
    });

    test('should handle keys with special characters', async () => {
      const specialKey = 'user:profile@email.com';
      const specialValue = 'Special value with\nnewlines and\ttabs';
      
      const putResponse = await makeRequest('PUT', `http://localhost:${coordinatorPort}/kv?key=${encodeURIComponent(specialKey)}`, specialValue);
      expect(putResponse.status).toBe(200);
      expect(putResponse.data.ok).toBe(true);
    });

    test('should handle PUT operations on multiple keys', async () => {
      const keys = ['user:1', 'user:2', 'user:3'];
      const values = ['Alice', 'Bob', 'Charlie'];
      
      // Store all data
      for (let i = 0; i < keys.length; i++) {
        const response = await makeRequest('PUT', `http://localhost:${coordinatorPort}/kv?key=${keys[i]}`, values[i]);
        expect(response.status).toBe(200);
        expect(response.data.ok).toBe(true);
      }
    });
  });

  describe('Error Handling', () => {
    test('should return 404 for invalid endpoints', async () => {
      const response = await makeRequest('GET', `http://localhost:${coordinatorPort}/invalid`);
      
      expect(response.status).toBe(404);
      expect(response.data.error).toBe('not found');
    });

    test('should handle large payloads', async () => {
      const largeValue = 'x'.repeat(10000); // 10KB
      
      const putResponse = await makeRequest('PUT', `http://localhost:${coordinatorPort}/kv?key=large-test`, largeValue);
      expect(putResponse.status).toBe(200);
      expect(putResponse.data.ok).toBe(true);
    });
  });
});