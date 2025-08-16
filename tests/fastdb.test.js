const { makeFastDB } = require('../fastdb');
const fs = require('fs');
const path = require('path');

describe('FastDB', () => {
  let db;
  let testDir;

  beforeEach(() => {
    testDir = path.join(__dirname, 'test-data-' + Math.random().toString(36).slice(2));
    db = makeFastDB({ dataDir: testDir });
  });

  afterEach(() => {
    if (db && !db._closed) {
      try {
        db.close();
      } catch (e) {
        // Ignore close errors in tests
      }
    }
    if (fs.existsSync(testDir)) {
      fs.rmSync(testDir, { recursive: true, force: true });
    }
  });

  describe('Basic Operations', () => {
    test('should put and get a value', () => {
      db.put('key1', 'value1');
      const result = db.get('key1');
      
      expect(result).toBeTruthy();
      expect(result.value).toBe('value1');
      expect(result.ts).toBeGreaterThan(0);
    });

    test('should return null for non-existent key', () => {
      const result = db.get('nonexistent');
      expect(result).toBeNull();
    });

    test('should delete a key', () => {
      db.put('key1', 'value1');
      expect(db.get('key1')).toBeTruthy();
      
      db.del('key1');
      expect(db.get('key1')).toBeNull();
    });

    test('should overwrite existing value', () => {
      db.put('key1', 'value1');
      db.put('key1', 'value2');
      
      const result = db.get('key1');
      expect(result.value).toBe('value2');
    });
  });

  describe('Data Types', () => {
    test('should handle string keys and values', () => {
      db.put('stringKey', 'stringValue');
      expect(db.get('stringKey').value).toBe('stringValue');
    });

    test('should handle numeric keys and values', () => {
      db.put(123, 456);
      const result = db.get(123);
      expect(result).toBeTruthy();
      expect(String(result.value)).toBe('456'); // Values are converted to strings
    });

    test('should handle empty values', () => {
      db.put('emptyKey', '');
      expect(db.get('emptyKey').value).toBe('');
    });

    test('should handle JSON object values', () => {
      const jsonObj = { name: 'Alice', age: 30, tags: ['developer', 'nodejs'] };
      db.put('jsonKey', jsonObj);
      
      const result = db.get('jsonKey');
      expect(result).toBeTruthy();
      expect(result.value).toEqual(jsonObj);
      expect(typeof result.value).toBe('object');
    });

    test('should handle nested JSON objects', () => {
      const nestedObj = {
        user: { id: 123, name: 'Bob' },
        settings: { theme: 'dark', notifications: true },
        data: [1, 2, { nested: 'value' }]
      };
      db.put('nestedKey', nestedObj);
      
      const result = db.get('nestedKey');
      expect(result.value).toEqual(nestedObj);
      expect(result.value.user.name).toBe('Bob');
      expect(result.value.data[2].nested).toBe('value');
    });

    test('should handle JSON arrays', () => {
      const arrayValue = [1, 'two', { three: 3 }, [4, 5]];
      db.put('arrayKey', arrayValue);
      
      const result = db.get('arrayKey');
      expect(result.value).toEqual(arrayValue);
      expect(Array.isArray(result.value)).toBe(true);
    });

    test('should handle mixed string and JSON values', () => {
      db.put('string1', 'plain text');
      db.put('json1', { type: 'object' });
      db.put('string2', 'more text');
      
      expect(db.get('string1').value).toBe('plain text');
      expect(db.get('json1').value).toEqual({ type: 'object' });
      expect(db.get('string2').value).toBe('more text');
    });
  });

  describe('Persistence', () => {
    test('should create data directory', () => {
      expect(fs.existsSync(testDir)).toBe(true);
    });

    test('should create WAL file', () => {
      db.put('test', 'value');
      const walFile = path.join(testDir, 'wal.log');
      expect(fs.existsSync(walFile)).toBe(true);
    });

    test('should recover data after restart', async () => {
      // Insert data
      db.put('persistent1', 'value1');
      db.put('persistent2', 'value2');
      db.del('persistent1');
      
      // Wait for WAL flush
      await new Promise(resolve => setTimeout(resolve, 10));
      db.close();

      // Create new instance with same data directory
      const db2 = makeFastDB({ dataDir: testDir });
      
      expect(db2.get('persistent1')).toBeNull();
      expect(db2.get('persistent2').value).toBe('value2');
      
      db2.close();
    });

    test('should recover JSON data after restart', async () => {
      // Insert mixed data
      const jsonData = { users: [{ id: 1, name: 'Alice' }], count: 5 };
      db.put('stringKey', 'plain string');
      db.put('jsonKey', jsonData);
      db.put('numberKey', 42);
      
      // Wait for WAL flush
      await new Promise(resolve => setTimeout(resolve, 10));
      db.close();

      // Create new instance with same data directory
      const db2 = makeFastDB({ dataDir: testDir });
      
      expect(db2.get('stringKey').value).toBe('plain string');
      expect(db2.get('jsonKey').value).toEqual(jsonData);
      expect(db2.get('numberKey').value).toBe('42');
      
      db2.close();
    });
  });

  describe('Snapshot Operations', () => {
    test('should create snapshot manually', () => {
      db.put('snap1', 'value1');
      db.put('snap2', 'value2');
      
      db.takeSnapshot();
      
      const snapshotFile = path.join(testDir, 'snapshot.json.gz');
      expect(fs.existsSync(snapshotFile)).toBe(true);
    });

    test('should load from snapshot', () => {
      // Create data and snapshot
      db.put('snap1', 'value1');
      db.put('snap2', 'value2');
      db.takeSnapshot();
      db.close();

      // Create new instance
      const db2 = makeFastDB({ dataDir: testDir });
      
      expect(db2.get('snap1').value).toBe('value1');
      expect(db2.get('snap2').value).toBe('value2');
      
      db2.close();
    });
  });

  describe('Error Handling', () => {
    test('should throw error when using closed database', () => {
      db.close();
      
      expect(() => db.put('key', 'value')).toThrow('DB closed');
      expect(() => db.del('key')).toThrow('DB closed');
    });

    test('should handle corrupted WAL gracefully', () => {
      // Write some valid data
      db.put('valid', 'data');
      db.close();

      // Corrupt the WAL file
      const walFile = path.join(testDir, 'wal.log');
      fs.appendFileSync(walFile, 'corrupted data');

      // Should still work (skip corrupted entries)
      expect(() => {
        const db2 = makeFastDB({ dataDir: testDir });
        db2.close();
      }).not.toThrow();
    });
  });

  describe('Performance', () => {
    test('should handle large number of operations', () => {
      const start = Date.now();
      
      for (let i = 0; i < 1000; i++) {
        db.put(`key${i}`, `value${i}`);
      }
      
      const end = Date.now();
      expect(end - start).toBeLessThan(1000); // Should complete in under 1 second
      
      // Verify data integrity
      expect(db.get('key0').value).toBe('value0');
      expect(db.get('key999').value).toBe('value999');
    });
  });
});