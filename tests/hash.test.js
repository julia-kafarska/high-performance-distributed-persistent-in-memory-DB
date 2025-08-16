const { buildRing, pickNode } = require('../hash');

describe('Consistent Hashing', () => {
  describe('buildRing', () => {
    test('should create ring with default virtual nodes', () => {
      const nodes = ['node1', 'node2', 'node3'];
      const ring = buildRing(nodes);
      
      expect(ring).toHaveLength(300); // 3 nodes * 100 vnodes each
      expect(ring[0]).toHaveProperty('hash');
      expect(ring[0]).toHaveProperty('node');
    });

    test('should create ring with custom virtual nodes', () => {
      const nodes = ['A', 'B'];
      const ring = buildRing(nodes, 50);
      
      expect(ring).toHaveLength(100); // 2 nodes * 50 vnodes each
    });

    test('should sort ring by hash values', () => {
      const nodes = ['node1', 'node2'];
      const ring = buildRing(nodes, 10);
      
      for (let i = 1; i < ring.length; i++) {
        expect(ring[i].hash).toBeGreaterThanOrEqual(ring[i-1].hash);
      }
    });

    test('should handle single node', () => {
      const ring = buildRing(['solo'], 5);
      
      expect(ring).toHaveLength(5);
      ring.forEach(entry => {
        expect(entry.node).toBe('solo');
      });
    });

    test('should create unique hashes for virtual nodes', () => {
      const ring = buildRing(['node1'], 100);
      const hashes = new Set(ring.map(entry => entry.hash));
      
      expect(hashes.size).toBe(100); // All hashes should be unique
    });
  });

  describe('pickNode', () => {
    let ring;

    beforeEach(() => {
      ring = buildRing(['node1', 'node2', 'node3'], 100);
    });

    test('should consistently pick same node for same key', () => {
      const key = 'test-key';
      const node1 = pickNode(ring, key);
      const node2 = pickNode(ring, key);
      
      expect(node1).toBe(node2);
    });

    test('should distribute keys across all nodes', () => {
      const keys = [];
      for (let i = 0; i < 1000; i++) {
        keys.push(`key-${i}`);
      }
      
      const distribution = {};
      keys.forEach(key => {
        const node = pickNode(ring, key);
        distribution[node] = (distribution[node] || 0) + 1;
      });
      
      // All nodes should receive some keys
      expect(Object.keys(distribution)).toHaveLength(3);
      expect(distribution.node1).toBeGreaterThan(0);
      expect(distribution.node2).toBeGreaterThan(0);
      expect(distribution.node3).toBeGreaterThan(0);
      
      // Distribution should be reasonably balanced (within 30% of expected)
      const expected = 1000 / 3;
      Object.values(distribution).forEach(count => {
        expect(count).toBeGreaterThan(expected * 0.7);
        expect(count).toBeLessThan(expected * 1.3);
      });
    });

    test('should handle different key types', () => {
      expect(pickNode(ring, 'string-key')).toBeTruthy();
      expect(pickNode(ring, 12345)).toBeTruthy();
      expect(pickNode(ring, 'user:123:profile')).toBeTruthy();
    });

    test('should work with single node ring', () => {
      const singleRing = buildRing(['only-node'], 10);
      
      expect(pickNode(singleRing, 'any-key')).toBe('only-node');
      expect(pickNode(singleRing, 'another-key')).toBe('only-node');
    });

    test('should handle edge cases in binary search', () => {
      const smallRing = buildRing(['A', 'B'], 2);
      
      // Test with many different keys to exercise binary search paths
      for (let i = 0; i < 100; i++) {
        const node = pickNode(smallRing, `key-${i}`);
        expect(['A', 'B']).toContain(node);
      }
    });
  });

  describe('Load Balancing', () => {
    test('should provide better distribution with more virtual nodes', () => {
      const nodes = ['node1', 'node2', 'node3'];
      const keys = Array.from({length: 1000}, (_, i) => `key-${i}`);
      
      // Test with fewer virtual nodes
      const ring1 = buildRing(nodes, 10);
      const dist1 = getDistribution(ring1, keys);
      const variance1 = calculateVariance(Object.values(dist1));
      
      // Test with more virtual nodes
      const ring2 = buildRing(nodes, 100);
      const dist2 = getDistribution(ring2, keys);
      const variance2 = calculateVariance(Object.values(dist2));
      
      // More virtual nodes should provide better (lower variance) distribution
      expect(variance2).toBeLessThan(variance1);
    });

    function getDistribution(ring, keys) {
      const distribution = {};
      keys.forEach(key => {
        const node = pickNode(ring, key);
        distribution[node] = (distribution[node] || 0) + 1;
      });
      return distribution;
    }

    function calculateVariance(values) {
      const mean = values.reduce((a, b) => a + b, 0) / values.length;
      const squaredDiffs = values.map(value => Math.pow(value - mean, 2));
      return squaredDiffs.reduce((a, b) => a + b, 0) / values.length;
    }
  });

  describe('Node Addition/Removal', () => {
    test('should minimize key redistribution when adding nodes', () => {
      const originalNodes = ['node1', 'node2', 'node3'];
      const originalRing = buildRing(originalNodes, 100);
      
      const keys = Array.from({length: 1000}, (_, i) => `key-${i}`);
      const originalMapping = {};
      keys.forEach(key => {
        originalMapping[key] = pickNode(originalRing, key);
      });
      
      // Add a new node
      const newNodes = [...originalNodes, 'node4'];
      const newRing = buildRing(newNodes, 100);
      
      let unchangedKeys = 0;
      keys.forEach(key => {
        if (originalMapping[key] === pickNode(newRing, key)) {
          unchangedKeys++;
        }
      });
      
      // Most keys should remain on the same node (> 70%)
      expect(unchangedKeys / keys.length).toBeGreaterThan(0.7);
    });

    test('should handle node removal gracefully', () => {
      const ring = buildRing(['node1', 'node2', 'node3'], 50);
      const key = 'test-key';
      
      // Pick a node with full ring
      const originalNode = pickNode(ring, key);
      expect(['node1', 'node2', 'node3']).toContain(originalNode);
      
      // Remove one node and test
      const reducedRing = buildRing(['node1', 'node2'], 50);
      const newNode = pickNode(reducedRing, key);
      expect(['node1', 'node2']).toContain(newNode);
    });
  });
});