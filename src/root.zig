const std = @import("std");

const MAX_LEVEL_COUNT: usize = 25;
const MAX_LEVEL: usize = MAX_LEVEL_COUNT - 1;

fn NodeType(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();

        key: K,
        value: V,
        nextNodes: []?*Self,

        pub fn init(allocator: std.mem.Allocator, key: K, value: V, replicateUptoLevel: usize) !*Self {
            const newNode = try allocator.create(Self);
            errdefer allocator.destroy(newNode);

            newNode.key = key;
            newNode.value = value;

            newNode.nextNodes = try allocator.alloc(?*Self, replicateUptoLevel + 1);
            @memset(newNode.nextNodes, null);

            return newNode;
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            allocator.free(self.nextNodes);
            allocator.destroy(self);
        }
    };
}

pub fn SkipMapType(
    comptime K: type,
    comptime V: type,
    comptime isLessThan: fn (LHS: K, RHS: K) bool,
    comptime isEqual: fn (LHS: K, RHS: K) bool,
) type {
    const SkipMap = struct {
        const Self = @This();
        const Node = NodeType(K, V);

        allocator: std.mem.Allocator,
        randomNumberGenerator: std.rand.DefaultPrng = std.rand.DefaultPrng.init(0),
        currentHighestLevel: usize = 0,
        topLeftNode: *Node,

        pub fn init(allocator: std.mem.Allocator) !Self {
            return Self{
                .allocator = allocator,
                .topLeftNode = try Self.Node.init(allocator, 0, 0, MAX_LEVEL),
            };
        }

        // pub fn deinit(self: *Self) void {}

        // If the key already exists, then its corresponding value is updated.
        // Otherwise, the key-value pair is inserted.
        pub fn upsert(self: *Self, key: K, value: V) !void {
            var previousNodes: [MAX_LEVEL_COUNT]*Node = [_]*Node{undefined} ** MAX_LEVEL_COUNT;
            var nextNodes: [MAX_LEVEL_COUNT]?*Node = [_]?*Node{null} ** MAX_LEVEL_COUNT;

            const searchResult = self.searchNode(key, &previousNodes, &nextNodes);
            if (searchResult) |node| {
                node.value = value;
                return;
            }

            // The key doesn't exist in the SkipMap. So, create a Node for that corresponding key
            // and insert it into the SkipMap.

            const highestLevelBeforeRandomLevelGeneration = self.currentHighestLevel;

            const randomLevel = self.randomLevelGenerator();

            if (randomLevel > highestLevelBeforeRandomLevelGeneration) {
                for ((highestLevelBeforeRandomLevelGeneration + 1)..(randomLevel + 1)) |newLevel| {
                    previousNodes[newLevel] = self.topLeftNode;
                }
            }

            const newNode = try Node.init(self.allocator, key, value, randomLevel);

            var currentLevel: usize = 0;
            while (currentLevel <= randomLevel) : (currentLevel += 1) {
                previousNodes[currentLevel].nextNodes[currentLevel] = newNode;
                newNode.nextNodes[currentLevel] = nextNodes[currentLevel];
            }
        }

        // Returns the value for the given key, if found.
        pub fn get(self: *Self, key: K) ?V {
            var currentNode = self.topLeftNode;

            var currentLevel = self.currentHighestLevel;
            while (currentLevel >= 0) : (currentLevel -= 1) {
                var nextNodePointer = currentNode.nextNodes[currentLevel];

                while (nextNodePointer) |nextNode| {
                    if (isEqual(nextNode.key, key))
                        return nextNode.value;

                    if (!isLessThan(nextNode.key, key))
                        break;

                    currentNode = nextNode;
                    nextNodePointer = currentNode.nextNodes[currentLevel];
                }
            }
            return null;
        }

        // TODO : Fix.
        pub fn delete(self: *Self, key: K) void {
            var previousNodes: [MAX_LEVEL_COUNT]*Node = [_]*Node{undefined} ** MAX_LEVEL_COUNT;
            var nextNodes: [MAX_LEVEL_COUNT]?*Node = [_]?*Node{null} ** MAX_LEVEL_COUNT;

            const searchResult = self.searchNode(key, &previousNodes, &nextNodes);
            if (searchResult == null)
                return;

            const node = searchResult.?;

            var currentLevel: usize = 0;
            while (currentLevel <= MAX_LEVEL) : (currentLevel += 1) {
                const previousNode = previousNodes[currentLevel];
                previousNode.nextNodes[currentLevel] = node.nextNodes[currentLevel];
            }

            self.allocator.destroy(node);
        }

        // Searches for the node that corresponds to the given key.
        fn searchNode(
            self: *Self,
            key: K,
            previousNodes: *[MAX_LEVEL_COUNT]*Node,
            nextNodes: *[MAX_LEVEL_COUNT]?*Node,
        ) ?*Node {
            var currentNode = self.topLeftNode;

            var currentLevel = self.currentHighestLevel;
            while (currentLevel >= 0) {
                var nextNodePointer = currentNode.nextNodes[currentLevel];

                while (nextNodePointer) |nextNode| {
                    if (!isLessThan(nextNode.key, key))
                        break;

                    currentNode = nextNode;
                    nextNodePointer = currentNode.nextNodes[currentLevel];
                }

                previousNodes[currentLevel] = currentNode;
                nextNodes[currentLevel] = nextNodePointer;

                // When the key is already present in the SkipMap.
                if (nextNodePointer) |nextNode|
                    if (isEqual(nextNode.key, key))
                        return nextNode;

                if (currentLevel == 0) break;

                currentLevel -= 1;
            }
            return null;
        }

        // Uses randomness to generate the level upto which an element will be replicated. For
        // simplicity, we're currently using uniform probability distribution.
        fn randomLevelGenerator(self: *Self) usize {
            var level: usize = 0;

            // We'll try to generate a random float between 0 and 1. If it's less than 0.5, then the
            // element will get replicated to the next level, otherwise not.
            while (self.randomNumberGenerator.random().float(f32) < 0.5) : (level += 1)
                if (level == MAX_LEVEL)
                    break;

            // Update current highest level (if necessary).
            if (level > self.currentHighestLevel)
                self.currentHighestLevel = level;

            return level;
        }
    };
    return SkipMap;
}

fn isLessThan_u8(a: u8, b: u8) bool {
    return a < b;
}

fn isEqual_u8(a: u8, b: u8) bool {
    return a == b;
}

const assert = std.debug.assert;

test "SkipMap" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    const SkipMap = SkipMapType(u8, u8, isLessThan_u8, isEqual_u8);
    var skipMap = try SkipMap.init(allocator);
    // defer skipMap.deinit();

    try skipMap.upsert(1, 1);
    try skipMap.upsert(2, 2);

    assert(skipMap.get(1).? == 1);

    skipMap.delete(1);
    assert(skipMap.get(1) == null);
}
