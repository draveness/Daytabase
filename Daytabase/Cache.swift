//
//  Cache.swift
//  Database
//
//  Created by Draveness on 3/2/17.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation


class CacheItem: Equatable {
    var key: CollectionKey
    var value: Any {
        didSet {
            self.data = NSKeyedArchiver.archivedData(withRootObject: value)
        }
    }
    private var data: Data

    var prev: CacheItem?
    var next: CacheItem?

    init(key: CollectionKey, value: Any) {
        self.key = key
        self.value = value
        self.data = NSKeyedArchiver.archivedData(withRootObject: value)
    }

    static func ==(lhs: CacheItem, rhs: CacheItem) -> Bool {
        return lhs.key == rhs.key && lhs.data == rhs.data
    }
}

extension CacheItem: CustomStringConvertible {
    var description: String {
        return "CacheItem<key: (\(key), value: \(value)>>"
    }
}

public class Cache {
    public let capacity: Int

    private var dictionary: [CollectionKey: CacheItem] = [:]
    private var mostRecentCacheItem: CacheItem?
    private var leastRecentCacheItem: CacheItem?
    private var evictedCacheItem: CacheItem?

    public init(capacity: Int = 0) {
        self.capacity = capacity
    }

    public func object(forKey key: CollectionKey) -> Any? {
        guard let item = dictionary[key] else { return nil }
        if item != mostRecentCacheItem {
            // Remove item from current position in linked-list.
            //
            // Notes:
            // We fetched the item from the list,
            // so we know there's a valid mostRecentCacheItem & leastRecentCacheItem.
            // Furthermore, we know the item isn't the mostRecentCacheItem.

            item.prev?.next = item.next

            if item == leastRecentCacheItem {
                leastRecentCacheItem = item.prev
            } else {
                item.next?.prev = item.prev
            }

            // Move item to beginning of linked-list

            item.prev = nil
            item.next = mostRecentCacheItem

            mostRecentCacheItem?.prev = item
            mostRecentCacheItem = item
        }
        return item.value
    }

    public func set(object: Any, forKey key: CollectionKey) {
        if let exisitingItem = dictionary[key] {
            exisitingItem.value = object

            if exisitingItem != mostRecentCacheItem {
                // Remove item from current position in linked-list
                //
                // Notes:
                // We fetched the item from the list,
                // so we know there's a valid mostRecentCacheItem & leastRecentCacheItem.
                // Furthermore, we know the item isn't the mostRecentCacheItem.

                exisitingItem.prev?.next = exisitingItem.next
                if exisitingItem == leastRecentCacheItem {
                    leastRecentCacheItem = exisitingItem.prev
                } else {
                    exisitingItem.next?.prev = exisitingItem.prev
                }

                // Move item to beginning of linked-list

                exisitingItem.prev = nil
                exisitingItem.next = mostRecentCacheItem

                mostRecentCacheItem?.prev = exisitingItem
                mostRecentCacheItem = exisitingItem

                Daytabase.log.verbose("key(\(key)) <- existing, new mostRecent")
            } else {
                Daytabase.log.verbose("key(\(key)) <- existing, already mostRecent")
            }
        } else {

            // Create new item (or recycle old evicted item)
            var newItem: CacheItem!
            if let evictedCacheItem = evictedCacheItem {
                evictedCacheItem.key = key
                evictedCacheItem.value = object
                newItem = evictedCacheItem
                self.evictedCacheItem = nil
            } else {
                newItem = CacheItem(key: key, value: object)
            }

            dictionary[key] = newItem

            newItem.next = mostRecentCacheItem
            mostRecentCacheItem?.prev = newItem
            mostRecentCacheItem = newItem

            if capacity != 0 && dictionary.count > capacity,
                let keyToEvict = leastRecentCacheItem?.key {
                Daytabase.log.verbose("key(\(key)), out(\(self.leastRecentCacheItem?.key))")

                if let _ = evictedCacheItem {
                    leastRecentCacheItem = leastRecentCacheItem?.prev
                    leastRecentCacheItem?.next = nil
                } else {
                    evictedCacheItem = leastRecentCacheItem
                    leastRecentCacheItem = leastRecentCacheItem?.prev
                    leastRecentCacheItem?.next = nil

                    evictedCacheItem?.prev = nil
                    evictedCacheItem?.next = nil
                }
                dictionary.removeValue(forKey: keyToEvict)
            } else {
                Daytabase.log.verbose("key(\(key)) <- new, new mostRecent [\(self.dictionary.count) of \(self.capacity)]")
            }

            if let key = dictionary.keys.first, dictionary.count > capacity {
                dictionary.removeValue(forKey: key)
            }
        }

        if Daytabase.log.outputLevel <= .verbose {
            Daytabase.log.verbose("dictionary: \(self.dictionary)")

            var loopItem = mostRecentCacheItem
            var i = 0
            while loopItem != nil {
                Daytabase.log.verbose("\(i): \(loopItem!)")
                loopItem = loopItem?.next
                i += 1
            }
        }
    }

    public func contains(key inKey: CollectionKey) -> Bool {
        return dictionary.reduce(false) { result, keyValue in
            result || (keyValue.key == inKey)
        }
    }

    public func removeObject(forKey key: CollectionKey) {
        if let item = dictionary[key] {
            if mostRecentCacheItem == item {
                mostRecentCacheItem = item.next
            } else if item.prev != nil {
                item.prev?.next = item.next
            }

            if leastRecentCacheItem == item {
                leastRecentCacheItem = item.prev
            } else if item.next != nil {
                item.next?.prev = item.prev
            }
            dictionary.removeValue(forKey: key)
        }
    }

    public func removeObjects(forKeys keys: [CollectionKey]) {
        for key in keys {
            removeObject(forKey: key)
        }
    }

    public func removeAllObjects() {
        mostRecentCacheItem = nil
        leastRecentCacheItem = nil
        evictedCacheItem = nil
        dictionary.removeAll()
    }
}
