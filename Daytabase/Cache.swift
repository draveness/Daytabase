//
//  Cache.swift
//  Daytabase
//
//  Created by Draveness on 3/2/17.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation


class CacheItem: CustomStringConvertible, Equatable {
    let key: CollectionKey
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

    var description: String {
        return "<CacheItem[\(self)] key(\(key))>"
    }
}

public struct Cache {
    public let capacity: Int

    var dictionary: [CollectionKey: CacheItem] = [:]
    var mostRecentCacheItem: CacheItem?
    var leastRecentCacheItem: CacheItem?

    public init(capacity: Int) {
        self.capacity = capacity
    }

    public mutating func object(forKey key: CollectionKey) -> Any? {
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

    mutating func set(object: Any, forKey key: CollectionKey) {
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

                Log.verbose("key(\(key)) <- existing, new mostRecent")
            } else {
                Log.verbose("key(\(key)) <- existing, already mostRecent")
            }
        } else {
            let item = CacheItem(key: key, value: object)
            dictionary[key] = item
            if let key = dictionary.keys.first, dictionary.count > capacity {
                dictionary.removeValue(forKey: key)
            }
        }
    }
}
