//
//  Cache.swift
//  Daytabase
//
//  Created by Draveness on 3/2/17.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation


class CacheItem: CustomStringConvertible {
    let key: NSCopying
    let value: Any

    init(key: NSCopying, value: Any) {
        self.key = key
        self.value = value
    }

    var description: String {
        return "<CacheItem[\(self)] key(\(key))>"
    }
}

public struct Cache {
    public var capacity: Int {
        get {
            return self.capacity
        }
        set {
            capacity = newValue
        }
    }

    var dictionary: [CollectionKey: CacheItem] = [:]
    var mostRecentCacheItems: [CacheItem] = []
    var leastRecentCacheItems: [CacheItem] = []

    public init(capacity: Int) {
        self.capacity = capacity
    }

    public func object(forKey key: CollectionKey) -> Any? {
        guard let item = dictionary[key] else { return nil }
//        if mostRecentCacheItems.count == 0 || mostRecentCacheItems.first! == item {
//
//        }

        return item.value
    }
}
