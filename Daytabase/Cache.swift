//
//  Cache.swift
//  Daytabase
//
//  Created by Draveness on 3/2/17.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation


public class CacheItem {
    let key: String
    let value: Any
    var prev: CacheItem?
    var next: CacheItem?

    init(key: String, value: String) {
        self.key = key
        self.value = value
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
//    var mostRecentCacheItem: CacheItem?
//    var leastRecentCacheItem: CacheItem?

    public init(capacity: Int) {
        self.capacity = capacity
    }

    public func object(forKey key: CollectionKey) -> Any? {
        guard let item = dictionary[key] else { return nil }
        return item.value
    }
}
