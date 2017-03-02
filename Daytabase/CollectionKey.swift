//
//  CollectionKey.swift
//  Daytabase
//
//  Created by Draveness on 3/2/17.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation


public class CollectionKey: NSCoder, NSCoding, NSCopying {
    public let collection: String
    public let key: String

    public static func equal(ck1: CollectionKey, ck2: CollectionKey) -> Bool {
        return ck1.isEqual(collectionKey: ck2)
    }

    public init(key: String, collection: String = "") {
        self.collection = collection
        self.key = key
    }

    public override var hash: Int {
        return Utility.MurmurHash2(hash1: collection.hash, hash2: key.hash)
    }

    public required init?(coder aDecoder: NSCoder) {
        guard let collection = aDecoder.decodeObject(forKey: "collection") as? String,
            let key = aDecoder.decodeObject(forKey: "key") as? String else {
                return nil
        }

        self.collection = collection
        self.key = key
    }

    public func encode(with aCoder: NSCoder) {
        aCoder.encode(collection, forKey: "collection")
        aCoder.encode(key, forKey: "key")
    }

    public func copy(with zone: NSZone? = nil) -> Any {
        return self
    }

    public func isEqual(collectionKey: CollectionKey) -> Bool {
        if hash != collectionKey.hash {
            return false
        } else {
            return key == collectionKey.key && collection == collectionKey.collection
        }
    }

    public override func isEqual(_ object: Any?) -> Bool {
        guard let object = object as? CollectionKey else { return false }
        return isEqual(collectionKey: object)
    }
}

