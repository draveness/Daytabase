//
//  CollectionKey.swift
//  Daytabase
//
//  Created by Draveness on 3/2/17.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation


public struct CollectionKey: Hashable {

    public let key: String
    public let collection: String

    /// Returns a Boolean value indicating whether two values are equal.
    ///
    /// Equality is the inverse of inequality. For any values `a` and `b`,
    /// `a == b` implies that `a != b` is `false`.
    ///
    /// - Parameters:
    ///   - lhs: A value to compare.
    ///   - rhs: Another value to compare.
    public static func ==(lhs: CollectionKey, rhs: CollectionKey) -> Bool {
        return lhs.collection == rhs.collection && lhs.key == rhs.key
    }

    /// The hash value.
    ///
    /// Hash values are not guaranteed to be equal across different executions of
    /// your program. Do not save hash values to use during a future execution.
    public var hashValue: Int {
        return Utility.MurmurHash2(hash1: collection.hash, hash2: key.hash)
    }

    public init(key: String, collection: String = "") {
        self.collection = collection
        self.key = key
    }
}

