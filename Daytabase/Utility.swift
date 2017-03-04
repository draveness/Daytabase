//
//  Utility.swift
//  Database
//
//  Created by Draveness on 3/2/17.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation

struct Utility {
    static func MurmurHash2(hash1: Int, hash2: Int) -> Int {
        return Int(DayMurmurHash2(UInt(hash1), UInt(hash2)))
    }
}

extension String {
    var length: Int32 {
        return Int32(self.characters.count)
    }
}
