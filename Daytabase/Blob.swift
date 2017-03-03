//
//  Blob.swift
//  Database
//
//  Created by Draveness on 3/1/17.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation

public struct Blob {

    public let bytes: [UInt8]

    public init(bytes: [UInt8]) {
        self.bytes = bytes
    }

    public init(bytes: UnsafeRawPointer, length: Int) {
        let i8bufptr = UnsafeBufferPointer(start: bytes.assumingMemoryBound(to: UInt8.self), count: length)
        self.init(bytes: [UInt8](i8bufptr))
    }

    public func toHex() -> String {
        return bytes.map {
            ($0 < 16 ? "0" : "") + String($0, radix: 16, uppercase: false)
            }.joined(separator: "")
    }
    
}
