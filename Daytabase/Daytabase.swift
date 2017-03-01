//
//  Daytabase.swift
//  Daytabase
//
//  Created by Draveness on 01/03/2017.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation
import sqlite3

public typealias DaytabaseSerializer = (String, String, Any) -> Data
public typealias DaytabaseDeserializer = (String, String, Data) -> Any?

let defaultSerializer: DaytabaseSerializer = { (collection: String, key: String, value: Any) in
    return NSKeyedArchiver.archivedData(withRootObject: value)
}

let defaultDeserializer: DaytabaseDeserializer = { (collection: String, key: String, data: Data) in
    return NSKeyedUnarchiver.unarchiveObject(with: data)
}

public final class Daytabase {
    public let databasePath: String
    public let objectSerializer: DaytabaseSerializer
    public let objectDeserializer: DaytabaseDeserializer
    
    init(path: String,
         serializer: @escaping DaytabaseSerializer = defaultSerializer,
         deserializer: @escaping DaytabaseDeserializer = defaultDeserializer) {
        self.databasePath = path
        self.objectSerializer = serializer
        self.objectDeserializer = deserializer
    }
    
}
