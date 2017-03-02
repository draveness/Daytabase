//
//  Murmurhash.m
//  Daytabase
//
//  Created by Draveness on 3/2/17.
//  Copyright © 2017 Draveness. All rights reserved.
//

/**
 * MurmurHash2 was written by Austin Appleby, and is placed in the public domain.
 * http://code.google.com/p/smhasher
 * This file is original located in https://github.com/yapstudios/YapDatabase/blob/master/YapDatabase/Utilities/YapMurmurHash.m
 **/

@import Foundation;

NSUInteger DayMurmurHash2(NSUInteger hash1, NSUInteger hash2) {
    if (NSUIntegerMax == UINT32_MAX) {
            // MurmurHash2 (32-bit)
            //
            // uint32_t MurmurHash2 ( const void * key, int len, uint32_t seed )
            //
            // Normally one would pass a chunk of data ('key') and associated data chunk length ('len').
            // Instead we're going to use our 2 hashes.
            // And we're going to randomly make up a 'seed'.

        const uint32_t seed = 0xa2f1b6f; // Some random value I made up
        const uint32_t len = 8;          // 2 hashes, each 4 bytes = 8 bytes

            // 'm' and 'r' are mixing constants generated offline.
            // They're not really 'magic', they just happen to work well.

        const uint32_t m = 0x5bd1e995;
        const int r = 24;

            // Initialize the hash to a 'random' value

        uint32_t h = seed ^ len;
        uint32_t k;

            // Mix hash1

        k = hash1;

        k *= m;
        k ^= k >> r;
        k *= m;

        h *= m;
        h ^= k;

            // Mix khash

        k = hash2;

        k *= m;
        k ^= k >> r;
        k *= m;

        h *= m;
        h ^= k;

            // Do a few final mixes of the hash to ensure the last few
            // bytes are well-incorporated.

        h ^= h >> 13;
        h *= m;
        h ^= h >> 15;

        return (NSUInteger)h;
    } else {
            // MurmurHash2 (64-bit)
            //
            // uint64_t MurmurHash64A ( const void * key, int len, uint64_t seed )
            //
            // Normally one would pass a chunk of data ('key') and associated data chunk length ('len').
            // Instead we're going to use our 3 hashes.
            // And we're going to randomly make up a 'seed'.

        const uint32_t seed = 0xa2f1b6f; // Some random value I made up
        const uint32_t len = 16;         // 2 hashes, each 8 bytes = 16 bytes

            // 'm' and 'r' are mixing constants generated offline.
            // They're not really 'magic', they just happen to work well.

        const uint64_t m = 0xc6a4a7935bd1e995LLU;
        const int r = 47;

            // Initialize the hash to a 'random' value

        uint64_t h = seed ^ (len * m);
        uint64_t k;

            // Mix hash1

        k = hash1;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;

            // Mix hash2

        k = hash2;
        
        k *= m;
        k ^= k >> r;
        k *= m;
        
        h ^= k;
        h *= m;
        
            // Do a few final mixes of the hash to ensure the last few
            // bytes are well-incorporated.
        
        h ^= h >> r;
        h *= m;
        h ^= h >> r;
        
        return (NSUInteger)h;
    }
}
