
#ifndef __PIX_CODEC_H_
#define __PIX_CODEC_H_

#include <ctype.h>
#include <stdint.h>
#include <vector>

namespace mongo {

class PixCodec {

public:
    /*
    *  Var compress from array to array
    */
    static uint32_t varCompress(            // return: number of output bits
        unsigned char* cbuf,                // output: array of packed bits
        uint32_t  clen,                     // input:  output buffer capacity
        uint32_t* ubuf,                     // input:  array to compress
        uint32_t  uoffset,                  // input:  start offset for input
        uint32_t  ulen);                    // input:  input length

    /*
    *  Var compress from vector to array
    */
    static uint32_t varCompress(            // return: number of output bits    
        unsigned char* cbuf,                // output: array of packed bits
        uint32_t clen,                      // input:  output buffer capacity
        const std::vector<uint32_t>& uvec); // input:  vector to compress

    /*
    *  Var compress from vector to (array,offset,length)
    */
    static uint32_t varCompress(            // return: number of output bits    
        unsigned char* cbuf,                // output: output buffer
        uint32_t offset,                    // input:  output buffer offset
        uint32_t max_offset,                // input:  output buffer capacity
        const std::vector<uint32_t>& uvec); // input:  vector to compress

    static uint32_t varCompress(            // return: number of output bytes
        unsigned char* cbuf,                // output: output array
        uint32_t offset,                    // input:  output segment offset
        uint32_t max_offset,                // input:  output buffer capacity
        const std::vector<uint32_t>& uvec,  // input:  vector to compress
        uint32_t uvec_offset,               // input:  input segment offset
        uint32_t uvec_len);                 // output: input segment length

    /*
    *  Var compress from vector to vector
    */
    static uint32_t varCompress(            // return: number of output bytes
        std::vector<unsigned char>& cvec,   // output: vector of compressed values
        const std::vector<uint32_t>& uvec); // input:  vector to compress

    /*
    *  Var compress from vector to vector
    */
    static uint32_t varCompress(            // return: number of output bytes
        std::vector<unsigned char>& cvec,   // output: vector of compressed values
        const std::vector<uint64_t>& uvec); // input:  vector to compress

    /*
    *  Var uncompress from array to vector
    */
    static void varUncompress(
        std::vector<uint32_t>& uvec,        // output: vector of uncompressed values
        unsigned char* cbuf,                // input:  array of compressed values
        uint32_t coffset,                   // input:  start offset
        uint32_t clen);                     // input:  input byte count

    /*
    *  Var uncompress from vector iterator to vector
    */
    static void varUncompress(
        std::vector<uint32_t>& uvec,        // output: vector of uncompressed values
        std::vector<unsigned char>::const_iterator& begin,  // start position
        std::vector<unsigned char>::const_iterator& end);   // one past end position

    /*
    *  Var uncompress from vector to vector
    */
    static void varUncompress(
        std::vector<uint32_t>& uvec,             // output: vector of uncompressed values
        const std::vector<unsigned char>& cvec,  // input:  vector to uncompress
        uint32_t coffset,                        // input:  starting index in cvec
        uint32_t clen);                          // input:  input count
  
    /*
    *  Var uncompress from vector to vector
    */
    static void varUncompress(
        std::vector<uint64_t>& uvec,             // output: vector of uncompressed values
        const std::vector<unsigned char>& cvec,  // input:  vector to uncompress
        uint32_t coffset,                        // input:  starting index in cvec
        uint32_t clen);                          // input:  input count

// Gamma

    /*
    *   Gamma compress array
    *   Gamma(x) = unary(lg(x)), binary(x - 2^lg(x))
    */
    static uint32_t gammaCompress(      // return: number of output bits
        unsigned char* cbuf,            // output: array of packed bits
        uint32_t  clen,                 // input:  output buffer capacity
        uint32_t* ubuf,                 // input:  array to compress
        uint32_t uoffset,               // input:  start offset for input
        uint32_t ulen);                 // input:  input length

    /*
    *  Gamma compress vector
    *  Gamma(x) = unary(lg(x)), binary(x - 2^lg(x))
    */
    static uint32_t gammaCompress(      // return: number of output bits    
        unsigned char* cbuf,            // output: array of packed bits
        uint32_t clen,                  // input:  output buffer capacity
        std::vector<uint32_t> uvec);    // input:  vector to compress

    /*
    *  Gamma uncompress to vector
    */
    static void gammaUncompress(
      std::vector<uint32_t>& uvec,      // output: vector of uncompressed values
      unsigned char* cbuf,              // input:  array of compressed values
      uint32_t coffset,                 // input:  start offset
      uint32_t clen);                   // input:  input byte count

    /*
    *  Gamma uncompress to array
    */
    static void gammaUncompress(
        uint32_t* ubuf,                 // output: array of uncompressed values
        unsigned char* cbuf,            // input:  array of compressed values
        uint32_t cbuf_offset);          // input:  start offset

// Delta

    /*
    *  Delta compress array
    *  Delta(x) = Gamma([ lg(x) ]) , binary(x - 2^lg(x))
    */
    static uint32_t deltaCompress(      // return: number of output bits
        unsigned char* cbuf,            // output: array of packed bits
        uint32_t  clen,                 // input:  output buffer capacity
        uint32_t* ubuf,                 // input:  array to compress
        uint32_t  uoffset,              // input:  start offset for input
        uint32_t  ulen);                // input:  input length

    /*
    *  Delta compress vector
    *  Delta(x) = Gamma([ lg(x) ]) , binary(x - 2^lg(x))
    */
    static uint32_t deltaCompress(      // return: number of output bits
        unsigned char* cbuf,            // output: array of packed bits
        uint32_t clen,                  // input:  start offset for output
        std::vector<uint32_t> uvec);    // input:  vector to compress

    /*
    *  Delta uncompress to vector
    */
    static void deltaUncompress(
        std::vector<uint32_t>& uvec,    // output: vector of uncompressed values
        unsigned char* cbuf,            // input:  array of compressed values
        uint32_t coffset,               // input:  start offset
        uint32_t clen);                 // input:  input byte count

    /*
    *  Delta uncompress to array
    */
    static void deltaUncompress(
        uint32_t* ubuf,                 // output: array of uncompressed values
        unsigned char* cbuf,            // input:  array of compressed values
        uint32_t cbuf_offset);          // input:  start offset

// Bernoulli 

    /*
    *  Bernoulli compress array
    */
    static uint32_t bernoulliCompress(  // return: number of output bits
        unsigned char* cbuf,            // output: array of packed bits
        uint32_t cbuf_offset,           // input:  start offset for output
        uint32_t* ubuf,                 // input:  array to compress
        uint32_t ubuf_offset);          // input:  start offset for input
    
    /*
    *  Bernoulli compress vector
    */
    static uint32_t bernoulliCompress(  // return: number of output bits
        unsigned char* cbuf,            // output: array of packed bits
        uint32_t cbuf_offset,           // input:  start offset for output
        std::vector<uint32_t> uvec);    // input:  vector to compress
  
    /*
    *  Bernoulli uncompress to vector
    */
    static void bernoulliUncompress(
        std::vector<uint32_t> uvec,     // output: vector of uncompressed values
        unsigned char* cbuf,            // input:  array of compressed values
        uint32_t cbuf_offset );         // input:  start offset
  
    /*
    *  Bernoulli uncompress to array
    */
    static void bernoulliUncompress(
        uint32_t* ubuf,                 // output: array of uncompressed values
        unsigned char* cbuf,            // input:  array of compressed values
        uint32_t cbuf_offset);          // input:  start offset
  
};

}  /* namespace mongo */

#endif
