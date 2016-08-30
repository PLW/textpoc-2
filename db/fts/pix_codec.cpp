
#include <assert.h>
#include <iostream>

#include "pix_codec.h"
#include "pix_bit_buffer.h"

using namespace std;
namespace mongo {

#define HIGHBIT ((unsigned char)0x80)
#define LOWMASK ((unsigned char)0x7f)

static bool debug = false;

static uint32_t log(uint32_t x) {
    assert(x > 0);
    for (int i=0; i<32; i++) {
        if (x==0) return i-1;
        x >>= 1;
    }
    return 31;
}

// Variable-length integer coding

uint32_t PixCodec::varCompress(   // return: number of output bytes
    unsigned char* cbuf,          // output: array of packed bits
    uint32_t  clen,               // input:  output buffer capacity
    uint32_t* ubuf,               // input:  array to compress
    uint32_t  uoffset,            // input:  start offset for input
    uint32_t  ulen)               // input:  input length
{
    uint32_t delta;
    uint32_t lastX = 0;
    uint32_t offset = 0;

    for (uint32_t i=uoffset; i<uoffset+ulen; ++i) {
        uint32_t x = ubuf[i];
        assert(lastX < x);
        delta = x-lastX;
        lastX = x;

        assert((delta&0xf0000000) == 0);

        uint32_t a = (delta & 0x0fe00000) >> 21; // 7 bits 27..21
        uint32_t b = (delta & 0x001fc000) >> 14; // 7 bits 20..14
        uint32_t c = (delta & 0x00003f80) >>  7; // 7 bits 13..7
        uint32_t d = (delta & 0x0000007f);       // 7 bits  6..0
        if (a!=0) {
            if (offset+4 > clen) goto error;
            cbuf[offset++] = (unsigned char)(a|HIGHBIT);
            cbuf[offset++] = (unsigned char)(b|HIGHBIT);
            cbuf[offset++] = (unsigned char)(c|HIGHBIT);
            cbuf[offset++] = (unsigned char)(d);
        }
        if (b!=0) {
            if (offset+3 > clen) goto error;
            cbuf[offset++] = (unsigned char)(b|HIGHBIT);
            cbuf[offset++] = (unsigned char)(c|HIGHBIT);
            cbuf[offset++] = (unsigned char)(d);
        }
        if (c!=0) {
            if (offset+2 > clen) goto error;
            cbuf[offset++] = (unsigned char)(c|HIGHBIT);
            cbuf[offset++] = (unsigned char)(d);
        }
        if (offset+1 > clen) goto error;
        cbuf[offset++] = (unsigned char)(d);
    }
    return offset;

error:
    std::cout << "Output buffer overrun: clen = " << clen <<
                 "offset = " << offset << std::endl;
    assert(false);
}

uint32_t PixCodec::varCompress(     // return: number of output bytes
    unsigned char* cbuf,            // output: array of packed bits
    uint32_t clen,                  // input:  output buffer capacity
    const vector<uint32_t>& uvec)   // input:  vector to compress
{
    uint32_t delta = 0;
    uint32_t lastX = 0;
    uint32_t offset = 0;

    for (vector<uint32_t>::const_iterator it = uvec.begin(); it!=uvec.end(); ++it) {
        uint32_t x = *it;
        assert(lastX < x);
        delta = x-lastX;
        lastX = x;

        assert((delta&0xf0000000) == 0);

        uint32_t a = (delta & 0x0fe00000) >> 21; // 7 bits 27..21
        uint32_t b = (delta & 0x001fc000) >> 14; // 7 bits 20..14
        uint32_t c = (delta & 0x00003f80) >>  7; // 7 bits 13..7
        uint32_t d = (delta & 0x0000007f);       // 7 bits  6..0
        if (a!=0) {
            assert(offset+4 <= clen);
            cbuf[offset++] = (unsigned char)(a|HIGHBIT);
            cbuf[offset++] = (unsigned char)(b|HIGHBIT);
            cbuf[offset++] = (unsigned char)(c|HIGHBIT);
            cbuf[offset++] = (unsigned char)(d);
        }
        if (b!=0) {
            assert(offset+3 <= clen);
            cbuf[offset++] = (unsigned char)(b|HIGHBIT);
            cbuf[offset++] = (unsigned char)(c|HIGHBIT);
            cbuf[offset++] = (unsigned char)(d);
        }
        if (c!=0) {
            assert(offset+2 <= clen);
            cbuf[offset++] = (unsigned char)(c|HIGHBIT);
            cbuf[offset++] = (unsigned char)(d);
        }
        assert(offset+1 <= clen);
        cbuf[offset++] = (unsigned char)(d);
    }
    return offset;
}

uint32_t PixCodec::varCompress(     // return: number of output bytes
    unsigned char* cbuf,            // output: output array
    uint32_t offset0,               // input:  output buffer offset
    uint32_t max_offset,            // input:  output buffer capacity
    const vector<uint32_t>& uvec)   // input:  vector to compress
{
    uint32_t offset = offset0;

    for (vector<uint32_t>::const_iterator it = uvec.begin(); it!=uvec.end(); ++it) {
        uint32_t x = *it;

        assert((x&0xf0000000) == 0);

        uint32_t a = (x & 0x0fe00000) >> 21; // 7 bits 27..21
        uint32_t b = (x & 0x001fc000) >> 14; // 7 bits 20..14
        uint32_t c = (x & 0x00003f80) >>  7; // 7 bits 13..7
        uint32_t d = (x & 0x0000007f);       // 7 bits  6..0
        if (a!=0) {
            assert(offset+3 <= max_offset);
            cbuf[offset++] = (unsigned char)(a|HIGHBIT);
            cbuf[offset++] = (unsigned char)(b|HIGHBIT);
            cbuf[offset++] = (unsigned char)(c|HIGHBIT);
            cbuf[offset++] = (unsigned char)(d);
        }
        else if (b!=0) {
            assert(offset+2 <= max_offset);
            cbuf[offset++] = (unsigned char)(b|HIGHBIT);
            cbuf[offset++] = (unsigned char)(c|HIGHBIT);
            cbuf[offset++] = (unsigned char)(d);
        }
        else if (c!=0) {
            assert(offset+1 <= max_offset);
            cbuf[offset++] = (unsigned char)(c|HIGHBIT);
            cbuf[offset++] = (unsigned char)(d);
        }
        else {
            assert(offset <= max_offset);
            cbuf[offset++] = (unsigned char)(d);
        }
    }
    return (offset-offset0);
}

uint32_t PixCodec::varCompress(     // return: number of output bytes
    unsigned char* cbuf,            // output: output array
    uint32_t offset0,               // input:  output segment offset
    uint32_t max_offset,            // input:  output buffer capacity
    const vector<uint32_t>& uvec,   // input:  vector to compress
    uint32_t uvec_offset,           // input:  input segment offset
    uint32_t uvec_len)              // output: input segment length
{
    uint32_t offset = offset0;

    for (uint32_t i=0; i<uvec_len; ++i) {
        uint32_t x = uvec[uvec_offset+i];

        assert((x&0xf0000000) == 0);

        uint32_t a = (x & 0x0fe00000) >> 21; // 7 bits 27..21
        uint32_t b = (x & 0x001fc000) >> 14; // 7 bits 20..14
        uint32_t c = (x & 0x00003f80) >>  7; // 7 bits 13..7
        uint32_t d = (x & 0x0000007f);       // 7 bits  6..0
        if (a!=0) {
            assert(offset+3 <= max_offset);
            cbuf[offset++] = (unsigned char)(a|HIGHBIT);
            cbuf[offset++] = (unsigned char)(b|HIGHBIT);
            cbuf[offset++] = (unsigned char)(c|HIGHBIT);
            cbuf[offset++] = (unsigned char)(d);
        }
        else if (b!=0) {
            assert(offset+2 <= max_offset);
            cbuf[offset++] = (unsigned char)(b|HIGHBIT);
            cbuf[offset++] = (unsigned char)(c|HIGHBIT);
            cbuf[offset++] = (unsigned char)(d);
        }
        else if (c!=0) {
            assert(offset+1 <= max_offset);
            cbuf[offset++] = (unsigned char)(c|HIGHBIT);
            cbuf[offset++] = (unsigned char)(d);
        }
        else {
            assert(offset <= max_offset);
            cbuf[offset++] = (unsigned char)(d);
        }
    }
    return (offset-offset0);
}

uint32_t PixCodec::varCompress(     // return: number of output bytes
    vector<unsigned char>& cvec,    // output: vector of compressed values
    const vector<uint32_t>& uvec)   // input:  vector of uncompressed values
{
    uint32_t n = 0;
    for (vector<uint32_t>::const_iterator it = uvec.begin(); it!=uvec.end(); ++it) {
        uint32_t x = *it;
        assert((x&0xf0000000) == 0);
        uint32_t a = (x & 0x0fe00000) >> 21; // 7 bits 27..21
        uint32_t b = (x & 0x001fc000) >> 14; // 7 bits 20..14
        uint32_t c = (x & 0x00003f80) >>  7; // 7 bits 13..7
        uint32_t d = (x & 0x0000007f);       // 7 bits  6..0

        if (a!=0) {
            cvec.push_back( (unsigned char)(a|HIGHBIT) ); 
            cvec.push_back( (unsigned char)(b|HIGHBIT) );
            cvec.push_back( (unsigned char)(c|HIGHBIT) );
            cvec.push_back( (unsigned char)d );
            n += 4;
        } else if (b!=0) {
            cvec.push_back( (unsigned char)(b|HIGHBIT) );
            cvec.push_back( (unsigned char)(c|HIGHBIT) );
            cvec.push_back( (unsigned char)d );
            n += 3;
        } else if (c!=0) {
            cvec.push_back( (unsigned char)(c|HIGHBIT) );
            cvec.push_back( (unsigned char)d );
            n += 2;
        } else {
            cvec.push_back( (unsigned char)d );
            n += 1;
        }
    }
    return n;
}

uint32_t PixCodec::varCompress(     // return: number of output bytes
    vector<unsigned char>& cvec,    // output: vector of compressed values
    const vector<uint64_t>& uvec)   // input:  vector of uncompressed values
{
    uint32_t counter[] = {0, 0, 0, 0, 0, 0, 0, 0, 0};
    uint32_t n = 0;
    for (vector<uint64_t>::const_iterator it = uvec.begin(); it!=uvec.end(); ++it) {
        uint64_t x = *it;

        assert((x&0x8000000000000000) == 0);

        uint32_t a = (x & 0x7f00000000000000) >> 56; // 7 bits 62..56
        uint32_t b = (x & 0x00fe000000000000) >> 49; // 7 bits 55..49
        uint32_t c = (x & 0x0001fc0000000000) >> 42; // 7 bits 48..42
        uint32_t d = (x & 0x000003f800000000) >> 35; // 7 bits 41..35
        uint32_t e = (x & 0x00000007f0000000) >> 28; // 7 bits 34..28
        uint32_t f = (x & 0x000000000fe00000) >> 21; // 7 bits 27..21
        uint32_t g = (x & 0x00000000001fc000) >> 14; // 7 bits 20..14
        uint32_t h = (x & 0x0000000000003f80) >>  7; // 7 bits 13..07
        uint32_t i = (x & 0x000000000000007f);       // 7 bits 06..00

        if (a!=0) {
            cvec.push_back( (unsigned char)(a|HIGHBIT) ); 
            cvec.push_back( (unsigned char)(b|HIGHBIT) );
            cvec.push_back( (unsigned char)(c|HIGHBIT) );
            cvec.push_back( (unsigned char)(d|HIGHBIT) );
            cvec.push_back( (unsigned char)(e|HIGHBIT) );
            cvec.push_back( (unsigned char)(f|HIGHBIT) );
            cvec.push_back( (unsigned char)(g|HIGHBIT) );
            cvec.push_back( (unsigned char)(h|HIGHBIT) );
            cvec.push_back( (unsigned char)i );
            n += 9;
            counter[0]++;
        } else if (b!=0) {
            cvec.push_back( (unsigned char)(b|HIGHBIT) );
            cvec.push_back( (unsigned char)(c|HIGHBIT) );
            cvec.push_back( (unsigned char)(d|HIGHBIT) );
            cvec.push_back( (unsigned char)(e|HIGHBIT) );
            cvec.push_back( (unsigned char)(f|HIGHBIT) );
            cvec.push_back( (unsigned char)(g|HIGHBIT) );
            cvec.push_back( (unsigned char)(h|HIGHBIT) );
            cvec.push_back( (unsigned char)i );
            n += 8;
            counter[1]++;
        } else if (c!=0) {
            cvec.push_back( (unsigned char)(c|HIGHBIT) );
            cvec.push_back( (unsigned char)(d|HIGHBIT) );
            cvec.push_back( (unsigned char)(e|HIGHBIT) );
            cvec.push_back( (unsigned char)(f|HIGHBIT) );
            cvec.push_back( (unsigned char)(g|HIGHBIT) );
            cvec.push_back( (unsigned char)(h|HIGHBIT) );
            cvec.push_back( (unsigned char)i );
            n += 7;
            counter[2]++;
      } else if (d!=0) {
            cvec.push_back( (unsigned char)(d|HIGHBIT) );
            cvec.push_back( (unsigned char)(e|HIGHBIT) );
            cvec.push_back( (unsigned char)(f|HIGHBIT) );
            cvec.push_back( (unsigned char)(g|HIGHBIT) );
            cvec.push_back( (unsigned char)(h|HIGHBIT) );
            cvec.push_back( (unsigned char)i );
            n += 6;
            counter[3]++;
        } else if (e!=0) {
            cvec.push_back( (unsigned char)(e|HIGHBIT) );
            cvec.push_back( (unsigned char)(f|HIGHBIT) );
            cvec.push_back( (unsigned char)(g|HIGHBIT) );
            cvec.push_back( (unsigned char)(h|HIGHBIT) );
            cvec.push_back( (unsigned char)i );
            n += 5;
            counter[4]++;
        } else if (f!=0) {
            cvec.push_back( (unsigned char)(f|HIGHBIT) );
            cvec.push_back( (unsigned char)(g|HIGHBIT) );
            cvec.push_back( (unsigned char)(h|HIGHBIT) );
            cvec.push_back( (unsigned char)i );
            n += 4;
            counter[5]++;
        } else if (g!=0) {
            cvec.push_back( (unsigned char)(g|HIGHBIT) );
            cvec.push_back( (unsigned char)(h|HIGHBIT) );
            cvec.push_back( (unsigned char)i );
            n += 3;
            counter[6]++;
        } else if (h!=0) {
            cvec.push_back( (unsigned char)(h|HIGHBIT) );
            cvec.push_back( (unsigned char)i );
            n += 2;
            counter[7]++;
        } else {
            cvec.push_back( (unsigned char)i );
            n += 1;
            counter[8]++;
        }
    }

#ifdef DEBUG
    cout << "pack[9] = " << counter[0] <<
            "\npack[8] = " << counter[1] << 
            "\npack[7] = " << counter[2] <<
            "\npack[6] = " << counter[3] <<
            "\npack[5] = " << counter[4] <<
            "\npack[4] = " << counter[5] <<
            "\npack[3] = " << counter[6] <<
            "\npack[2] = " << counter[7] <<
            "\npack[1] = " << counter[8] << endl;
#endif

    return n;
}

void PixCodec::varUncompress(
    vector<uint32_t>& uvec,         // output: vector of uncompressed values
    unsigned char* cbuf,            // input: array of compressed values
    uint32_t coffset,               // input: start offset
    uint32_t clen)                  // input: input byte count
{
    unsigned char* it = cbuf+coffset;
    unsigned char* end = it + clen;
    uint32_t accum = 0;

    while (it!=end) {
        int result = 0;
        for (uint32_t n=0; n<4; ++n) {
            if (it==end) break;
            unsigned char w = *it++;
            result <<= 7;
            if (!(w&HIGHBIT)) {
                result |=  w;
                break;
            }
            result |= (w&LOWMASK);
        }
        uvec.push_back(accum += result);
    }
}

void PixCodec::varUncompress(
    vector<uint32_t>& uvec,                         // output: vector of uncompressed values
    vector<unsigned char>::const_iterator& begin,   // start position of input vector<char>
    vector<unsigned char>::const_iterator& end)     // end position of input vector<char>
{
    uint32_t accum = 0;
    vector<unsigned char>::const_iterator it = begin;
    for (; it!=end; ++it) {
        int result = 0;
        uint32_t n = 0;
        while (true) {
            char w = *it;
            result <<= 7;
            if (++n==4 || !(w&HIGHBIT)) {
                result |=  w;
                break;
            }
            result |= (w&LOWMASK);
        }
        uvec.push_back(accum += result);
    }
}

void PixCodec::varUncompress(
    vector<uint32_t>& uvec,                 // output: vector of uncompressed values
    const vector<unsigned char>& cvec,      // input:  vector of compressed values
    uint32_t coffset,                       // input:  starting index in cvec
    uint32_t clen)                          // input:  input count
{
    if (cvec.begin()+coffset > cvec.end()) {
        cout << "PixCodec::varUncompress got bad length [01]" << std::endl;
        return;
    }
    vector<unsigned char>::const_iterator it  = cvec.begin() + coffset;
    if (it + clen > cvec.end()) {
        cout << "PixCodec::varUncompress got bad length [02]" << std::endl;
        return;
    }

    uint32_t accum = 0;
    vector<unsigned char>::const_iterator end = it + clen;
    while (it!=end) {
        int result = 0;
        for (uint32_t n=0; n<4; ++n) {
            if (it==end) break;
            unsigned char w = *it++;
            result <<= 7;
            if (!(w&HIGHBIT)) {
                result |=  w;
                break;
            }
            result |= (w&LOWMASK);
        }
        uvec.push_back(accum += result);
    }
}

void PixCodec::varUncompress(
    vector<uint64_t>& uvec,             // output: vector of uncompressed values
    const vector<unsigned char>& cvec,  // input:  vector of compressed values
    uint32_t coffset,                   // input:  starting index in cvec
    uint32_t clen)                      // input:  input count
{

    if (cvec.begin()+coffset > cvec.end()) {
        cout << "PixCodec::varUncompress got bad length [03]" << std::endl;
        return;
    }
    vector<unsigned char>::const_iterator it  = cvec.begin() + coffset;
    if (it + clen > cvec.end()) {
        cout << "PixCodec::varUncompress got bad length [04]" << std::endl;
        return;
    }
    vector<unsigned char>::const_iterator end = it + clen;
    while (it!=end) {
        uint64_t result = 0;
        for (uint32_t n=0; n<9; ++n) {
            if (it==end) break;
            unsigned char w = *it++;
            result <<= 7;
            if (!(w&HIGHBIT)) {
                result |=  w;
                break;
            }
            result |= (w&LOWMASK);
        }
        uvec.push_back(result);
    }
}


// Gamma coding
// ------------

uint32_t PixCodec::gammaCompress(   // return: number of output bits
    unsigned char* cbuf,            // output: array of packed bits
    uint32_t  clen,                 // input:  output buffer capacity
    uint32_t* ubuf,                 // input:  array to compress
    uint32_t uoffset,               // input:  start offset for input
    uint32_t ulen)                  // input:  input length
{
    if (ulen==0) return 0;
    BitBuffer bits(cbuf,clen);
    bits.clear();
    uint32_t k = 0;                 // bit buffer index
    uint32_t delta;                 // (ubuf[a] - ubuf[a-1])
    uint32_t lastX = 0;             // ubuf[a-1]

    for (uint32_t a=uoffset; a<uoffset+ulen; ++a) {
        uint32_t x = ubuf[a];       //cout << "x=" << x;
        assert(lastX < x);
        delta = x-lastX;            //cout << ",delta=" << delta;
        lastX = x;
        uint32_t lg = log(delta);   //cout << ",log(delta)=" << lg;
        uint32_t r = delta-(1<<lg); //cout << ",residue=" << r << endl;

        // unary code 1+log(x)
        if (debug)
            std::cout << "unary code "<<(1+lg)<<std::endl;
        uint32_t i;
        for (i=0; i<=lg; i++,k++)
            bits.set(k);
        k++;                        // trailing 0 ends unary

        // binary code residue
        if (debug)
            std::cout << "binary code residue "<<r<<std::endl;
        for (i=0; i<lg; i++, k++) {
            if (r & 1) bits.set(k);
            r >>= 1;
        }
    }

    while ((k&7) != 0) k++;         // byte padding
    return k;
}

uint32_t PixCodec::gammaCompress(   // return: number of output bits
    unsigned char* cbuf,            // output: array of packed bits
    uint32_t clen,                  // input:  output buffer capacity
    vector<uint32_t> uvec)          // input:  vector to compress
{
    if (uvec.size()==0) return 0;
    BitBuffer bits(cbuf,clen);
    uint32_t k = 0;                 // bit buffer index
    uint32_t delta;                 // (uvec[a] - uvec[a-1])
    uint32_t lastX = 0;             // uvec[a-1]

    for (vector<uint32_t>::const_iterator it = uvec.begin(); it!=uvec.end(); ++it) {
        uint32_t x = *it;           //cout << "x = " << x << endl;
        assert(lastX < x);
        delta = x-lastX;            //cout << "delta = " << delta << endl;
        lastX = x;
        uint32_t lg = log(delta);   //cout << "log(delta) = " << lg << endl;
        uint32_t r = delta-(1<<lg); //cout << "residue   = " << r << endl;

        // unary code 1+log(x)
        uint32_t i;
        for (i=0; i<=lg; i++,k++) bits.set(k);
        k++;                        // trailing 0 ends unary

        // binary code residue
        for (i=0; i<lg; i++, k++) {
            if ((r & 1)==1) bits.set(k);
            r >>= 1;
        }
    }
    while ((k&7) != 0) k++;         // byte padding
    return k;
}

void PixCodec::gammaUncompress(
    vector<uint32_t>& uvec,         // output: vector of uncompressed values
    unsigned char* cbuf,            // input:  array of compressed values
    uint32_t coffset,               // input:  start offset
    uint32_t clen)                  // input:  input byte count
{
    if (clen==0) return;
    BitBuffer bits(&cbuf[coffset],clen<<3);
    uint32_t base;
    uint32_t residue;
    uint32_t accum = 0;

    uint32_t k = 0;                 // bit index
    while (k < clen<<3) {
        base = 1;
        uint32_t lg = 0;
        residue = 0;

        if (!bits.get(k))
            break;                  // break on trailing padding
    
        // decode unary part
        while (bits.get(++k)) {
            base <<= 1;
            lg++;
        }

        // decode binary part
        uint32_t i;
        for (i=k+lg; i>k; i--) {
            residue = (residue<<1) | (bits.get(i) ? 1 : 0);
        }
        k += (lg+1);
        accum += (base + residue);
        uvec.push_back(accum);
    }
}

// Delta coding
// ------------

uint32_t PixCodec::deltaCompress(   // return: number of output bits
    unsigned char* cbuf,            // output: array of packed bits
    uint32_t clen,                  // input:  output buffer capacity
    uint32_t* ubuf,                 // input:  array to compress
    uint32_t uoffset,               // input:  start offset for input
    uint32_t ulen)                  // input:  input length
{
    if (ulen==0) return 0;
    BitBuffer bits(cbuf,clen);
    bits.clear();
    uint32_t k = 0;                 // bit buffer index
    uint32_t delta;                 // (ubuf[a] - ubuf[a-1])
    uint32_t lastX = 0;             // ubuf[a-1]

    for (uint32_t a=uoffset; a<uoffset+ulen; ++a) {
        uint32_t x = ubuf[a];               //cout << "x = " << x << endl;
        assert(lastX < x);
        delta = x - lastX;
        lastX = x;
        uint32_t l  = log(1+delta);         //cout << "log(1+delta) = " << l << endl;
        uint32_t r  = (1+delta)-(1<<l);     //cout << "delta residue = " << r << endl;
        uint32_t ll = log(1+l);             //cout << "log(1+log(delta)) = " << ll << endl;
        uint32_t rr = (1+l)-(1<<ll);        //cout << "log-residue = " << rr << endl;

    //Gamma code 1+log(x)
    uint32_t i;
    for (i=0; i<=ll; i++,k++) bits.set(k);  // unary code ll
    k++;
    for (i=0; i<ll; i++,k++) {              // binary code rr
        if ((rr & 1)==1) bits.set(k);
        rr >>= 1;
    }

    //Binary code residue
    for (i=0; i<l; i++, k++) {
        if ((r & 1)==1) bits.set(k);
        r >>= 1;
        }
     }
    while ((k&7) != 0) k++;                 // byte padding
    return k;
}


uint32_t PixCodec::deltaCompress(   // return: number of output bits
    unsigned char* cbuf,            // output: array of packed bits
    uint32_t  clen,                 // input:  output buffer capacity
    vector<uint32_t> uvec)          // input:  input vector to uncompress
{
    if (uvec.size()==0) return 0;
    BitBuffer bits(cbuf,clen);
    bits.clear();
    uint32_t k = 0;                 // bit buffer index
    uint32_t delta;                 // (ubuf[a] - ubuf[a-1])
    uint32_t lastX = 0;             // ubuf[a-1]

    for (vector<uint32_t>::const_iterator it=uvec.begin(); it!=uvec.end(); ++it) {
        uint32_t x = *it;                   //cout << "x = " << x << endl;
        assert(lastX < x);
        delta = x - lastX;
        lastX = x;
        uint32_t l  = log(1+delta);         //cout << "log(1+delta) = " << l << endl;
        uint32_t r  = (1+delta)-(1<<l);     //cout << "delta residue = " << r << endl;
        uint32_t ll = log(1+l);             //cout << "log(1+log(delta)) = " << ll << endl;
        uint32_t rr = (1+l)-(1<<ll);        //cout << "log-residue = " << rr << endl;

        //Gamma code 1+log(x)
        uint32_t i;
        for (i=0; i<=ll; i++,k++) bits.set(k);    // unary code ll
        k++;
        for (i=0; i<ll; i++,k++) {                // binary code rr
            if ((rr & 1)==1) bits.set(k);
            rr >>= 1;
        }
    
        //Binary code residue
        for (i=0; i<l; i++, k++) {
            if ((r & 1)==1) bits.set(k);
            r >>= 1;
        }
    }
    while ((k&7) != 0) k++;                 // byte padding
    return k;
}

void PixCodec::deltaUncompress(
    vector<uint32_t>& uvec,         // output: vector of uncompressed values
    unsigned char* cbuf,            // input:  array of compressed values
    uint32_t coffset,               // input:  start offset
    uint32_t clen)                  // input:  input byte count
{
    if (clen==0) return;
    BitBuffer bits(&cbuf[coffset],clen<<3);
    uint32_t base;
    uint32_t lgBase;
    uint32_t residue;
    uint32_t accum = 0;

    uint32_t k = 0;
    while (k < clen<<3) {
        lgBase = base = 1;
        uint32_t lglg = 0;
        uint32_t lg   = 0;
        residue = 0;
        if (!bits.get(k)) break;            // break on trailing padding

        while (bits.get(++k)) {
            lgBase <<= 1;
            lglg++;
        }
        uint32_t i;
        for (i=k+lglg; i>k; i--) {
            residue = (residue<<1) | (bits.get(i) ? 1 : 0);
        }
        k += (lglg+1);
        lg = lgBase + residue - 1;
        for (i=0; i<lg; i++) { base <<= 1; };
        residue = 0;
        for (i=k+lg-1; i>=k; i--) {
            residue = (residue<<1) | (bits.get(i) ? 1 : 0);
        }
        k += lg;
        accum += (base + residue - 1);  // input is offset encoded
        uvec.push_back(accum);
    }
}

// Bernoulli codec

static uint32_t bernoulliCompress(  // return: bit count
    unsigned char *cbuf,            // output: output buffer - compressed
    uint32_t  clen,                 // input: output buffer capacity
    const uint32_t* ubuf,           // input: input buffer
    uint32_t ulen,                  // input: input buffer length
    uint32_t b)                     // input: parameter (0 => gamma)
{
    BitBuffer* _bitset = new BitBuffer(cbuf, clen);
    BitBuffer& bitset = *_bitset;
    uint32_t i, lg, q, r, x;
    uint32_t k = 0;  // current bit set index
    uint32_t lastX = 0;
    uint32_t delta;
    uint32_t bitCount;

    if (b==0) {

        k++;    // add leading 0 bit
        for (uint32_t a=0; a < ulen; a++) {
            x = ubuf[a];              
            delta = x - lastX;
            lastX = x;
            lg = log(delta);        
            r  = delta - (1<<lg);   

            if (debug)
                std::cout << "x = "<<x<<"\nlog(x) = "<<lg<<"\nr = "<<r<< std::endl;

            for (i=0; i<lg; i++,k++) bitset.set(k);
            k++; // add trailing 0

            if (debug)
                std::cout << "binary code residue" << std::endl;

            for (i=0; i<lg; i++, k++) {
                if ((r & 1)==1) bitset.set(k);
                r >>= 1;
            }
        }

    } else {

        if (debug) std::cout << "b > 0 => compress leading value" << std::endl;

        lg = log(b);
        r  = b - (1<<lg);

        if (debug)
            std::cout << "log(b) = " << lg << "\nresidue = " << r << std::endl;

        for (i=0; i<lg; i++,k++) bitset.set(k);
        k++;    // add trailing 0

        for (i=0; i<lg; i++, k++) {
            if ((r & 1)==1) bitset.set(k);
            r >>= 1;
        }

        if (debug)
            std::cout << "Bernoulli: compress remaining bits" << std::endl;

        for (int a = 0; a < ulen; a++) {
            x     = ubuf[a];
            delta = x - lastX;
            lastX = x;
            q = (delta-1)/b;

            if (debug)
                std::cout << "x = " << x << "\nq = " << q << std::endl;

            if (q > 6) {
                lg = log(x);
                r  = delta - (1<<lg);
                k++;    // add trailing 0
                for (i=0; i<lg; i++,k++) bitset.set(k);

                if (debug) {
                    std::cout << "Gamma code" 
                              << "\nlog(x) = " << lg 
                              << "\nresidue = " << r << std::endl;
                }

            } else {
                if (debug)
                    std::cout << "Bernoulli code" << std::endl;
                lg = log(b) + 1;
                r = delta - q*b - 1;
                if (debug) std::cout << "residue = " << r << std::endl;
                for (i=0; i<=q; i++,k++) bitset.set(k);
            }

            if (debug)
                std::cout << "Binary code residue" << std::endl;
            k++;    // add trailing 0
            for (i=0; i<lg; i++, k++) {
                if ((r & 1)==1) bitset.set(k);
                r >>= 1;
            }
        }
    }

    while ((k&7) != 0) k++; // padding
    bitCount = k;

    bitset.pack(cbuf, clen);
    return bitCount;
}

static void bernoulliUnCompress(
    unsigned char* cbuf,            // input: input buffer, compressed
    uint32_t clen,                  // input: input buffer length
    std::vector<uint32_t>& uvec)    // output: uncompressed vector
{
    uint32_t b, i, k, l, lg, q, base, residue;

    BitBuffer* _bitset = new BitBuffer(cbuf, clen);
    BitBuffer& bitset = *_bitset;
    uint32_t accum = 0;

    if (!bitset.get(0)) {
        if (debug) std::cout << "param =0, Gamma decode only" << std::endl;
        k = 1;

        for (k=0; k<bitset.getBitCount(); ) {
            base    = 1;
            lg      = 0;
            residue = 0;
            if (!bitset.get(k)) break;
            while (bitset.get(k++))   { base <<= 1; lg++; }
            for (i=k+lg-1; i>=k; i--) { residue = (residue<<1) | (bitset.get(i) ? 1 : 0); }
            k += lg;
            accum += (base + residue);
            uvec.push_back(accum);
        }
    } else {
        if (debug) std::cout << "parameter >0, Bernoulli decode" << std::endl;
        k       = 0;
        base    = 1;
        lg      = 0;
        residue = 0;

        while (bitset.get(k++))   { base <<= 1; lg++; }
        for (i=k+lg-1; i>=k; i--) { residue = (residue<<1) | (bitset.get(i) ? 1 : 0); }
        k += lg;
        b = base + residue;

        if (debug) std::cout << "Bernoulli decode remaining bits" << std::endl;
        lg = log(b)+1;
        while(k < bitset.getBitCount()) {
            q       = 0;
            residue = 0;
            base    = 1;
            if (!bitset.get(k)) {   // Gamma decode
                k++;
                if (!bitset.get(k)) break;  // 00  -> end of coding
                l = 0;
                while (bitset.get(k++))  { base <<= 1; l++; }
                for (i=k+l-1; i>=k; i--) { residue = (residue<<1) | (bitset.get(i) ? 1 : 0); }
                k += l;
                uvec.push_back(base + residue);
            } else {                // Bernoulli decode
                while (bitset.get(k++))   { q++; }
                for (i=k+lg-1; i>=k; i--) { residue = (residue<<1) | (bitset.get(i) ? 1 : 0); }
                k += lg;
                accum += ((q-1)*b + residue + 1);
                uvec.push_back(accum);
            }
        }
    }
}

}   // namespace mongo

