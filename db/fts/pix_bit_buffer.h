
#ifndef __BITBUFFER_H_
#define __BITBUFFER_H_

#include <stdint.h>

namespace mongo {

class BitBuffer {
protected:
    uint64_t* ubuf;         // raw buffer, 64 bits per entry
    uint32_t  buflen;       // raw buffer allocated size
    uint32_t  offset;       // index of highest bit used
    uint32_t  maxoffset;    // index of last possible bit in raw buffer

public:
    BitBuffer(uint64_t* buf, uint32_t buflen);
    BitBuffer(unsigned char* buf, uint32_t buflen);
    BitBuffer(unsigned char* buf, uint32_t buflen, uint32_t offset);
    ~BitBuffer();

    bool get(uint32_t pos) const;
    void set(uint32_t pos);
    void clear(uint32_t pos);
    void flip(uint32_t pos);
    void append(bool bit);
    void clear(uint32_t offset, uint32_t length);
    void clear();
    void setBitCount(uint32_t);
    uint32_t getBitCount() const;

    uint32_t  getBuflen() const { return buflen; }
    uint32_t  getOffset() const { return offset; }
    uint64_t  getBuf(uint32_t k) const { return ubuf[k]; }
    uint64_t& getBuf0(uint32_t k) { return ubuf[k]; }

    void pack(unsigned char* cbuf, uint32_t clen);
    void print() const;

    BitBuffer& operator&=(const BitBuffer& s);  // AND with a given BitBuffer
    BitBuffer& operator|=(const BitBuffer& s);  // OR  with a given BitBuffer
    BitBuffer& operator^=(const BitBuffer& s);  // XOR with a given BitBuffer
    bool operator==(const BitBuffer& s) const;
    bool operator!=(const BitBuffer& s) const;

};

}   // namespace mongo 

#endif  // !BITBUFFER
