
#include <assert.h>
#include <iostream>

#include "pix_bit_buffer.h"

namespace mongo {

BitBuffer::BitBuffer(uint64_t* _buf, uint32_t _buflen)
:   ubuf(_buf),
    buflen(_buflen),
    offset(0),              // largest bit index in use
    maxoffset(_buflen<<6)   // maximum bit index
{}

BitBuffer::BitBuffer(unsigned char* _buf, uint32_t _buflen)
:   ubuf((uint64_t*)_buf),
    buflen(_buflen >> 3),   // byte length to uint64 length
    offset(0),              // largest bit index in use
    maxoffset(buflen<<6)    // maximum bit index
{}

BitBuffer::BitBuffer(unsigned char* _buf, uint32_t _buflen, uint32_t _offset)
:   ubuf((uint64_t*)_buf),
    buflen(_buflen >> 3),
    offset(_offset),
    maxoffset(buflen<<6)
{}

BitBuffer::~BitBuffer()
{}

void BitBuffer::set(uint32_t pos) {
    uint64_t p = pos >> 6;
    assert(p<buflen);
    ubuf[p] |= ((uint64_t)1 << (pos & 63));
}

void BitBuffer::clear(uint32_t pos) {
    uint64_t p = pos >> 6;
    assert(p<buflen);
    ubuf[p] &= ~((uint64_t)1 << (pos & 63));
}

void BitBuffer::flip(uint32_t pos) {
    uint64_t p = pos >> 6;
    assert(p<buflen);
    ubuf[p] ^= ((uint64_t)1 << (pos & 63));
}

bool BitBuffer::get(uint32_t pos) const {
    uint64_t p = pos >> 6;
    assert(p<buflen);
    return (ubuf[p] >> (pos & 63)) & 1;
}

void BitBuffer::append(bool bit) {
    assert(offset < maxoffset);
    bit ? set(offset++) : clear(offset++);
}

uint32_t BitBuffer::getBitCount() const {
    return offset;
}

void BitBuffer::setBitCount(uint32_t k) {
    offset = k;
}

void BitBuffer::clear(uint32_t offset, uint32_t length) {
    assert(offset < maxoffset);
    assert(offset+length < maxoffset);
    for (uint32_t i=offset; i<offset+length; ++i) clear(i);
}

void BitBuffer::clear() {
    for (uint32_t i=0; i<buflen; ++i) ubuf[i] = 0l;
    offset = 0;
}

void BitBuffer::pack(unsigned char* cbuf, uint32_t clen) {
    unsigned char* p = (unsigned char*)ubuf;
    unsigned char* q = (unsigned char*)cbuf;
    for (uint32_t i=0; i < (buflen<<2); ++i) *q = *p;
}

void BitBuffer::print() const {
    for (int j=0; j<buflen; ++j)
        std::cout << (get(j) ? "1" : "0");
}

BitBuffer& BitBuffer::operator&=(const BitBuffer& s) {
    assert(s.getBuflen()==buflen);
    for (uint32_t j=0; j<buflen; j++) ubuf[j] &= s.getBuf(j);
    return *this;    
}

BitBuffer& BitBuffer::operator|=(const BitBuffer& s) {
    assert(s.getBuflen()==buflen);
    for (uint32_t j=0; j<buflen; j++) ubuf[j] |= s.getBuf(j);
    return *this;    
}

BitBuffer& BitBuffer::operator^=(const BitBuffer& s) {
    assert(s.getBuflen()==buflen);
    for (uint32_t j=0; j<buflen; j++) ubuf[j] ^= s.getBuf(j);
    return *this;    
}

bool BitBuffer::operator==(const BitBuffer& s) const {
    if (s.getBuflen()!=buflen) return false;
    for (uint32_t j=0; j<buflen; j++)
        if (ubuf[j] != s.getBuf(j)) return false;
    return true;    
}

bool BitBuffer::operator!=(const BitBuffer& s) const {
    if (s.getBuflen()!=buflen) return true;
    for (uint32_t j=0; j<buflen; j++)
        if (ubuf[j] != s.getBuf(j)) return true;
    return false;    
}

}   // namespace mongo 

