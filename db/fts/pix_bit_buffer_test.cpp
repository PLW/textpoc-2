
#include "pix_bit_buffer.h"

#include <iostream>
#include <string>

int main(int argc, char* argv[])
{
    std::cout << "= = = = = = = = = = = uint64 buffer = = = = = = = = = = = = =";

    uint64_t buf[1024];
    mongo::BitBuffer bitbuf(buf, 1024);

    std::cout << "\n\nset even entries" << std::endl;
    bitbuf.clear();
    for (int i=0; i<256; ++i) { bitbuf.set(i<<1); }
    for (int i=0; i<512; ++i) { std::cout << (bitbuf.get(i) ? "1" : "0"); }

    std::cout << "\n\nappend 1,2 mod 3 entries" << std::endl;
    bitbuf.clear();
    for (int i=0; i<512; ++i) { bitbuf.append( (i%3) ? true : false ); }
    for (int i=0; i<512; ++i) { std::cout << (bitbuf.get(i) ? "1" : "0"); }

    std::cout << "\n\nappend 1,2,3 mod 4 entries" << std::endl;
    bitbuf.clear();
    for (int i=0; i<768; ++i) { bitbuf.append( (i%4) ? true : false ); }
    for (int i=0; i<768; ++i) { std::cout << (bitbuf.get(i) ? "1" : "0"); }

    std::cout << "\n\n= = = = = = = = = = = char buffer = = = = = = = = = = = = = =";

    unsigned char buf0[64];
    mongo::BitBuffer bitbuf0(buf0, 64);

    std::cout << "\n\nset even entries" << std::endl;
    bitbuf0.clear();
    for (int i=0; i<256; ++i) { bitbuf0.set(i<<1); }
    for (int i=0; i<512; ++i) { std::cout << (bitbuf0.get(i) ? "1" : "0"); }

    std::cout << "\n\nappend 1,2 mod 3 entries" << std::endl;
    bitbuf0.clear();
    for (int i=0; i<512; ++i) { bitbuf0.append( (i%3) ? true : false ); }
    for (int i=0; i<512; ++i) { std::cout << (bitbuf0.get(i) ? "1" : "0"); }

    std::cout << "\n\nappend 1,2,3 mod 4 entries" << std::endl;
    bitbuf0.clear();
    for (int i=0; i<512; ++i) { bitbuf0.append( (i%4) ? true : false ); }
    for (int i=0; i<512; ++i) { std::cout << (bitbuf0.get(i) ? "1" : "0"); }

    std::cout << "\n\n= = = = = = = = = = = char buffer = = = = = = = = = = = = = =";

    unsigned char buf1[128];
    mongo::BitBuffer bitbuf1(buf1, 128);
    unsigned char buf2[128];
    mongo::BitBuffer bitbuf2(buf2, 128);

    std::cout << "\n\nappend 0,1 mod 4 entries" << std::endl;
    bitbuf1.clear();
    for (int i=0; i<1024; ++i) { bitbuf1.append( (i%4<2) ? true : false ); }
    std::cout << "bitbuf1 : \n";
    for (int i=0; i<1024; ++i) { std::cout << (bitbuf1.get(i) ? "1" : "0"); }

    std::cout << "\n\nappend 0,1,2 mod 6 entries" << std::endl;
    bitbuf1.clear();
    for (int i=0; i<1024; ++i) { bitbuf1.append( (i%6<3)? true : false ); }
    std::cout << "bitbuf1 : \n";
    for (int i=0; i<1024; ++i) { std::cout << (bitbuf1.get(i) ? "1" : "0"); }

    std::cout << "\n\nset 8 byte blocks" << std::endl;
    for (int i=0; i<16; ++i) { bitbuf2.getBuf0(i) = 0xffffffffffffffffL; }
    std::cout << "bitbuf2 : \n";
    for (int i=0; i<1024; ++i) { std::cout << (bitbuf2.get(i) ? "1" : "0"); }

    std::cout << "\n\nXOR with 8 byte blocks" << std::endl;
    bitbuf1 ^= bitbuf2;
    std::cout << "bitbuf1 ^= bitbuf2 : \n";
    for (int i=0; i<1024; ++i) { std::cout << (bitbuf1.get(i) ? "1" : "0"); }

    std::cout << "\n\nappend 3,4,5 mod 6 entries and compare" << std::endl;
    unsigned char buf3[128];
    mongo::BitBuffer bitbuf3(buf3, 128);
    for (int i=0; i<1024; ++i) { bitbuf3.append( (i%6<3)? false : true ); }
    std::cout << "bitbuf3 : \n";
    for (int i=0; i<1024; ++i) { std::cout << (bitbuf3.get(i) ? "1" : "0"); }

    std::cout << "\n\n(bitbuf1==bitbuf3) = " << (bitbuf1==bitbuf3) << std::endl;
    return 0;
}

