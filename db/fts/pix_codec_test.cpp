
#include "pix_codec.h"

#include <iostream>
#include <stdlib.h>

int main(int argc, char* argv[]) {

    uint32_t ulen = 1000;
    uint32_t clen = 10000;
    uint32_t* ubuf = new uint32_t[ulen];
    unsigned char* cbuf = new unsigned char[clen];
    std::vector<uint32_t> uvecv;

    std::cout << "Init" << std::endl;
    for (uint32_t i=0; i<ulen; ++i) {
        uint32_t base = (i>0 ? ubuf[i-1] : 0);
        ubuf[i] = base + (rand()%100) + 1;
        //std::cout << "ubuf[" << i << "] = " << ubuf[i] << std::endl;
    }

    std::cout << "Var compress" << std::endl;
    uint32_t n = mongo::PixCodec::varCompress(cbuf, clen, ubuf, 0, ulen);
    std::cout << "Var uncompress" << std::endl;
    mongo::PixCodec::varUncompress(uvecv, cbuf, 0, n);
    std::cout << "Var compress - check errors" << std::endl;
    for (uint32_t j=0; j<uvecv.size(); ++j) {
        if (uvecv[j] != ubuf[j])
            std::cout << "varCompress error: [" << j << "] "
                      << uvecv[j] << " != " << ubuf[j] << std::endl;
    }

    std::cout << "Gamma compress" << std::endl;
    n = mongo::PixCodec::gammaCompress(cbuf, clen, ubuf, 0, ulen);
    std::cout << "Gamma uncompress, n = " << n << std::endl;
    uvecv.clear();
    mongo::PixCodec::gammaUncompress(uvecv, cbuf, 0, n>>3);
    std::cout << "Gamma compress - check errors" << std::endl;
    for (uint32_t j=0; j<uvecv.size(); ++j) {
        if (uvecv[j] != ubuf[j])
            std::cout << "gammaCompress error: [" << j << "] "
                      << uvecv[j] << " != " << ubuf[j] << std::endl;
    }

    std::cout << "Delta compress" << std::endl;
    n = mongo::PixCodec::deltaCompress(cbuf, clen, ubuf, 0, ulen);
    std::cout << "Delta uncompress, n = " << n << std::endl;
    uvecv.clear();
    mongo::PixCodec::deltaUncompress(uvecv, cbuf, 0, n>>3);
    std::cout << "Delta compress - check errors" << std::endl;
    for (uint32_t j=0; j<uvecv.size(); ++j) {
        if (uvecv[j] != ubuf[j])
            std::cout << "deltaCompress error: [" << j << "] "
                      << uvecv[j] << " != " << ubuf[j] << std::endl;
    }

}

