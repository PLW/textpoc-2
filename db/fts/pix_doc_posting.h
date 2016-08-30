#ifndef __DOCPOSTING_H_
#define __DOCPOSTING_H_

#include <iostream>
#include <vector>
#include <utility>

#include "RC.h"
#include "Coder.h"
#include "Logger.h"

namespace mongo {

/*_____________________________________________________________________
|                                                                      |
|  DocPosting contains all the information about the appearances       |
|  of one term in one document.                                        |
|                                                                      |
|  DocPosting index format:                                            |
|                                                                      |
|     docid           -- document id                                   |
|     length          -- pos list length = term frequency = tf         |
|     pos             -- term position in doc                          |
|     ...                                                              |
|     pos                                                              |
|                                                                      |
|     The list appears uncompressed as   vector<unsigned>              |
|     The list is compressed to index as vector<unsigned char>         |
|______________________________________________________________________*/

class DocPosting
{
protected:    // state
    unsigned docid;
    unsigned tf;                 // term frequency (= posv.size())
    float score;                 // aggregate score
    std::vector<unsigned> uvec;  // uncompressed index list

public:
    // Create from compressed index list window
    DocPosting(
        const std::vector<unsigned char>& ifv,// compressed index list
        unsigned offset,         // starting offset
        unsigned length);        // byte length

    DocPosting(
        const std::vector<unsigned char>& ifv,// compressed index list
        unsigned offset,         // starting offset
        unsigned length,         // byte length
        Logger& logger);

    // Create from compressed index list window
    DocPosting(
        unsigned char* ifv,      // compressed index list
        unsigned offset,         // starting offset
        unsigned length);        // byte length

    // Create from uncompressed inputs
    DocPosting(
        unsigned docid,                    // new document id
        const std::vector<unsigned> posv); // position list

    DocPosting(
        unsigned docid,          // new document id
        unsigned pos);           // position list

    // Create empty DocPosting
    DocPosting(unsigned d) : docid(d) {}

    // common initialization
    void init(
        unsigned docid,                    // new document id
        const std::vector<unsigned> posv,  // position list
        bool takeDeltas);                  // store (posv[i] - posv[i-1])

    DocPosting() {}

    ~DocPosting();

    // getters & setters
    unsigned getTF() const    { return tf;    }
    float getScore() const    { return score; }
    unsigned getDocid() const { return docid; }
    void setTF(unsigned t)    { tf    = t; }
    void setScore(float s)    { score = s; }
    void setDocid(unsigned d) { docid = d; }
    unsigned getPos(unsigned n) const { return tf>0 ? uvec[2+n] : 0; }
    unsigned size() const     { return uvec.size(); }

    // basic merge
    static void merge(
        const DocPosting&,
        const DocPosting&,
        DocPosting& result);

    // merge with proximity
    static void proximity_merge(
        const DocPosting&,
        const DocPosting&,
        DocPosting& result,
        unsigned& sepresult);

    // iterator interface
    class DocPostingIterator : public RCObject {

    public:
        DocPostingIterator(const DocPosting&);
        ~DocPostingIterator() {}
    
    public: 
        bool done() const;
        void operator++();
        unsigned operator*();
    
    protected:
        std::vector<unsigned>::const_iterator posIt;    // pos list iterator
        std::vector<unsigned>::const_iterator end;
        unsigned pos,lastPos;
    };

    DocPostingIterator iterator() const;
    std::string toString() const;
    std::string compactString() const;

    // append the uvec representation to a given vector
    void append_uvec(std::vector<unsigned>& v);

};

inline DocPosting::DocPosting(
    const std::vector<unsigned char>& ifv,  // raw index list
    unsigned offset,                        // the start offset
    unsigned length)                        // the compressed length
:
    docid(0),
    tf(1),
    score(1.0f),
    uvec()
{
    Coder::varUncompress(uvec, ifv, offset, length);
    if (uvec.size()>=2) {
        docid = uvec[0];
        tf    = uvec[1];
        score = (float)tf;
    }
}

inline DocPosting::DocPosting(
    const std::vector<unsigned char>& ifv,  // raw index list
    unsigned offset,                        // the start offset
    unsigned length,                        // the compressed length
    Logger& logger)
:
    docid(0),
    tf(1),
    score(1.0f),
    uvec()
{
    Coder::varUncompress(uvec, ifv, offset, length, logger);
    if (uvec.size()>=2) {
        docid = uvec[0];
        tf    = uvec[1];
        score = (float)tf;
    }
}

inline DocPosting::DocPosting(
    unsigned char* ifv,                     // raw index list
    unsigned offset,                        // the start offset
    unsigned length)                        // the compressed length
:
    docid(0),
    tf(1),
    score(1.0f),
    uvec()
{
    Coder::varUncompress(uvec, ifv, offset, length);
    if (uvec.size()>=2) {
        docid = uvec[0];
        tf    = uvec[1];
        score = (float)tf;
    }
}

inline DocPosting::DocPosting(
    unsigned _docid,
    const std::vector<unsigned> posv)
: 
    docid(_docid),
    tf(1),
    score (1.0f),
    uvec()
{
    init(docid, posv, false);
}

inline DocPosting::DocPosting(
    unsigned _docid,
    unsigned _pos)
:
    docid(_docid),
    tf(1),
    score(1.0f),
    uvec()
{
  uvec.push_back(docid);
  uvec.push_back(tf);
  uvec.push_back(_pos);
}

inline DocPosting::DocPostingIterator DocPosting::iterator() const
{
    return DocPostingIterator(*this);
}


// DocPostingIterator

inline DocPosting::DocPostingIterator::DocPostingIterator(
    const DocPosting& dp)
:
    lastPos(0)
{
    std::vector<unsigned>::const_iterator p = dp.uvec.begin();
    posIt = p + 2;          // skip tf
    end = posIt + dp.tf;
    pos = *posIt;
    lastPos = pos;
}

inline bool DocPosting::DocPostingIterator::done() const
{
    return (posIt == end);
}

inline void DocPosting::DocPostingIterator::operator++()
{
    ++posIt;
    pos = *posIt + lastPos;    // expand difference coding
    lastPos = pos;
}

inline unsigned DocPosting::DocPostingIterator::operator*()
{
  return pos;
}

}  /* namespace mongo */
#endif 

