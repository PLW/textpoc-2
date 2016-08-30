
#ifndef __POSTING_LIST_H_
#define __POSTING_LIST_H_

#include <vector>

#include "docid_set.h"
#include "Logger.h"
#include "DocPosting.h"


namespace mongo {

/*________________________________________________________________
|                                                                 |
|  A sequence of DocPostings                                      |
|_________________________________________________________________*/

class PostingList
{
public:    // types
    typedef std::pair<unsigned,unsigned> hit_t;
    typedef std::vector<hit_t> hitlist_t;

protected: // state
    std::vector<DocPosting> uvec;              // uncompressed format
    const std::vector<unsigned char>* cvec;    // compressed raw posting list
    std::string term;                          // index term
    float handicap;                            // scoring handicap.

public:

    PostingList(    // construct from compressed index list 
        const std::string& term,
        const std::vector<unsigned char>*);

    PostingList(
        const std::string& term,
        const std::vector<unsigned char>*,
        Logger& logger);

    PostingList(    // construct from compressed index list 
        const std::string& term,
        unsigned char*,
        unsigned len);

    PostingList(    // variant: cut on DocPosting bad length
        const std::string& term,
        unsigned char*,
        unsigned len,
        bool);

    PostingList(    // variant: as above, and dedup docids
        const std::string& term,
        unsigned char*,
        unsigned len,
        int);

    PostingList(
        const std::string& term,
        const std::vector<DocPosting>&);

    PostingList(
        const std::string& term);

    PostingList(
        const PostingList& pList,
        const docid_set& dset);

    PostingList() : cvec(NULL) {}

    PostingList(const PostingList& pList)
    : uvec(pList.uvec), cvec(pList.cvec), term(pList.term) {}
    
    ~PostingList() {}

    void swap(PostingList& pL)
    { uvec.swap(pL.uvec); cvec = pL.cvec; term.swap(pL.term); }

    // iterator interface
    typedef std::vector<DocPosting>::const_iterator PostingListIterator;
    PostingListIterator begin() const { return uvec.begin(); }
    PostingListIterator end() const { return uvec.end(); }

    std::string toString() const;

    // manipulators

    // append DocPosting to this list 
    void append(const DocPosting&);

    // append PostingList to this list 
    void append(const PostingList&);

    // return the number of DocPostings in this list
    unsigned size() const;

    // get or set the term for this PostingList 
    const std::string& getTerm() const;
    void setTerm(const std::string& t);

    // get or set the scoring handicap
    float getHandicap() const { return handicap; }
    void setHandicap(float _handicap) { handicap = _handicap; }


    // intersect two PostingLists 
    static void _and(
        const PostingList&,
        const PostingList&,
        PostingList& result);

    // intersect two PostingLists with weights
    static void _and2(
        const PostingList&,
        const PostingList&,
        float w1,
        float w2,
        PostingList& result);

    // accrue two PostingLists
    static void _accrue(
        const PostingList&,
        const PostingList&,
        PostingList& result);

    // accrue two PostingLists with weights
    static void _accrue2(
        const PostingList&,
        const PostingList&,
        float w1,
        float w2,
        PostingList& result);

    // union two PostingLists
    static void _or(
        const PostingList&,
        const PostingList&,
        PostingList& result);

    // merge two PostingLists 
    static void merge(
        const PostingList&,
        const PostingList&,
        PostingList& result);
        //const HashSet& deleteSet);

    // merge phrase PostingLists
    static void _phrase(
        const vector<PostingList*>&,
        PostingList&);

    // merge phrase PostingLists
    static void _phrase2(
        const vector<PostingList*>&,
        const vector<float>&,
        PostingList&);

    // merge two PostingLists in AndNot condition
    static void _andnot(
        const PostingList&,
        const PostingList&,
        PostingList&);

    // merge two PostingLists in Near cndition
    static void _near(
        PostingList&,
        PostingList&,
        int,
        PostingList&);

    // merge two PostingLists in Near cndition
    static void _near2(
        PostingList&,
        PostingList&,
        int,
        float w1,
        float w2,
        PostingList&);

    // filter PostingLists by tag
    static void _tag(
        PostingList&,
        int,
        PostingList&);

    // filter PostingLists by tag
    static void _tag2(
        PostingList&,
        int, float w,
        PostingList&);

    // return high-tf entries
    static void findtf(
        unsigned char* indexbuf1,   // index block
        unsigned buflen,            // block length
        unsigned start,             // requested docid start
        unsigned count,             // requested docid count
        unsigned tfmin,             // lower bound on tf
        PostingList& pL);           // return value


};

inline void PostingList::append(const DocPosting& dp) { uvec.push_back(dp); }
inline unsigned PostingList::size() const { return uvec.size(); }
inline const std::string& PostingList::getTerm() const { return term; }
inline void PostingList::setTerm(const std::string& t) { term = t; }

}    /* namespace mongo */

#endif
