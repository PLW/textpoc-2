//@file PostingList.cpp

#include "hash32map.h"
#include "Scorer.h"
#include "PostingList.h"

#include <iostream>
#include <sstream>
#include <vector>

using namespace std;
namespace mongo {

/*---------------------------------------------------------------------------
              _________
              offset_0 \
             [offset_1]|__skiplist frame
              ...      |
             [offset_a]|
              _ _ _ _ _/
              docid_0  \
              tf       |
              pos      |__DocPosting
              ...      |
              pos      |
              tag      |
              ...      |
              tag      |
              _________/
              _________
              offset_0 \
             [offset_1]|__skiplist frame
              ...      |
             [offset_b]|
              _ _ _ _ _/
              docid_1  \
              tf       |
              pos      |__DocPosting 
              ...      |
              pos      |
              tag      |
              ...      |
              tag      |
             __________/
  
      Item        Size        Description
      ----        ----        -----------
      offset_0    [4]         byte length of the DocPosting block
     [offset_1]   [4]         byte offset to the DocPosting block 4 steps ahead
     [offset_2]   [4]         byte offset to the DocPosting block 16 steps ahead
     [offset_3]   [4]         byte offset to the DocPosting block 256 steps ahead
     [offset_4]   [4]         byte offset to the DocPosting block 65536 steps ahead
    _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
      docid_0     [var]       docid delta from previous block
     [docid_1]    [var]       docid delta from block back 4 steps
     [docid_2]    [var]       docid delta from block back 16 steps
     [docid_3]    [var]       docid delta from block back 256 steps
     [docid_4]    [var]       docid delta from block back 65536 steps
      tf          [var]       position list length = document term frequency
      pos         [var]       term positions
      tag         [var]       term contexts (term field id)
  
      for j>0:
          offset_j \__ occurs <=> (ordinal_position % 2^(2^j) == 0)
          docid_j  /              
  
      The level in the skip list is implicit, based on the ordinal position
      of the DocPosting. The offsets are forward references and have to be poked
      into place, hence fixed width.  The docid deltas are backward refernces and
      can be computed from context.
  ---------------------------------------------------------------------------*/

PostingList::PostingList(
    const std::string& t,
    const vector<unsigned char>* _cvec)
:
    cvec(_cvec),
    term(t)
{
    // unpack
    vector<unsigned char>::const_iterator p = cvec->begin();
    uint32_t offset = 0;
    uint32_t length = 0;

    #ifdef DEBUG
    cout <<__FUNCTION__<<": offset = " << offset << endl;
    cout <<__FUNCTION__<<": length = " << length << endl;
    cout <<__FUNCTION__<<": cvec.size() = " << cvec.size() << endl;
    #endif

    while (p!=cvec->end()) {
        // extract length to next DocPosting
        unsigned char a = *p++; 
        unsigned char b = *p++;
        unsigned char c = *p++;
        unsigned char d = *p++;

        if (a!=0) return;    // sanity check

        length = (a<<24)|(b<<16)|(c<<8)|d;
        offset += 4;

        // extract DocPosting

        #ifdef DEBUG
        cout <<__FUNCTION__<<": offset = " << offset << endl;
        cout <<__FUNCTION__<<": length = " << length << endl;
        #endif

        DocPosting dp(*cvec, offset, length);

        #ifdef DEBUG
        cout << "DocPosting = " << dp.toString() << endl;
        #endif

        if (dp.size() > 0) uvec.push_back(dp);
        offset += length;
        p += length;
    }
}


PostingList::PostingList(
    const std::string& t,
    const vector<unsigned char>* _cvec)
:
    cvec(_cvec),
    term(t)
{
    // unpack
    vector<unsigned char>::const_iterator p = cvec->begin();
    unsigned offset = 0;
    unsigned length = 0;

    #ifdef DEBUG
    cout <<__FUNCTION__<<": offset = " << offset << endl;
    cout <<__FUNCTION__<<": length = " << length << endl;
    cout <<__FUNCTION__<<": cvec.size() = " << cvec.size() << endl;
    #endif

    while (p!=cvec->end()) {
        // extract length to next DocPosting
        unsigned char a = *p++; 
        unsigned char b = *p++;
        unsigned char c = *p++;
        unsigned char d = *p++;

        if (a!=0) return;    // sanity check

        length = (a<<24)|(b<<16)|(c<<8)|d;
        offset += 4;

        // extract DocPosting

        #ifdef DEBUG
        cout <<__FUNCTION__<<": offset = " << offset << endl;
        cout <<__FUNCTION__<<": length = " << length << endl;
        #endif

        DocPosting dp(*cvec, offset, length, logger);

        #ifdef DEBUG
        cout << "DocPosting = " << dp.toString() << endl;
        #endif

        if (dp.size() > 0) uvec.push_back(dp);
        offset += length;
        p += length;
    }
}


PostingList::PostingList(
    const std::string& t,
    unsigned char* indexBlock,
    unsigned len)
:
    term(t)
{
    // unpack
    unsigned char* p = indexBlock;
    unsigned length = 0;

    #ifdef DEBUG
    cout <<__FUNCTION__<<": offset = " << offset << endl;
    cout <<__FUNCTION__<<": length = " << length << endl;
    cout <<__FUNCTION__<<": len = " << len << endl;
    #endif

    unsigned char* end = indexBlock+len;
    while (p!=end) {
        // extract length to next DocPosting
        unsigned char a = *p++; 
        unsigned char b = *p++;
        unsigned char c = *p++;
        unsigned char d = *p++;
        length = (a<<24)|(b<<16)|(c<<8)|d;

        // extract DocPosting

        #ifdef DEBUG
        cout <<__FUNCTION__<<": offset = " << offset << endl;
        cout <<__FUNCTION__<<": length = " << length << endl;
        #endif

        DocPosting dp(p, 0, length);

        #ifdef DEBUG
        cout << "DocPosting = " << dp.toString() << endl;
        #endif

        if (dp.size() > 0) uvec.push_back(dp);
        p      += length;
    }
}

#define HIGHBIT ((unsigned char)0x80)
#define LOWMASK ((unsigned char)0x7f)

PostingList::PostingList(
    const std::string& t,
    unsigned char* indexBlock,
    unsigned len,
    bool b)
:
    term(t)
{
    // unpack
    unsigned char* p = indexBlock;
    unsigned length = 0;

    #ifdef DEBUG
    cout <<__FUNCTION__<<": offset = " << offset << endl;
    cout <<__FUNCTION__<<": length = " << length << endl;
    cout <<__FUNCTION__<<": len = " << len << endl;
    #endif

    unsigned char* end = indexBlock+len;
    while (p<end)
    {
        // extract length to next DocPosting
        unsigned char a = *p++; 
        unsigned char b = *p++;
        unsigned char c = *p++;
        unsigned char d = *p++;
        length = (a<<24)|(b<<16)|(c<<8)|d;
        
        // unpack length to next DocPosting
        if (length>1024) break;

        // extract DocPosting

        #ifdef DEBUG
        cout <<__FUNCTION__<<": offset = " << offset << endl;
        cout <<__FUNCTION__<<": length = " << length << endl;
        #endif

        DocPosting dp(p, 0, length);

        #ifdef DEBUG
        cout << "DocPosting = " << dp.toString() << endl;
        #endif

        if (dp.size() > 0) uvec.push_back(dp);
        p += length;
    }
}


PostingList::PostingList(
    const std::string& t,
    unsigned char* indexBlock,
    unsigned len,
    int b)
:
    term(t)
{
    // unpack
    unsigned char* p = indexBlock;
    unsigned length = 0;

    #ifdef DEBUG
    cout <<__FUNCTION__<<": offset = " << offset << endl;
    cout <<__FUNCTION__<<": length = " << length << endl;
    cout <<__FUNCTION__<<": len = " << len << endl;
    #endif

    unsigned char* end = indexBlock+len;
    hash32map<char> docidmap;
    while (p<end)
    {
        // extract length to next DocPosting
        unsigned char a = *p++; 
        unsigned char b = *p++;
        unsigned char c = *p++;
        unsigned char d = *p++;
        length = (a<<24)|(b<<16)|(c<<8)|d;
        
        // unpack length to next DocPosting
        if (length>1024) break;

        // extract DocPosting

        #ifdef DEBUG
        cout <<__FUNCTION__<<": offset = " << offset << endl;
        cout <<__FUNCTION__<<": length = " << length << endl;
        #endif

        DocPosting dp(p, 0, length);

        unsigned docid = dp.getDocid();
        unsigned idx;
        if (!docidmap.find(docid,idx) && dp.size()>0) {
            docidmap.put(docid,'1');
            uvec.push_back(dp);
        }

        #ifdef DEBUG
        cout << "DocPosting = " << dp.toString() << endl;
        #endif

        p += length;
    }
}


PostingList::PostingList(
    const std::string& t,
    const vector<DocPosting>& dPv)
:
    uvec(dPv),
    term(t)
{
}


PostingList::PostingList(
    const std::string& t)
: term(t)
{
}


PostingList::PostingList(
    const PostingList& pList,
    const docid_set& dset)
{
    PostingListIterator it = pList.begin();
    for (; it!=pList.end(); ++it) {
        unsigned docid = it->getDocid();
        if (dset.contains(docid)) append(*it);
    }
}


// append a PostingList to this one
void PostingList::append(
    const PostingList& pList)
{
    PostingListIterator it = pList.begin();
    for (; it!=pList.end(); ++it) {
        append(*it);
    }
}


// Merge two PostingLists for the same term
void PostingList::merge(
    const PostingList& pL1,
    const PostingList& pL2,
    PostingList& result)
    //const HashSet<unsigned> deleteSet)    // tombstones
{
    PostingListIterator it1 = pL1.begin();
    PostingListIterator e1  = pL1.end();
    PostingListIterator it2 = pL2.begin();
    PostingListIterator e2  = pL2.end();
    unsigned docid1, docid2;
    DocPosting p1 = *it1;
    DocPosting p2 = *it2;

    int c = 0;
    while (it1!=e1 || it2!=e2) {
        if (it1!=e1 && it2!=e2) {
            docid1 = p1.getDocid();
            docid2 = p2.getDocid();
            c = docid1 - docid2;
        }
         else if (it1==e1) {
            docid2 = p2.getDocid();
            c = +1;
        }
        else { // if (it2==e2)
            docid1 = p1.getDocid();
            c = -1;
        }

        if (c < 0) {
            //if (!deleteSet.contains( docid1 )) result.append( p1 );
            result.append( p1 );
            ++it1;
            if (it1!=e1) p1 = *it1;
        }
        else if (c > 0) {
            //if (!deleteSet.contains( docid2 )) result.append( p2 );
            result.append( p2 );
            ++it2;
            if (it2!=e2) p2 = *it2;
        }
        else {
            //ostringstream oss;
            //oss << "duplicate docid " << docid1 << " in merge";
            //throw Exception("POSTINGLIST", oss.str());
            DocPosting p3;
            DocPosting::merge(p1,p2,p3);
            result.append( p3 );
            ++it1; if (it1!=e1) p1 = *it1;
            ++it2; if (it2!=e2) p2 = *it2;
        }
    }
}


// merge phrase PostingLists
void PostingList::_phrase(
    const vector<PostingList*> &pLv,
    PostingList& pList)
{
    std::vector<float> wv;  // default weight vector
    for (unsigned i=0; i<pLv.size(); ++i) wv.push_back(1.0f);
    PostingList::_phrase2(pLv, wv, pList);
}


// merge phrase PostingLists
void PostingList::_phrase2(
    const vector<PostingList*> &pLv,    // per term
    const vector<float>& wv,            // field weights
    PostingList& pList)
{
    if (!pLv.size()) return;
  
    // get the begining and end iterator for each PostingList
    // also form the term ("term1 term2...")

    vector<PostingListIterator> itv;
    vector<PostingListIterator> iendv;
    ostringstream oss;
    oss << "(\"";
    unsigned i;
    vector<unsigned> posidxv;
    unsigned posidx = 0;
    bool bNextStop = true;
    for (i=0; i<pLv.size(); ++i) {
        itv.push_back(pLv[i]->begin());
        iendv.push_back(pLv[i]->end());

        string term = pLv[i]->getTerm();
        oss << term;
        if (i != pLv.size() - 1) oss << " ";

        if (bNextStop) {
            bNextStop = false; 
            posidxv.push_back(posidx); 
            continue; 
        }
        if (i && !bNextStop) {
            posidxv.push_back(++posidx);
            if (term.find('#') != string::npos) bNextStop = true;
        }
    }
    oss << "\")";
    pList.setTerm(oss.str());

    // look for common docid in PostingLists for all terms

    // target docid
    unsigned docid = (*itv[0]).getDocid();  

    bool terminate = false;
    while (true) {     
        bool match = true;     
        for (i=0; i<itv.size(); ++i) {       
            // advance docid if it's less than the target docid
            while (itv[i]!=iendv[i] && (*itv[i]).getDocid() < docid) ++itv[i];

            // termination condition
            if (itv[i]==iendv[i]) { terminate = true; break; }

            // if the next docid is greater than target, it's the next target docid
            if ((*itv[i]).getDocid() > docid) {
                docid = (*itv[i]).getDocid();
                match = false;
                break;
            } // else match
        }
    
        if (terminate) break;

        if (match) { // found common docid in PostingList for all terms
            // construct the DocPosting
            vector<unsigned> posv;
            vector<unsigned> tagv;
            bool addNewDocs = false;

            // iterators of DocPostings for other terms except the first
            vector<DocPosting::DocPostingIterator> idpv;
            for (i=0; i<itv.size(); ++i)
                idpv.push_back((*itv[i]).iterator()); 
      
            bool done = false;
            unsigned pos = (*idpv[0]).first;

            while (true) {
                // loop through DocPostings
                for (i=0; i<idpv.size(); ++i) {

                    // advance DocPosting that is less than the first term's pos + i
                    while (!(idpv[i]).done() && (*idpv[i]).first < pos + posidxv[i]) ++idpv[i];

                    // done with DocPosting condition
                    if ((idpv[i]).done()) {
                        done = true;
                        match = false; 
                        break;
                    }
    
                    if ((*idpv[i]).first != pos+posidxv[i]) { 
                        pos = (*idpv[i]).first-posidxv[i]; match = false; break;
                    }
                } 

                // Found consecutive postions for the phrase
                if (match) {
                    // put DocPostings for all terms
                    for (i=0; i<idpv.size(); ++i) {
                        posv.push_back((*idpv[i]).first);
                        tagv.push_back((*idpv[i]).second);
                        addNewDocs = true;
                    }

                    if (!(idpv[0].done())) {
                        ++idpv[0];
                        pos = (*idpv[0]).first;
                    }
                    else
                        done = true;

                }

                if (done) break;
                match = true;
            } // end: inner while

            if (addNewDocs) {
                DocPosting p;
                p.init(docid, posv, tagv, true);
                vector<DocPosting*> dpv;
                for (i=0; i<itv.size(); ++i)
                    dpv.push_back(const_cast<DocPosting*>(&(*itv[i])));
                p.setScore(Scorer::_phrase(dpv,wv));
                pList.append(p);
            }

            if (done) { 
                // not found in this docid, advance docid to the next one
                if (itv[0] != iendv[0]) {
                    ++itv[0];
                    docid = (*itv[0]).getDocid();
                }
                else
                    terminate = true;
            }
        }   // end: if (match)
    }  // end: outer while 
}


// merge two PostingLists for AndNot condition
void PostingList::_andnot(
    const PostingList &pL1,
    const PostingList &pL2,
    PostingList& pList)
{
    if (pL1.size()==0 || pL2.size()==0) return;

    PostingListIterator it1 = pL1.begin();
    PostingListIterator e1  = pL1.end();
    PostingListIterator it2 = pL2.begin();
    PostingListIterator e2  = pL2.end();
  
    if (it1==e1 || it2==e2) return;
    
    // for the term (&- term1 term2)
    ostringstream oss;
    oss << "(&- " << pL1.getTerm() << " " << pL2.getTerm() << ")";
    pList.setTerm(oss.str());

    unsigned docid1, docid2;
    while (it1!=e1 && it2!=e2) {
        docid1 = (*it1).getDocid();
        docid2 = (*it2).getDocid();
        if (docid1 < docid2) { 
            pList.append(*it1); 
            ++it1; 
        }
        else if (docid1 > docid2) {
            ++it2;
        }
        else {
            ++it1;
            ++it2;
        }
    }
}


// merge two PostingLists in Near cndition
void PostingList::_near(
    PostingList &pL1,
    PostingList &pL2,
    int radius,
    PostingList &pList)
{
    _near2(pL1, pL2, radius, 1.0f, 1.0f, pList);
}


// merge two PostingLists in Near cndition
void PostingList::_near2(
    PostingList &pL1,
    PostingList &pL2,
    int radius;
    float w1;
    float w2,
    PostingList &pList)
{
    if (pL1.size()==0 || pL2.size()==0) return;

    // form the term (term1 w/n term2)
    ostringstream oss;
    oss << "(" << pL1.getTerm() << "  w/" << radius << " " << pL2.getTerm() << ")";
    pList.setTerm(oss.str());

    vector<PostingListIterator> itv;
    vector<PostingListIterator> iendv;

    itv.push_back(pL1.begin());
    itv.push_back(pL2.begin());
    iendv.push_back(pL1.end());
    iendv.push_back(pL2.end());

    unsigned i, docid = (*itv[0]).getDocid();  //target docid 

    bool terminate = false;
    while (true) {     
        bool match = true;     
        for (i=0; i<itv.size(); ++i) {       
            // advance docid if it's less than the target docid
            while (itv[i]!=iendv[i] && (*itv[i]).getDocid() < docid) ++itv[i];

            // termination condition
            if (itv[i]==iendv[i]) { terminate = true; break; }

            // if the next docid is greater than the target, it's the next target docid
            if ((*itv[i]).getDocid() > docid) {
                docid = (*itv[i]).getDocid();
                match = false;
                break;
            }
        }
        
        if (terminate) break;

        if (match) { // found common docid in PostingList for all terms
            // construct the DocPosting
            vector<unsigned> posv;
            vector<unsigned> tagv;
            bool addNewDocs = false;

            vector<DocPosting::DocPostingIterator> idpv;
            for (i=0; i<itv.size(); ++i) idpv.push_back((*itv[i]).iterator());

            bool done = false;
            int posidx = 0;
            unsigned pos = (*idpv[posidx]).first;
            while (true) {

                // loop through DocPostings of other terms
                for (i=0; i<idpv.size(); ++i) {

                    // advance DocPosting that is less than the first term's pos + i
                    while (!(idpv[i].done()) && (((*idpv[i]).first+radius) < pos)) ++idpv[i];

                    // done with DocPosting condition
                    if (idpv[i].done()) {
                        done = true;
                        if (((*idpv[i]).first+radius) < pos || ((*idpv[i]).first > (pos+radius))) {
                        match = false;
                        break; }
                    }

                    if (((*idpv[i]).first) > (pos+radius)) {
                        posidx = i;
                        pos = (*idpv[posidx]).first;
                        match = false;
                        break;
                    }
                }

                if (match) {
                    posidx = posidx? 0:1;
                    while ((*idpv[posidx]).first <= pos+radius) {
                        posv.push_back((*idpv[0]).first);
                        posv.push_back((*idpv[1]).first);
                        tagv.push_back((*idpv[0]).second);
                        tagv.push_back((*idpv[1]).second);
                        addNewDocs = true;

                        if (!(idpv[posidx].done()))
                            ++idpv[posidx];
                        else {
                            done = true;
                            break;
                        }
                    }

                    if (!done)
                        pos = (*idpv[posidx]).first;
                }

                if (done) break; 
                match = true;

            }   // end: while (true)

            if (addNewDocs) {
                DocPosting p;
                p.init(docid, posv, tagv, true);
                p.setScore(Scorer::_and2(*itv[0],*itv[1],w1,w2));
                pList.append(p);          
            }

            if (done) { 
                if (itv[0]!=iendv[0]) { ++itv[0]; docid = (*itv[0]).getDocid(); }
                else terminate = true; 
            }
        }
    }
}


void PostingList::_tag(
  PostingList &pL,
  int type,
  PostingList &pList)
{
  _tag2(pL, type, 1.0f, pList);
}


void PostingList::_tag2(
  PostingList &pL,
  int type, float w,
  PostingList &pList)
{
    if (pL.size()==0) return;

  // for the term (tag:token)
  ostringstream oss;
  oss << "(";
  if (type == 1) // title_tag
    oss << "intitle:";
  else if (type == 2) // url_tag
    oss << "inurl:";
  else if (type == 0) // site_tag
    oss << "site:";
  oss << pL.getTerm() << ")";
  pList.setTerm(oss.str());

  // filter out result based on tag
  PostingList::PostingListIterator ipL = pL.begin(); 
  for(; ipL != pL.end(); ++ipL)
  {
    unsigned docid = (*ipL).getDocid();
    vector<unsigned> posv;
    vector<unsigned> tagv;
    bool match = false;

    DocPosting::DocPostingIterator idp = (*ipL).iterator();
    for (; !idp.done(); ++idp)
    {
      if ((type == 1 /*title_tag*/ && (*idp).second == TagScanner::h_title) ||
          (type == 2 /*url_tag*/   && (*idp).second == TagScanner::h_url) ||
          (type == 0 /*site_tag*/   && (*idp).second == TagScanner::h_url)) 
      {
        posv.push_back((*idp).first);
        tagv.push_back((*idp).second);
        match = true;
      }
    }

    if (match)
    {
      DocPosting p;
      p.init(docid, posv, tagv, true);
            p.setScore(Scorer::_accrue1(p,w));
      pList.append(p);
    }
  }
}


// intersect two PostingLists with weights
void PostingList::_and2(
    const PostingList& pL1,
    const PostingList& pL2,
    float w1, float w2,
    PostingList& result)
{
    if (pL1.size()==0 || pL2.size()==0) return;
    if (pL1.getTerm()=="__ID") { result = pL2; return; }
    if (pL2.getTerm()=="__ID") { result = pL1; return; }

    // form the term "(and term1 term2)"
    ostringstream oss;
    oss << "(& " << pL1.getTerm() << " " << pL2.getTerm() << ")";
    result.setTerm(oss.str());

    PostingListIterator it1 = pL1.begin();
    PostingListIterator e1  = pL1.end();
    PostingListIterator it2 = pL2.begin();
    PostingListIterator e2  = pL2.end();

    if (it1==e1 || it2==e2) return;

    DocPosting  p1 = *it1;
    DocPosting  p2 = *it2;
    unsigned docid1 = p1.getDocid();
    unsigned docid2 = p2.getDocid();

    while (it1!=e1 && it2!=e2) {
        //cout << "_and: (docid1,docid2) = (" << docid1 << "," << docid2 << ")\n";
        if (docid1 < docid2) {
            ++it1;
            if (it1!=e1) {
                p1 = *it1;    // XXX skiplist here
                docid1 = p1.getDocid();
            }
        }
        else if (docid1 > docid2) {
            ++it2;
            if (it2!=e2){
                p2 = *it2;    // XXX skiplist here
                docid2 = p2.getDocid();
            }
        }
        else {    // docid1==docid2
            DocPosting p;
            DocPosting::merge( p1, p2, p );
            p.setScore(Scorer::_and2(p1,p2,w1,w2));
            result.append( p );
            ++it1;
            ++it2;
            if (it1!=e1) {
                p1 = *it1;
                docid1 = p1.getDocid();
            }
            if (it2!=e2) {
                p2 = *it2;
                docid2 = p2.getDocid();
            }
        }
    }
}


// intersect two PostingLists with default weights
void PostingList::_and(
    const PostingList& pL1,
    const PostingList& pL2,
    PostingList& result)
{
    _and2(pL1, pL2, 1.0f, 1.0f, result);
}


// Union two PostingLists
void PostingList::_or(
    const PostingList& pL1,
    const PostingList& pL2,
    PostingList& result)
{
    if (pL1.size()==0) { result = pL2; return; }
    if (pL2.size()==0) { result = pL1; return; }

    // form the term "(or term1 term2)"
    ostringstream oss;
    oss << "(| " << pL1.getTerm() << " " << pL2.getTerm() << ")";
    result.setTerm(oss.str());

    PostingListIterator it1 = pL1.begin();
    PostingListIterator e1  = pL1.end();
    PostingListIterator it2 = pL2.begin();
    PostingListIterator e2  = pL2.end();

    DocPosting  p1, p2;
    unsigned docid1, docid2;

    if (it1!=e1) { p1 = *it1; }
    if (it2!=e2) { p2 = *it2; }

    int c;
    while (it1!=e1 || it2!=e2)
    {
        if (it1!=e1 && it2!=e2) {
            docid1 = p1.getDocid();
            docid2 = p2.getDocid();
            c = docid1 - docid2;
        }
        else if (it1==e1) {
            docid2 = p2.getDocid();
            c = +1;
        }
        else { //(it2==e2) 
            docid1 = p1.getDocid();
            c = -1;
        }

        if (c < 0) {
            result.append( p1 );
            ++it1;
            if (it1!=e1) p1 = *it1;
        }
        else if (c > 0) {
            result.append( p2 );
            ++it2;
            if (it2!=e2) p2 = *it2;
        }
        else {
            DocPosting p(docid1);
            DocPosting::merge( p1, p2, p );
            result.append( p );
            ++it1; ++it2;
            if (it1!=e1) p1 = *it1;
            if (it2!=e2) p2 = *it2;
        }
    }
}


// Accrue two PostingLists with weights
void PostingList::_accrue2(
    const PostingList& pL1,
    const PostingList& pL2,
    float w1, float w2,
    PostingList& result)
{
    string term1 = pL1.getTerm();
    string term2 = pL2.getTerm();
    unsigned loc1 = term1.find("vert:", 0);
    unsigned loc2 = term2.find("vert:", 0);

    if (loc1==0 || loc2==0) {
        _and2(pL1, pL2, w1, w2, result);
        return;
    }
    
    if (pL1.size()==0) { result = pL2; return; }
    if (pL2.size()==0) { result = pL1; return; }

    // form the term "(@ term1 term2)"
    ostringstream oss;
    oss << "(@ " << term1 << " " << term2 << ")";
    result.setTerm(oss.str());

    PostingListIterator it1 = pL1.begin();
    PostingListIterator e1  = pL1.end();
    PostingListIterator it2 = pL2.begin();
    PostingListIterator e2  = pL2.end();

    DocPosting  p1, p2;
    unsigned docid1, docid2;

    if (it1!=e1) { p1 = *it1; }
    if (it2!=e2) { p2 = *it2; }

    int c;
    while (it1!=e1 || it2!=e2)
    {
        if (it1!=e1 && it2!=e2) {
            docid1 = p1.getDocid();
            docid2 = p2.getDocid();
            c = docid1 - docid2;
        }
        else if (it1==e1) {
            docid2 = p2.getDocid();
            c = +1;
        }
        else { //(it2==e2) 
            docid1 = p1.getDocid();
            c = -1;
        }

        if (c < 0) {
            p1.setScore(Scorer::_accrue1(p1,w1));
            result.append( p1 );
            ++it1;
            if (it1!=e1) p1 = *it1;
        }
        else if (c > 0) {
            p2.setScore(Scorer::_accrue1(p2,w2));
            result.append( p2 );
            ++it2;
            if (it2!=e2) p2 = *it2;
        }
        else {
            DocPosting p(docid1);
            DocPosting::merge( p1, p2, p );
            p.setScore(Scorer::_accrue2(p1,p2,w1,w2));
            result.append( p );
            ++it1; ++it2;
            if (it1!=e1) p1 = *it1;
            if (it2!=e2) p2 = *it2;
        }
    }
}


// Accrue three PostingLists with weights
/*
void PostingList::_accrue3(
    const PostingList& pL1,
    const PostingList& pL2,
    const PostingList& pL3,
    float w1, float w2, float w3,
    PostingList& result)
{
    if (pL1.size()==0) { result = pL2; return; }
    if (pL2.size()==0) { result = pL1; return; }

    // form the term "(accrue term1 term2)"
    ostringstream oss;
    oss << "(@ " << pL1.getTerm() << " " << pL2.getTerm() << ")";
    result.setTerm(oss.str());

    PostingListIterator it1 = pL1.begin();
    PostingListIterator e1  = pL1.end();
    PostingListIterator it2 = pL2.begin();
    PostingListIterator e2  = pL2.end();

    DocPosting  p1, p2;
    unsigned docid1, docid2;

    if (it1!=e1) { p1 = *it1; }
    if (it2!=e2) { p2 = *it2; }

    int c;
    while (it1!=e1 || it2!=e2)
    {
        if (it1!=e1 && it2!=e2) {
            docid1 = p1.getDocid();
            docid2 = p2.getDocid();
            c = docid1 - docid2;
        }
        else if (it1==e1) {
            docid2 = p2.getDocid();
            c = +1;
        }
        else { //(it2==e2) 
            docid1 = p1.getDocid();
            c = -1;
        }

        if (c < 0) {
            p1.setScore(Scorer::_accrue1(p1,w1));
            result.append( p1 );
            ++it1;
            if (it1!=e1) p1 = *it1;
        }
        else if (c > 0) {
            p2.setScore(Scorer::_accrue1(p2,w2));
            result.append( p2 );
            ++it2;
            if (it2!=e2) p2 = *it2;
        }
        else {
            DocPosting p(docid1);
            DocPosting::merge( p1, p2, p );
            p.setScore(Scorer::_accrue2(p1,p2,w1,w2));
            result.append( p );
            ++it1; ++it2;
            if (it1!=e1) p1 = *it1;
            if (it2!=e2) p2 = *it2;
        }
    }
}
*/

// Accrue two PostingLists with default weights
void PostingList::_accrue(
    const PostingList& pL1,
    const PostingList& pL2,
    PostingList& result)
{
    _accrue2(pL1, pL2, 1.0f, 1.0f, result);
}


string PostingList::toString() const
{
    ostringstream oss;
    for (PostingListIterator it = begin(); it!=end(); ++it) {
      DocPosting docP =*it;
      oss << docP.toString() << endl;
    }
    return oss.str();
}


// Low-level access functions
void PostingList::findtf(
    unsigned char* indexbuf,
    unsigned buflen,
    unsigned start,
    unsigned count,
    unsigned tfmin,
    PostingList& pL)
{
    unsigned char* p = indexbuf;
    unsigned char* q = &indexbuf[buflen];
    unsigned len = 0;
    unsigned docid = 0;
    unsigned tf = 0;
    unsigned k = 0;

    while (p<q && k<start) {
        len = docid = tf = 0;

        // unpack length
        unsigned a = *p++;
        unsigned b = *p++;
        unsigned c = *p++;
        unsigned d = *p++;
        len = (a<<24)+(b<<16)+(c<<8)+d;
        unsigned char* p0 = p;
    
        // unpack docid
        for (; (*p0)&HIGHBIT ; ++p0) { docid <<= 7; docid += (*p0)&LOWMASK;    }
        docid <<= 7; docid += (*p0++);
    
        // unpack tf
        for (; (*p0)&HIGHBIT ; ++p0) { tf <<= 7; tf += (*p0)&LOWMASK;    }
        tf <<= 7; tf += (*p0++);

        #ifdef DEBUG
        cout << "len.="<<len;
        cout << ", docid.="<<docid;
        cout << ", tf.="<<tf<<endl;
        #endif
    
        if (tf>=tfmin) k++;
        p += len;
    
        // step past pos,pos,..
        //for (unsigned j=0; j<tf; ++j) { while ((*p0)&HIGHBIT) ++p0; ++p0; }
        // step past tag,tag,..
        //for (unsigned j=0; j<tf; ++j) { while ((*p0)&HIGHBIT) ++p0; ++p0; }
    }

    k = 0;
    while (p<q && k<count) {
        len = docid = tf = 0;

        // unpack length
        unsigned a = *p++;
        unsigned b = *p++;
        unsigned c = *p++;
        unsigned d = *p++;
        len = (a<<24)+(b<<16)+(c<<8)+d;
        unsigned char* p0 = p;
    
        // unpack docid
        for (; (*p0)&HIGHBIT ; ++p0) { docid <<= 7; docid += (*p0)&LOWMASK;    }
        docid <<= 7; docid += (*p0++);
    
        // unpack tf
        for (; (*p0)&HIGHBIT ; ++p0) { tf <<= 7; tf += (*p0)&LOWMASK;    }
        tf <<= 7; tf += (*p0++);
    
        #ifdef DEBUG
        cout << "len="<<len;
        cout << ", docid="<<docid;
        cout << ", tf="<<tf<<endl;
        #endif

        if (tf>=tfmin) {
            pL.append(DocPosting((unsigned char*)p,0,len));
            ++k;
        }
        p += len;
        
        // step past pos,pos,..
        //for (unsigned j=0; j<tf; ++j) { while ((*p0)&HIGHBIT) ++p0; ++p0; }
        // step past tag,tag,..
        //for (unsigned j=0; j<tf; ++j) { while ((*p0)&HIGHBIT) ++p0; ++p0; }
    }
}


}   // namespace mongo


