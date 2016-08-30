#include <iostream>
#include <sstream>

#include "Exception.h"
#include "DocPosting.h"

using namespace std;

namespace mongo {

DocPosting::~DocPosting()
{}

void DocPosting::init(
    unsigned d,
    const vector<unsigned> posv,
    bool takeDeltas)
{
    //store (docid,tf)
    docid = d;
    tf = posv.size();
    score = (float)tf;
    uvec.push_back(docid);
    uvec.push_back(tf);

    //store position list
    unsigned lastPos = 0;
    for (vector<unsigned>::const_iterator p1 = posv.begin(); p1!=posv.end(); p1++) {
    	unsigned pos = *p1;
    	if (takeDeltas) { pos -= lastPos; lastPos = *p1; }
        uvec.push_back(pos);
    }
}

string DocPosting::toString() const
{
    ostringstream oss;
    oss << "docid = " << docid << endl;
    oss << "tf = " << tf << endl;
    unsigned j = 0;
    for (DocPosting::DocPostingIterator it = iterator(); !it.done(); ++it) {
    	oss << "pos[" << j++ << "] = " << (*it).first << "\n";
    }
    return oss.str();
}


string DocPosting::compactString() const
{
    bool init = true;
    ostringstream oss;
    oss << docid << '['<<tf<<"]->";
    DocPosting::DocPostingIterator it = iterator();
    for (; !it.done(); ++it) {
        if (!init) oss << ", ";
        init = false;
    	oss << (*it).first;
    }
    return oss.str();
}

void DocPosting::append_uvec(
    vector<unsigned>& v)
{
    vector<unsigned>::const_iterator it = uvec.begin();
    for (; it!=uvec.end(); ++it) {
    	v.push_back(*it);
    }
}

// basic merge
void DocPosting::merge(
    const DocPosting& a,
    const DocPosting& b,
    DocPosting& result)
{
    if (a.getDocid()!=b.getDocid()) {
    	throw Exception("DOCPOSTING", "merge: unequal docid lists");
    }
    vector<unsigned> posv;

    DocPostingIterator aIt = a.iterator();
    DocPostingIterator bIt = b.iterator();

    while (!aIt.done() && !bIt.done()) {
    	unsigned aa = *aIt;
    	unsigned bb = *bIt;

    	//cout << "aa = " << aa << endl;
    	//cout << "bb = " << bb << endl;

    	if (aa.first <= bb.first) {
    		posv.push_back(aa);
    		++aIt;
    	} else {
    		posv.push_back(bb);
    		++bIt;
    	}
    }
    while (!aIt.done()) {
    	unsigned aa = *aIt;
    	posv.push_back(aa);
    	++aIt;
    }
    while (!bIt.done()) {
    	unsigned bb = *bIt;
    	posv.push_back(bb);
    	++bIt;
    }
    result.init(a.getDocid(), posv, true);
}


// merge with proximity
#define INITIAL_STEP    0
#define A_STEP     			1
#define B_STEP    			2

void DocPosting::proximity_merge(
    const DocPosting& a,
    const DocPosting& b,
    DocPosting& result,
    unsigned& sepresult)
{
    if (a.getDocid()!=b.getDocid()) {
    	throw Exception("DOCPOSTING", "merge: unequal docid lists");
    }

    vector<unsigned> posv;

    DocPostingIterator aIt = a.iterator();
    DocPostingIterator bIt = b.iterator();

    unsigned aa = *aIt;
    unsigned bb = *bIt;

    int lastStep = INITIAL_STEP;
    int sep = (1<<31);// minimum (a,b) term separation

    while (!aIt.done() && !bIt.done()) {
    	if (aa.first <= bb.first) {
    		if (lastStep==B_STEP) {
    			int n = (bb - aa);
    			if (n < sep) sep = n;
    		}
    		lastStep = A_STEP;
    		posv.push_back(aa);
    		++aIt;
    	} else {
    		if (lastStep==A_STEP) {
    			int n = (aa - bb);
    			if (n < sep) sep = n;
    		}
    		lastStep = B_STEP;
    		posv.push_back(bb.first);
    		++bIt;
    	}
    }
    if (!aIt.done() && lastStep==B_STEP) {
    	int n = (bb - aa);
    	if (n < sep) sep = n;
    }
    while (!aIt.done()) {
    	aa = *aIt;
    	posv.push_back(aa);
    	++aIt;
    }
    if (!bIt.done() && lastStep==A_STEP) {
    	int n = (aa - bb);
    	if (n < sep) sep = n;
    }
    while (!bIt.done()) {
    	bb = *bIt;
    	posv.push_back(bb);
    	++bIt;
    }
    result.init(a.getDocid(), posv, true);
    sepresult = sep;
}

}    /* namespace mongo */

