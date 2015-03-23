#include "Master_Worker.h"
#include "mpi.h"
#include <vector>

using namespace std;

struct work_t {
    int x, y;
    work_t(int a, int b) : x(a), y(b) {};
};

struct result_t {
    int y;
    string x = "2324";
    result_t(int a) : y(a) {};
};

class MW : public Master_Worker {
public:
    MW(int wk_sz, int rs_sz) : Master_Worker(wk_sz, rs_sz) {};
    void create() {
        for (int i=0; i<1000; i++) {
            work_t* w = new work_t(1, 1);
            wPool.push_back(w);
        }
    };

    int result(vector<result_t*> &res, result_t* &tmpR) {
        tmpR = new result_t(0);
        try {
            for (int i=0; i<res.size(); i++) {
                tmpR->y += res[i]->y;
            }
        } catch (int e) {
            return 0;
        }
        return 1; 
    }

    result_t* compute(work_t* work) {
        int r = 0;
        r = work->x + work->y;
        result_t *res = new result_t(r);
        return res;
    }

};

int main(int argc, char *argv[]) {

    MPI::Init (argc, argv);

    Master_Worker *mw = new MW(sizeof(work_t), sizeof(result_t));

    mw->Run();

    if (mw->isMaster()) {
        result_t *r = mw->getResult();
        cout << "The result is " << r->y << endl;
    }

    MPI::Finalize ();


    return 0;
}
