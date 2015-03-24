#include "Master_Worker.h"
#include "mpi.h"
#include "gmpxx.h"
#include <vector>
#include <cmath>
#include <string>

using namespace std;

#define NUM_SIZE 32
#define GRAIN 100000

int n_size = 0;

struct work_t {
    char start[NUM_SIZE]; 
    char end[NUM_SIZE];
};

struct result_t {
    char digits[GRAIN][NUM_SIZE];
};

class MW : public Master_Worker {
public:
    MW(unsigned long wk_sz, unsigned long rs_sz, char *n, int m) : Master_Worker(wk_sz, rs_sz, m) {
        this->n = n; 
    };

    void create() {
        mpz_class n, r, work_num;
        n = this->n;
        r = sqrt(n);  
        work_num = r/GRAIN;

        for (mpz_class i=0; i<work_num; i++) { 
            work_t* w = new work_t();
            mpz_class s = i*GRAIN+1;
            mpz_class e = i*GRAIN+GRAIN;
            char *start = mpz_to_char(s);
            char *end = mpz_to_char(s);
            strcpy(w->start, start);
            strcpy(w->end, end);
            wPool.push_back(w);
        }

        if (n%GRAIN > 0) {
            work_t* w = new work_t();
            mpz_class s = n-n%GRAIN+1;
            char *start = mpz_to_char(s);
            char *end = mpz_to_char(n);
            strcpy(w->start, start);
            strcpy(w->end, end);
            wPool.push_back(w);
        }
        cout << wPool.size() << " works created" << endl;
    };

    int result(vector<result_t*> &res, result_t* &tmpR) {
        try {
            tmpR = new result_t();
            unsigned long index=0;
            for (unsigned long i=0; i<res.size(); i++) {
                for (unsigned long j=0; j<GRAIN; j++) {
                    if (res[i]->digits[j][0] == 0)
                        break;
                    strcpy(tmpR->digits[index], res[i]->digits[j]);
                    index++;
                }
            }
        } catch (int e) {
            return 0;
        }
        return 1; 
    }

    result_t* compute(work_t* &work) {
        result_t *res = new result_t();
        unsigned long index = 0;
        mpz_class start, end, n;
        start = work->start;
        end = work->end;
        n = this->n;
        for (unsigned long i=0; i<GRAIN; i++) {
            if (n%(start+i) == 0) {
                mpz_class tmpI(start+i);
                char *charI = mpz_to_char(tmpI);
                strcpy(res->digits[index++], charI); 
            }
        }
        return res;
    }
private:
    char *n;

    char* mpz_to_char(mpz_class &x) {
       string strX = x.get_str();
       char *charX = new char(strX.length()+1);
       strcpy(charX, strX.c_str());
       return charX;
    }
};

int main(int argc, char *argv[]) {

    if (argc < 3)
        cout << "Please provide MODE and N" << endl;
    int mode = atoi(argv[1]);

    MPI::Init (argc, argv);

    Master_Worker *mw = new MW(sizeof(work_t), sizeof(result_t), argv[2], mode);
    

    mw->Run();

    if (mw->isMaster()) {
        result_t *r = mw->getResult();
        cout << "The result:" << endl;
        for (unsigned long i=0; i<GRAIN; i++) {
            if (r->digits[i][0] == 0)
                break;
            cout << r->digits[i] << " ";
        }
        cout << endl;
    }

    MPI::Finalize ();

    return 0;
}
