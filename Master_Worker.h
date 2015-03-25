#ifndef ____Master_Worker__
#define ____Master_Worker__

#include <vector>
#include <mpi.h>
#include <time.h>
#include <queue>

using namespace std;

struct work_t;
struct result_t;

class Master_Worker{
public:
    Master_Worker();
    Master_Worker(int wk_sz, int rs_sz, int m=0);
    
    //Run Function, Master Worker implementation
    void Run();
    
    result_t* getResult();
    void setWorkSize(int sz);
    void setResultSize(int sz);
    bool isMaster();

protected:
    vector<work_t*> wPool;
    result_t* finalR;
    
    //construct pList and wPool based on the input value
    virtual void create() = 0;
    
    //master process the results after recieving from workers
    virtual int result(vector<result_t*> &res, result_t* &tmpR) = 0;
    
    //distribute the works, and compute each portion of the list based on the rank number
    virtual result_t* compute(work_t* &work) = 0;

private:
    int rank, sz;
    int work_sz, result_sz;
    int mode;
    int MASTER_RANK;
    int BACKUP_MASTER;

    MPI::Status status;
    MPI::Request sendRq, recvRq;

    //work queue for sending
    queue<int> wQue;
    //vector act as map between workInd and time
    time_t *timeList;
    //vector act as map between worker and computing work
    int *workMap;
    //see which worker failed
    int *vWorker;

    void directMode();
    void assignMode();
    void Init();
    void MF_Send();
};

#endif
