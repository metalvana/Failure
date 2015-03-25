#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <string>
#include <iostream>
#include <mpi.h>
#include <ctime>
#include <cstdio>
#include "Master_Worker.h"
#include <queue>
#include <thread>
#include <chrono>


using namespace std;

#define PVAL 0.9

//implementation of MPI_Run

Master_Worker::Master_Worker() {
    MASTER_RANK = 0;
    BACKUP_MASTER = 1;
}
Master_Worker::Master_Worker(int wk_sz, int rs_sz, int m) : work_sz(wk_sz), result_sz(rs_sz), mode(m) {
    MASTER_RANK = 0;
    BACKUP_MASTER = 1;
}

//declaration of other function
void F_Send(void *buf, int count, MPI_Datatype datatype, int dest, int tag, int rank);
bool random_fail(int rank);

void Master_Worker::Run(){
    //have the rank, have the size, devide into 0 and 1
    sz = MPI::COMM_WORLD.Get_size();
    if (sz < 3) {
        cout << "Please assign at least two processors." << endl;
        exit(0);
    }
    rank = MPI::COMM_WORLD.Get_rank();
    cout << "Processor: " << rank << " starts working." << endl;


    //choose the mode, 1 is assign mdoe, 0 is direct mode
    if (mode == 1) {
        assignMode();
    } else {
        directMode();
    }
}

void Master_Worker::directMode() {
    int tag=1;
    vector<result_t*> rList;
    if(rank == MASTER_RANK){
        for(int i = 2; i < sz; i++){
            result_t* tmpR = (result_t*) malloc(result_sz);
            MPI::COMM_WORLD.Recv(tmpR, result_sz, MPI::BYTE, i, tag);
            rList.push_back(tmpR);
        }
        //get the final result
        result(rList, finalR);
    }
    else{
        create();
        int n = wPool.size();
        int wNum = sz-1;
        int tmpSize = n/wNum;
        int tmpRem = n%wNum;
        result_t* tmpR;
        for(int i = (rank-1)*tmpSize; i < rank*tmpSize; i++){
            tmpR = compute(wPool[i]);
            rList.push_back(tmpR);
        }
        if(rank <= tmpRem){
            tmpR = compute(wPool[n-rank]);
            rList.push_back(tmpR);
        }
        result(rList, finalR);
        MPI::COMM_WORLD.Send(finalR, result_sz, MPI::BYTE, 0, tag);
    }
    
}

void Master_Worker::Init() {
    //push everything into wQue
    for(int i = 0; i < wPool.size(); i++){
        wQue.push(i);
    }
    time_t curT;
    time(&curT);
    time_t tmpT[sz];
    int tmpM[sz];
    int tmpV[sz];
    timeList = new time_t[sz];
    workMap = new int[sz];
    vWorker = new int[sz];
    for (int i=0; i<sz; i++) {
        timeList[i] = curT;
        workMap[i] = -1;
        vWorker[i] = 1;
    }
}

void Master_Worker::assignMode() {
    vector<result_t*> rList;
    result_t* tmpR;
    work_t* tmpW;
    
    if (rank == MASTER_RANK) {
        if (MASTER_RANK != BACKUP_MASTER) {
            create();
            Init();
        }
        int n = wQue.size();
        int wNum = sz-2;
        //send sequentially to all workers, update send time
        for(int ind = 0; ind < wNum && ind < n; ind++){
            if (vWorker[ind+2]) {
                int exit = 0;
                MPI::COMM_WORLD.Send(&exit, 1, MPI::INT, ind+2, 0);
                tmpW = wPool[wQue.front()];
                MPI::COMM_WORLD.Send(tmpW, work_sz, MPI::BYTE, ind+2, 1);
                //update map
                workMap[ind+2] = wQue.front();
                wQue.pop();
                //update the time Table
                time_t tmpT; time(&tmpT);
                timeList[ind+2] = tmpT;
            }
        }
        //start to wait for responce,
        //****everytime receiving, check out failure****//
        int cnt = 0;
        time_t checkP;
        time(&checkP);
        int recvCnt = 0;
        while(recvCnt < n){
            /******* check timeList, see if any worker died *********/
            time_t curT; time(&curT);
            result_t* newR = (result_t*) malloc(result_sz);
            //see if received, if not->wait
            recvRq = MPI::COMM_WORLD.Irecv(newR, result_sz, MPI::BYTE, MPI::ANY_SOURCE, 1);
            //check request, if not received, wait
            bool suc = false;
            time_t startT; time(&startT);
            curT = startT;
            while(curT-startT < 2){
                suc = recvRq.Test(status);
                if(suc) break;
                time(&curT);
            }
            if(!suc){
                recvRq.Cancel();
                //assume all the worker within computation dead
                for(int i = 2; i < sz; i++){
                    //if(vWorker[i]) wCnt++;
                    if(workMap[i] != -1 && vWorker[i]){
                        //this worker dead
                        cout << "this worker dead: " << i << " with work number: " << workMap[i] << endl;
                        vWorker[i] = 0;
                        //send the work back to workQue
                        wQue.push(workMap[i]);
                        workMap[i] = -1;
                    }
                }
            }
            else{
                rList.push_back(newR);
                recvCnt++;
                int tmpTar = status.Get_source();
                //update workMap to -1, name it as idle
                workMap[tmpTar] = -1;
            }
            //see if any work left, if yes, push to the return processor or next available processor
            if(!wQue.empty()){
                tmpW = wPool[wQue.front()];
                int tmpTar = -1;
                if(suc) tmpTar = status.Get_source();
                else{//go through vWorker, find first available
                    for(int i = 2; i < sz; i++){
                        if(vWorker[i]){
                            tmpTar = i; break;
                        }
                    }
                }
                if(tmpTar == -1){
                    //no processor available, all workers dead, shout out and exit
                    cout << "all workers died!! No one available!!! " << endl;
                    MPI_Finalize();
                    exit(0);
                }
                int exit = 0;
                MPI::COMM_WORLD.Send(&exit, 1, MPI::INT, tmpTar, 0);
                MPI::COMM_WORLD.Send(tmpW, work_sz, MPI::BYTE, tmpTar, 1);
                time_t tmpT; time(&tmpT);
                timeList[tmpTar] = tmpT;
                workMap[tmpTar] = wQue.front();
                wQue.pop();
            }
            /************* send backup to backup master *******************/
            if (MASTER_RANK != BACKUP_MASTER)
                MF_Send();
        }
        //send msgs to stop workers
        for(int i = 1; i < sz; i++){
            //if(vWorker[i]){
                int exit = 1;
                MPI::COMM_WORLD.Send(&exit, 1, MPI::INT, i, 0);
            //}
        }
        result(rList, finalR);
        cout << "Processor " << rank << " completed." << endl;
    }
    else if (rank == BACKUP_MASTER) {
        create();
        Init();
        time_t check_point, cur_time;
        int que[sz];
        int isExit = 0;
        bool masterDied = false;
        recvRq = MPI::COMM_WORLD.Irecv(&isExit, 1, MPI::INT, MASTER_RANK, 0);
        while (1) {
            MPI::Request r1, r2, r3, r4;
            r1 = MPI::COMM_WORLD.Irecv(timeList, sz, MPI::INT, MASTER_RANK, 2);
            r2 = MPI::COMM_WORLD.Irecv(workMap, sz, MPI::INT, MASTER_RANK, 2);
            r3 = MPI::COMM_WORLD.Irecv(vWorker, sz, MPI::INT, MASTER_RANK, 2);
            r4 = MPI::COMM_WORLD.Irecv(que, sz, MPI::INT, MASTER_RANK, 2);
            time(&check_point);
            time(&cur_time);
            while (cur_time - check_point < 3) {
                masterDied = !r1.Test(status) || !r2.Test(status) || !r3.Test(status) || !r4.Test(status);
                if (!masterDied) break;
                time(&cur_time);
            }
            if (masterDied && wQue.size() != 0) {
                cout << "Master died. Backup Master starts working..." << endl;
                MASTER_RANK = BACKUP_MASTER;
                assignMode();
                break;
            } else {
                while (!wQue.empty()) {
                    wQue.pop();
                }
                for (int i=0; i<sz; i++) {
                    if (que[i] != -1)
                        wQue.push(que[i]);
                }
            }
            if (recvRq.Test(status) && isExit) {
                cout << "Backup Master completed." << endl;
                MPI_Finalize();
                exit(0);
            }
        }
    } else {
        //get work and send it back
        while(1){
            int exit;
            int tmpTar;
            MPI::COMM_WORLD.Recv(&exit, 1, MPI::INT, MPI::ANY_SOURCE, 0, status);
            if(exit){
                cout << "Processor " << rank << " completed." << endl;
                break;
            }
            work_t* newW = (work_t*) malloc(work_sz);
            MPI::COMM_WORLD.Recv(newW, work_sz, MPI::BYTE, MPI::ANY_SOURCE, 1, status);
            tmpTar = status.Get_source();
            tmpR = compute(newW);
            F_Send(tmpR, result_sz, MPI::BYTE, tmpTar, 1, rank);
            //MPI::COMM_WORLD.Isend(tmpR, result_sz, MPI::BYTE, tmpTar, 1);
        }
    }
}


result_t* Master_Worker::getResult() {
    return finalR;
}


void Master_Worker::setWorkSize(int sz) {
    work_sz = sz;
}

void Master_Worker::setResultSize(int sz) {
    result_sz = sz;
}

bool Master_Worker::isMaster() {
    if (rank == MASTER_RANK)
        return true;
    else
        return false;
}


// Master fail send
void Master_Worker::MF_Send() {
    if (random_fail(rank)) {
        MPI_Finalize();
        exit (0);
    } else {
        queue<int> tmpQ = wQue;
        int que[wPool.size()];
        for (int i=0; i<sz; i++) que[i] = -1;
        int i=0;
        while (!tmpQ.empty()) {
            que[i++] = tmpQ.front();
            tmpQ.pop();
        }
        MPI::COMM_WORLD.Send(timeList, sz, MPI::INT, BACKUP_MASTER , 2);
        MPI::COMM_WORLD.Send(workMap, sz, MPI::INT, BACKUP_MASTER , 2);
        MPI::COMM_WORLD.Send(vWorker, sz, MPI::INT, BACKUP_MASTER , 2);
        MPI::COMM_WORLD.Send(que, sz, MPI::INT, BACKUP_MASTER , 2);
    }
}

/***** Facilitating Function Fsend Dealing with Failures****/
void F_Send(void *buf, int count, MPI_Datatype datatype, int dest, int tag, int rank)
{
    if (random_fail(rank)) {
        cout << "this rank should fail: " << rank << endl;
        MPI_Finalize();
        exit (0);
    } else {
        MPI::COMM_WORLD.Isend(buf, count, datatype, dest, tag);
    }
}

//int randCnt = 0;
bool random_fail(int rank){
    /*auto timePnt = chrono::high_resolution_clock::now();
    auto ticks = chrono::duration_cast<chrono::microseconds>(timePnt-0);
    cout << "time: " << ticks << endl;*/
    srand(time(0)*rank*171);
    double tmpV = (double)rand()/RAND_MAX;
    return (tmpV > PVAL);
}





