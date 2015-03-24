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

#define PVAL 0.7

//implementation of MPI_Run

Master_Worker::Master_Worker() {}
Master_Worker::Master_Worker(int wk_sz, int rs_sz, int m) : work_sz(wk_sz), result_sz(rs_sz), mode(m) {}

//declaration of other function
void F_Send(void *buf, int count, MPI_Datatype datatype, int dest, int tag, int rank);
bool random_fail(int rank);

void Master_Worker::Run(){
    //have the rank, have the size, devide into 0 and 1
    sz = MPI::COMM_WORLD.Get_size();
    if (sz < 2) {
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

//initiation function after creation
void Master_Worker::Init(){
    //push everything into wQue
    //cout << work_sz << endl;
    for(int i = 0; i < wPool.size(); i++){
        wQue.push(i);
    }
    //push onto timeList;
    time_t curT;
    time(&curT);
    for(int i = 0; i < sz; i++){
        timeList.push_back(curT);
    }
    //update workMap;
    vector<int> tmpVec(sz, -1);
    workMap = tmpVec;
    //update vWorker
    for(int i = 0; i < sz; i++){
        vWorker.push_back(1);
    }
}

void Master_Worker::directMode() {
    int tag=1;
    vector<result_t*> rList;
    if(rank == MASTER_RANK){
        for(int i = 1; i < sz; i++){
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

void Master_Worker::assignMode() {
    int n = wPool.size();
    int wNum = sz-1;
    vector<result_t*> rList;
    result_t* tmpR;
    work_t* tmpW;
    
    if (rank == MASTER_RANK) {
        create();
        Init();
        int n = wPool.size();
        int wNum = sz-1;
        int ind;
        //send sequentially to all workers, update send time
        for(ind = 0; ind < wNum && ind < n; ind++){
            int exit = 0;
            MPI::COMM_WORLD.Send(&exit, 1, MPI::INT, ind+1, 0);
            tmpW = wPool[wQue.front()];
            MPI::COMM_WORLD.Send(tmpW, work_sz, MPI::BYTE, ind+1, 1);
            //update map
            workMap[ind+1] = wQue.front();
            wQue.pop();
            //update the time Table
            time_t tmpT; time(&tmpT);
            cout << "tmp0: " << tmpT << endl;
            timeList[ind+1] = tmpT;
        }
        //start to wait for responce,
        //****everytime receiving, check out failure****//
        int cnt = 0;
        time_t checkP;
        time(&checkP);
        cout << wQue.size() << endl;
        int recvCnt = 0;
        cout << "n size: " << n << endl;
        while(recvCnt < n){
            /******* check timeList, see if any worker died *********/
            time_t curT; time(&curT);
            cout << "curT: " << curT << endl;
            cout << "checkP: " << checkP << endl;
            if(curT - checkP > 1){
                for(int i = 1; i < sz; i++){
                    time_t tmpT = timeList[i];
                    cout << "tmpT1: " << tmpT << endl;
                    if(tmpT < checkP){
                        //worker death detected, push the sent work back to wQue
                        cout << "worker died with number: " << i << endl;
                        wQue.push(workMap[i]);
                        //set vWorker
                        vWorker[i] = 0;
                    }
                }
                checkP = curT;
            }
            //cout << "r_sz: " << result_sz << endl;
            result_t* newR = (result_t*) malloc(result_sz);
            //see if received, if not->wait
            recvRq = MPI::COMM_WORLD.Irecv(newR, result_sz, MPI::BYTE, MPI::ANY_SOURCE, 1);
            //check request, if not received, wait
            bool suc = false;
            time_t startT; time(&startT);
            curT = startT;
            while(!suc && (curT-startT < 10)){
                suc = recvRq.Test(status);
                //cout << "this suc: " << suc << endl;
                if(suc) break;
                time(&curT);
                this_thread::sleep_for (chrono::milliseconds(100));
            }
            if(!suc){
                //assume all worker die, cout msg and exit
                //check valid count;
                int wCnt = 0;
                for(int i = 0; i < vWorker.size(); i++){
                    if(vWorker[i]) wCnt++;
                }
                if(wCnt) continue;
                cout << "all workers died!" << endl;
                exit(0);
            }
            else{
                rList.push_back(newR);
                recvCnt++;
                int tmpTar = status.Get_source();
                //see if work left
                if(!wQue.empty()){
                    tmpW = wPool[wQue.front()];
                    int tmpTar = status.Get_source();
                    cout << "tmpTar: " << tmpTar << endl;
                    int exit = 0;
                    MPI::COMM_WORLD.Send(&exit, 1, MPI::INT, tmpTar, 0);
                    MPI::COMM_WORLD.Send(tmpW, work_sz, MPI::BYTE, tmpTar, 1);
                    time_t tmpT; time(&tmpT);
                    cout << "tmpT2: " << tmpT << endl;
                    timeList[tmpTar] = tmpT;
                    workMap[tmpTar] = wQue.front();
                    wQue.pop();
                }
                cout << "recvCnt: " << recvCnt << endl;
            }
        }
        //send msgs to stop workers
        for(int i = 1; i <= wNum; i++){
            if(vWorker[i]){
                int exit = 1;
                cout << "here" << endl;
                MPI::COMM_WORLD.Send(&exit, 1, MPI::INT, i, 0);
            }
        }
        result(rList, finalR);
    }
    else{
        //get work and send it back
        while(1){
            int exit;
            int tmpTar;
            MPI::COMM_WORLD.Recv(&exit, 1, MPI::INT, MPI::ANY_SOURCE, 0);
            tmpTar = status.Get_source();
            if(exit){
                cout << "fuck" << endl;
                break;
            }
            work_t* newW = (work_t*) malloc(work_sz);
            MPI::COMM_WORLD.Recv(newW, work_sz, MPI::BYTE, tmpTar, 1);
            tmpR = compute(newW);
            /***** Failure ****/
            F_Send(tmpR, result_sz, MPI::BYTE, tmpTar, 1, rank);
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


/***** Facilitating Function Fsend Dealing with Failures****/
void F_Send(void *buf, int count, MPI_Datatype datatype, int dest, int tag, int rank)
{
    if (random_fail(rank)) {
        cout << "I am rank " << rank << " and I am dead! " << endl;
        MPI_Finalize();
        exit (0);
    } else {
        MPI::COMM_WORLD.Send(buf, count, datatype, dest, tag);
    }
}

//int randCnt = 0;
bool random_fail(int rank){
    /*auto timePnt = chrono::high_resolution_clock::now();
    auto ticks = chrono::duration_cast<chrono::microseconds>(timePnt-0);
    cout << "time: " << ticks << endl;*/
    srand(time(0)*rank*171);
    double tmpV = (double)rand()/RAND_MAX;
    cout << "tmpV: " << tmpV << endl;
    return (tmpV > PVAL);
}





