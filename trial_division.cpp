#include <iostream>
#include <vector>
#include <cmath>

using namespace std;

vector<long long> cal_primes(long long n) {
    vector<long long> pre;
    pre.push_back(2);
    for (long long i=3; i<=n; i++) {
        if (i%2 == 0) continue;
        bool isPrime = true;
        for (long long j=0; j<pre.size(); j++) {
            if (i%pre[j] == 0) isPrime = false;
        }
        if (isPrime)
            pre.push_back(i);
    }

    return pre;
}

vector<long long> trial_division(long long n) {
    vector<long long> res;
    if (n < 2) return res;
    vector<long long> primes = cal_primes((long long)sqrt(n));

    for (long long i=0; i<primes.size(); i++) {
        if (primes[i]*primes[i] > n) break;
        while (n%primes[i] == 0) {
            res.push_back(primes[i]);
            n /= primes[i];
        }
    }

    if (n>1) res.push_back(n);

    return res;
}

vector<long long> trial_division_t(long long n) {
    vector<long long> res;
    if (n < 2) return res;

    int c = n;
    for (long long i=2; i<(long long)sqrt(n); i++) {
        if (i*i > c) break;
        while (c%i == 0) {
            res.push_back(i);
            c /= i;
        }
    }

    if (c>1) res.push_back(c);
    
    return res;
}


int main() {
    long long n;
    cin >> n;
    vector<long long> res;
    res = trial_division_t(n);
    for (long long i=0; i<res.size(); i++) 
        cout << res[i] << " ";

    cout << endl;
    return 0;
}

