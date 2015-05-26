//
// Created by Erik Muttersbach on 20/05/15.
//

#ifndef HDFS_BENCHMARK_PRIORITYQUEUE_H
#define HDFS_BENCHMARK_PRIORITYQUEUE_H

#include <queue>
#include <vector>
#include <functional>

using namespace std;

template<class _Tp,
        class _Container = vector<_Tp>,
        class _Compare = less<typename _Container::value_type> >
class PriorityQueue {
public:
    PriorityQueue() {
        make_heap(elements.begin(), elements.end(), _Compare());
    }

    void push(_Tp element) {
        elements.push_back(element);
        push_heap(elements.begin(), elements.end(), _Compare());
    }

    void remove(typename vector<_Tp>::iterator it) {
        elements.erase(it, it+1);
        make_heap(elements.begin(), elements.end(), _Compare());
    }

    _Tp pop() {
        pop_heap(elements.begin(), elements.end(), _Compare());
        _Tp element = elements.back();
        elements.pop_back();
        return element;
    }

    _Tp peek() {
        make_heap(elements.begin(), elements.end(), _Compare());
        return elements.front();
    }

    typename vector<_Tp>::iterator begin() {
        return elements.begin();
    }

    typename vector<_Tp>::iterator end() {
        return elements.end();
    }

private:
    vector<_Tp> elements;
};


#endif //HDFS_BENCHMARK_PRIORITYQUEUE_H
