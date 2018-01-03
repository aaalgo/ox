#include <iostream>
#include "ox.h"
using namespace std;
int main () {
    ::system("rm -rf test.db");
    ox::HashTable::Config conf;
    ox::HashTable::create("test.db", conf);
    {
        ox::HashTable hash("test.db");
        hash.report(std::cerr);
        hash.extend(100);
        for (int i = 0; i < 100; ++i) {
            for (int j = 0; j < i; ++j) {
                hash.append(i, sizeof(int), [i](char *buf){
                            *(int *)(buf) = i;
                            return buf + sizeof(int);
                        });
            }
        }
        hash.flush();
        for (int i = 0; i < 100; ++i) {
            int c = 0;
            hash.scan(i, [i, &c](char const *buf) {
                        int x = *(int const *)buf;
                        if (x ==i) ++c;
                        return buf + sizeof(int);
                    });
            CHECK(i == c);
            //cout << i << ' ' << c << endl;
        }
    }
    {
        ox::HashTable hash("test.db");
        for (int i = 0; i < 100; ++i) {
            int c = 0;
            hash.scan(i, [i, &c](char const *buf) {
                        int x = *(int const *)buf;
                        if (x ==i) ++c;
                        return buf + sizeof(int);
                    });
            CHECK(i == c);
            //cout << i << ' ' << c << endl;
        }
    }

}

