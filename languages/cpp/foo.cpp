#include <iostream>
using namespace std;

int main(int nargs, char* args[]) {
  if (nargs > 1) {
    size_t nchars = strlen(args[1]) + 1;
    wchar_t wcstring[nchars];
    mbstowcs(wcstring, args[1], nchars);

    cout << "original char*:" << args[1] << endl;
    wcout << "raw wchat_t:" << wcstring << endl;
  }
}
