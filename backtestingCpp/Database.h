#include <string>
#include <hdf5.h>

class Database
{
public:
    Database(const std::string &file_name);
    void close_file();
    double **get_data(const std::string &symbol, const std::string &exchange, int &array_size);

    hid_t h5_file;
};

int compare(const void *pa, const void *pb);