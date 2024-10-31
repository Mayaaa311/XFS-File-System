#ifndef GTFS
#define GTFS

#include <string>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fstream>
#include <cstring>
#include <map>
#include <vector>
#include <algorithm>
#include <sstream>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/file.h>
#include <iomanip>

using namespace std;

#define PASS "\033[32;1m PASS \033[0m\n"
#define FAIL "\033[31;1m FAIL \033[0m\n"

// GTFileSystem basic data structures

#define MAX_FILENAME_LEN 255
#define MAX_NUM_FILES_PER_DIR 1024

extern int do_verbose;

typedef struct gtfs gtfs_t;
typedef struct file file_t;
typedef struct write write_t;

typedef struct log_entry {
    char action;     // "BEGIN", "COMMIT", "ABORT", "WRITE"
    int write_id;      // Unique write ID
    string filename;
    int offset;
    int length;
    string data;       // Data (may contain any characters)
} log_entry_t;


struct gtfs {
    string dirname;
    struct flock fl;
    char mode;//recover, Normal
    unordered_map<string, file_t*> open_files;
    fstream log_file;
    string log_filename;
    int next_write_id;
    // Additional fields for crash recovery
};

struct write {
    gtfs_t *gtfs;
    file_t *file;
    int offset;
    int length;
    char *data;
    int write_id;   // Unique write ID for this operation

        // Constructor definition
    write(gtfs_t* g, file_t* f, int o, int l, char* d, int id)
        : gtfs(g), file(f), offset(o), length(l), data(d), write_id(id) {}

    ~write() {
    delete[] data;  // Ensure data is freed when write_t is destroyed
    }
};

struct file {
    string filename;
    int file_length;
    vector<write_t*> pending_writes;

    // Additional fields if necessary

    // Constructor to initialize filename and file_length
    file(const string& fname, int flength)
        : filename(fname), file_length(flength), pending_writes() {}  // pending_writes is default-initialized as an empty vector

};

// GTFileSystem basic API calls

gtfs_t* gtfs_init(string directory, int verbose_flag);
int gtfs_clean(gtfs_t *gtfs);

file_t* gtfs_open_file(gtfs_t* gtfs, string filename, int file_length);
int gtfs_close_file(gtfs_t* gtfs, file_t* fl);
int gtfs_remove_file(gtfs_t* gtfs, file_t* fl);

char* gtfs_read_file(gtfs_t* gtfs, file_t* fl, int offset, int length);
write_t* gtfs_write_file(gtfs_t* gtfs, file_t* fl, int offset, int length, const char* data);
int gtfs_sync_write_file(write_t* write_op);
int gtfs_abort_write_file(write_t* write_op);
int gtfs_clean_n_bytes(gtfs_t *gtfs, int bytes);
int gtfs_sync_write_file_n_bytes(write_t* write_op, int bytes);

// Additional helper functions
int recover_from_log(gtfs_t *gtfs);
int write_log_entry(gtfs_t *gtfs, log_entry_t &entry);
void flush_log_file(gtfs_t *gtfs);

#endif
