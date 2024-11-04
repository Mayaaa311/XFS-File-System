#include "../src/gtfs.hpp"

// Assumes files are located within the current directory
string directory;
int verbose;

// **Test 1**: Testing that data written by one process is then successfully read by another process.
void writer() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test1.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Hi, I'm the writer.\n";
    write_t *wrt = gtfs_write_file(gtfs, fl, 10, str.length(), str.c_str());

    gtfs_sync_write_file(wrt);

    gtfs_close_file(gtfs, fl);
}

void reader() {
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test1.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Hi, I'm the writer.\n";
    char *data = gtfs_read_file(gtfs, fl, 10, str.length());
    if (data != NULL) {
        cout<<"read data: "<<string(data)<<endl;
        str.compare(string(data)) == 0 ? cout << PASS : cout << FAIL;
    } else {
        cout << FAIL;
    }
    gtfs_close_file(gtfs, fl);
}

void test_write_read() {
    int pid;
    pid = fork();
    if (pid < 0) {
        perror("fork");
        exit(-1);
    }
    if (pid == 0) {
        writer();
        exit(0);
    }
    waitpid(pid, NULL, 0);
    reader();
}

// **Test 2**: Testing that aborting a write returns the file to its original contents.

void test_abort_write() {

    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test2.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);

    write_t *wrt2 = gtfs_write_file(gtfs, fl, 20, str.length(), str.c_str());
    gtfs_abort_write_file(wrt2);
    // gtfs_sync_write_file(wrt2);


    char *data1 = gtfs_read_file(gtfs, fl, 0, str.length());
    if (data1 != NULL) {
        // First write was synced so reading should be successfull
        if (str.compare(string(data1)) != 0) {
            cout << FAIL;
        }
        // Second write was aborted and there was no string written in that offset
        char *data2 = gtfs_read_file(gtfs, fl, 20, str.length());
        if (data2 == NULL) {
            cout << FAIL;
        } else if (string(data2).compare("") == 0) {
            cout << PASS;
        }
    } else {
        cout << FAIL;
    }
    gtfs_close_file(gtfs, fl);
}

// **Test 3**: Testing that the logs are truncated.

void test_truncate_log() {

    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test3.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);

    write_t *wrt2 = gtfs_write_file(gtfs, fl, 20, str.length(), str.c_str());
    gtfs_sync_write_file(wrt2);

    cout << "Before GTFS cleanup\n";
    system("ls -l .");

    gtfs_clean(gtfs);

    cout << "After GTFS cleanup\n";
    system("ls -l .");

    cout << "If log is truncated: " << PASS << "If exactly same output:" << FAIL;

    gtfs_close_file(gtfs, fl);

}

// TODO: Implement any additional tests
void test_truncate_log_n_bytes() {

    int truncate_byte = 18;
 //---------------------------case  where clean is used -----------------------
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test4.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing 4 string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);

    gtfs_clean_n_bytes(gtfs, truncate_byte);
    gtfs_close_file(gtfs, fl); 


    // get log file content
    std::ifstream infile("gtfs_log"); // Open the file
    if (!infile.is_open()) {
        std::cerr << "Error opening file." << std::endl;
    }
    std::string logcontent; // String to hold the file content
    // Read the entire file content
    logcontent.assign((std::istreambuf_iterator<char>(infile)), std::istreambuf_iterator<char>());
    // Close the file
    infile.close();

    // truncate log file
    std::ofstream clear_file( "gtfs_log", std::ios::out | std::ios::trunc);
    clear_file.close(); // Close after truncating to ensure no file locks
 //---------------------------case  where clean is unused -----------------------

    gtfs = gtfs_init(directory, verbose);
    filename = "test4.txt";
    fl = gtfs_open_file(gtfs, filename, 100);
    
    wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);
    gtfs_close_file(gtfs, fl); 

    std::ifstream logfile("gtfs_log"); // Open the file
    if (!logfile.is_open()) {
        std::cerr << "Error opening file." << std::endl;
    }
    std::string logcontent_noclean; // String to hold the file content
    // Read the entire file content
    logcontent_noclean.assign((std::istreambuf_iterator<char>(logfile)), std::istreambuf_iterator<char>());
    // Close the file
    logfile.close();

    //----------------compare size---------------------
    if(logcontent_noclean.size() - logcontent.size()== truncate_byte*8){
        cout << PASS;
    }
    else{
        cout << FAIL;
    }
}


void test_write_n_byteds() {
    int n_bytes = 5;
    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test5.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing 5 string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);
    
    std::string data1; // String to hold the file content
    // Read the entire file content
    std::ifstream infile(filename); // Open the file
    data1.assign((std::istreambuf_iterator<char>(infile)), std::istreambuf_iterator<char>());
    // Close the file
    infile.close();
    // truncate log file
    std::ofstream clear_file( "test5.txt", std::ios::out | std::ios::trunc);
    clear_file.close(); // Close after truncating to ensure no file locks


    write_t *wrt2 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file_n_bytes(wrt2, n_bytes);
    std::string data2; // String to hold the file content
    // Read the entire file content
    std::ifstream infile2(filename); // Open the file
    data2.assign((std::istreambuf_iterator<char>(infile2)), std::istreambuf_iterator<char>());
    // Close the file
    infile2.close();

    for(int i = 0; i < n_bytes; i++){
        if(data1[i] != data2[i]){
            cout<<FAIL<<" at index "<<i;
            cout<<"data1: "<<data1<<"; data2: "<<data2<<endl;
            return;
        }
    }
    if(data2.size()== n_bytes){
        cout<<PASS;
    }
    else{
        cout<<"data1: "<<data1<<"; data2: "<<data2<<endl;
        cout<<FAIL;
    }
    gtfs_close_file(gtfs, fl);
}


// Test 6 multi write and then sync
void test_multi_write() {

    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test6.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    

    write_t *wrt2 = gtfs_write_file(gtfs, fl, 20, str.length(), str.c_str());
    gtfs_sync_write_file(wrt2);
    gtfs_sync_write_file(wrt1);
    // gtfs_sync_write_file(wrt2);


    char *data1 = gtfs_read_file(gtfs, fl, 20, str.length());
    if (data1 != NULL) {
        // First write was synced so reading should be successfull
        if (str.compare(string(data1)) != 0) {
            cout << FAIL;
        }
        // Second write was aborted and there was no string written in that offset
        char *data2 = gtfs_read_file(gtfs, fl, 0, str.length());
        if (data2 == NULL) {
            cout << FAIL;
        } else if (str.compare(string(data2)) == 0) {
            cout << PASS;
        }
        else{
            cout<<"||||"<<data2<<"|||\n"<<endl;
            cout<<FAIL;
        }
    } else {
        cout << FAIL;
    }
    gtfs_close_file(gtfs, fl);
}

// // Test 7 multi read before sync
void test_read_before_syncwrite() {

    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test7.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    

    write_t *wrt2 = gtfs_write_file(gtfs, fl, 20, str.length(), str.c_str());
    // gtfs_sync_write_file(wrt2);


    char *data1 = gtfs_read_file(gtfs, fl, 20, str.length());
    if (data1 != NULL) {
        // First write was synced so reading should be successfull
        if (str.compare(string(data1)) != 0) {
            cout << FAIL;
        }
        // Second write was aborted and there was no string written in that offset
        char *data2 = gtfs_read_file(gtfs, fl, 0, str.length());
        if (data2 == NULL) {
            cout << FAIL;
        } else if (str.compare(string(data2)) == 0) {
            cout << PASS;
        }
    } else {
        cout << FAIL;
    }
    gtfs_sync_write_file(wrt2);
    gtfs_sync_write_file(wrt1);
    gtfs_close_file(gtfs, fl);
}

// Test 8 system crash during sync( one synced write and one abort write should show in recovery)
void test_sync_crush() {

    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test8.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    write_t *wrt2 = gtfs_write_file(gtfs, fl, 20, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);
    //delete and then recreate to simulate a crush on sync
    //remove the file
    if( remove(filename.c_str()) != 0){
       cout<<"fail to delete file"<<endl;
    };
    //recreate the file so that we have a clean file
    std::ofstream file(filename.c_str());

    gtfs = gtfs_init(directory, verbose);
    fl = gtfs_open_file(gtfs, filename, 100);
    char *data1 = gtfs_read_file(gtfs, fl, 0, str.length());
    if (data1 != NULL) {
        // First write was synced so reading should be successfull
        if (str.compare(string(data1)) != 0) {
            cout << FAIL;
        }
        // Second write was aborted and there was no string written in that offset
        char *data2 = gtfs_read_file(gtfs, fl, 20, str.length());
        if (data2 == NULL) {
            cout << FAIL;
        } else if (string(data2).compare("") == 0) {
            cout << PASS;
        }
        else{
            cout << FAIL;
        }
    } else {
        cout << FAIL;
    }
    gtfs_close_file(gtfs, fl);
}

// Test 9 close pending write and double open file
void test_reopen_close_pending() {

    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test9.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    write_t *wrt2 = gtfs_write_file(gtfs, fl, 20, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);
    gtfs_close_file(gtfs,fl);
    file_t *fl_place_holder = gtfs_open_file(gtfs, filename, 100);
    gtfs_close_file(gtfs, fl);
    cout<<"if 3 error messge occur above "<<PASS<<" else "<<FAIL<<endl;
    char *data1 = gtfs_read_file(gtfs, fl, 0, str.length());
    
    cout<<"test if both writes could still be read";
    if (data1 != NULL) {
        // First write was synced so reading should be successfull
        if (str.compare(string(data1)) != 0) {
            cout << FAIL;
        }

        char *data2 = gtfs_read_file(gtfs, fl, 20, str.length());
        if (data2 == NULL) {
            cout << FAIL;
        } else if (str.compare(string(data2)) == 0) {
            cout << PASS;
        }
        else{
            cout << FAIL;
        }
    } else {
        cout << FAIL;
    }
    
}

// Test 10 
void test_remove_file() {

    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test10.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);

    write_t *wrt2 = gtfs_write_file(gtfs, fl, 20, str.length(), str.c_str());
    gtfs_sync_write_file(wrt2);

    cout << "Before GTFS cleanup\n";
    system("ls -l .");
    // gtfs_close_file(gtfs, fl);
    gtfs_close_file(gtfs,fl);
    gtfs_remove_file(gtfs,fl);
    
    filename = "test1.txt";
    fl = gtfs_open_file(gtfs, filename, 100);
    gtfs_close_file(gtfs,fl);
    gtfs_remove_file(gtfs,fl);

    cout << "After GTFS cleanup\n";
    system("ls -l .");

    cout << "If test1.txt and test10.txt are gone: " << PASS << "If test1.txt and test10.txt are still in the directory: " << FAIL;



}

// Test 11
void test_open_file_size() {

    gtfs_t *gtfs = gtfs_init(directory, verbose);
    string filename = "test11.txt";
    file_t *fl = gtfs_open_file(gtfs, filename, 100);

    string str = "Testing string.\n";
    write_t *wrt1 = gtfs_write_file(gtfs, fl, 0, str.length(), str.c_str());
    gtfs_sync_write_file(wrt1);

    write_t *wrt2 = gtfs_write_file(gtfs, fl, 20, str.length(), str.c_str());
    gtfs_sync_write_file(wrt2);

    gtfs_close_file(gtfs,fl);    
    fl = gtfs_open_file(gtfs, filename, 120);
    gtfs_close_file(gtfs,fl);
    // gtfs_close_file(gtfs, fl);
    fl = gtfs_open_file(gtfs, filename, 100);
    gtfs_close_file(gtfs,fl);




    cout << "If error message on open file: " << PASS << " If sucess open file with smaller length " << FAIL;



}



int main(int argc, char **argv) {
    if (argc < 2)
        printf("Usage: ./test verbose_flag\n");
    else
        verbose = strtol(argv[1], NULL, 10);

    // Get current directory path
    char cwd[256];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        directory = string(cwd);
    } else {
        cout << "[cwd] Something went wrong.\n";
    }

    // Call existing tests
    cout << "================== Test 1 ==================\n";
    cout << "Testing that data written by one process is then successfully read by another process.\n";
    test_write_read();

    cout << "================== Test 2 ==================\n";
    cout << "Testing that aborting a write returns the file to its original contents.\n";
    test_abort_write();

    cout << "================== Test 3 ==================\n";
    cout << "Testing that the logs are truncated.\n";
    test_truncate_log();


    cout << "================== Custom test - Test 4 ==================\n";
    cout << "Testing that the logs are truncated by n bytes.\n";
    test_truncate_log_n_bytes();

    // TODO: Call any additional tests

    cout << "================== Custom test - Test 5 ==================\n";
    cout << "Testing that the write are synced by only n bytes\n";
    test_write_n_byteds();

    // test for 
    cout << "================== Custom test - Test 6 ==================\n";
    cout << "Testing that multi write worked\n";
    test_multi_write();


    cout << "================== Custom test - Test 7 ==================\n";
    cout << "Testing reading unsynced data\n";
    test_read_before_syncwrite();

    cout << "================== Custom test - Test 8 ==================\n";
    cout << "Testing crush during sync\n";
    test_sync_crush();


    cout << "================== Custom test - Test 9 ==================\n";
    cout << "Testing close file with pending write, double opening file, and read only synced data\n";
    test_reopen_close_pending();


    //remove file 
    cout << "================== Custom test - Test 10 ==================\n";
    cout << "Testing remove file\n";
    test_remove_file();

    cout << "================== Custom test - Test 11 ==================\n";
    cout << "Testing open file with larger size and smaller\n";
    test_open_file_size();
}
