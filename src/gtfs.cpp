#include "gtfs.hpp"

#include <fcntl.h>    // For fcntl
#include <unistd.h>   // For close
#include <unordered_set>
#define VERBOSE_PRINT(verbose, str...) do { \
    if (verbose) cout << "VERBOSE: "<< __FILE__ << ":" << __LINE__ << " " << __func__ << "(): " << str; \
} while(0)

int do_verbose;

std::string string_to_binary(const std::string &input) {
    std::string binary_result;

    for (char c : input) {
        // Convert each character to an 8-bit binary string
        std::bitset<8> binary_char(c);
        binary_result += binary_char.to_string(); // Append the binary string
    }

    return binary_result; // Return the complete binary representation
}
std::string binary_to_string(const std::string &binary) {
    std::string result;

    // Process the binary string in chunks of 8 bits
    for (std::size_t i = 0; i < binary.length(); i += 8) {
        // Extract the 8-bit chunk
        std::string byte = binary.substr(i, 8);

        // Convert the 8-bit binary string to an unsigned long
        std::bitset<8> bits(byte);
        char character = static_cast<char>(bits.to_ulong()); // Convert to character

        result += character; // Append the character to the result
    }

    return result; // Return the reconstructed string
}
gtfs_t* gtfs_init(string directory, int verbose_flag) {
    do_verbose = verbose_flag;
    gtfs_t *gtfs = new gtfs_t();
    gtfs->dirname = directory;
    gtfs->next_write_id = 1;  // Initialize write ID counter
    VERBOSE_PRINT(do_verbose, "Initializing GTFileSystem inside directory " << directory << "\n");
    // Set up the lock struct
    
    gtfs->fl.l_type = F_WRLCK;     // Exclusive write lock
    gtfs->fl.l_whence = SEEK_SET;  // Lock from the beginning of the file
    gtfs->fl.l_start = 0;          // Start of the lock
    gtfs->fl.l_len = 0;            // 0 means lock the whole file
    // Check if directory exists
    struct stat sb;
    if (stat(directory.c_str(), &sb) == 0 && S_ISDIR(sb.st_mode)) {
        // Directory exists
    } else {
        // Directory does not exist, create it
        if (mkdir(directory.c_str(), 0777) != 0) {
            perror("mkdir");
            std::cerr << "Failed to create directory\n";
            delete gtfs;
            return NULL;
        }
    }

    // Open the log file
    gtfs->log_filename = directory + "/gtfs_log";
    gtfs->log_file.open(gtfs->log_filename.c_str(), std::ios::out | std::ios::app | std::ios::binary);
    VERBOSE_PRINT(do_verbose, "FILE map: "<<gtfs->open_files.size()<<endl);
    // Recover from log if necessary
    if (recover_from_log(gtfs) != 0) {
        std::cerr << "Recovery from log failed\n";
        delete gtfs;
        return NULL;
    }

    gtfs->log_file.open(gtfs->log_filename.c_str(), std::ios::out | std::ios::app| std::ios::binary);

    if (!gtfs->log_file.is_open()) {
        std::cerr << "Failed to open log file\n";
        delete gtfs;
        return NULL;
    }


    gtfs->mode='N';
    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns non NULL.
    return gtfs;
}

int recover_from_log(gtfs_t *gtfs) {
    VERBOSE_PRINT(do_verbose, "Recovering from log file\n");
    fstream log_file_in(gtfs->log_filename.c_str(), ios::in | std::ios::binary);
    if (!log_file_in.is_open()) {
        std::cerr << "Failed to open log file for reading\n";
        return -1;
    }

    string line;
    unordered_set<int> committed_operations;             // Set of write_ids that are committed
    vector<string> removed_file;
    while (getline(log_file_in, line)) {
        gtfs->mode='R';
        if (line.empty()) continue; // Skip empty lines

        log_entry_t entry;
        // VERBOSE_PRINT(do_verbose, "Retrieved Line " << line << "\n");
        line = binary_to_string(line);
        istringstream iss(line);
        VERBOSE_PRINT(do_verbose, "line to string" << line << "END\n");
        if (!(iss >> entry.action >>  entry.write_id >> entry.filename >> entry.offset >> entry.length)) {
            std::cerr <<  "Malformed log entry: " << line << "\n";
            continue;  // Skip malformed entries
        }

        // if file does not exist in the directory, skip the log entry
        string filepath = gtfs->dirname + "/" + entry.filename;
        struct stat sb;
        if (!(stat(filepath.c_str(), &sb) == 0 && S_ISREG(sb.st_mode))){
            continue;
        }

        if(gtfs->open_files.find(entry.filename) == gtfs->open_files.end()){
            file_t curfile(entry.filename, entry.length);
            gtfs->open_files[entry.filename] = &curfile;
        }
        file_t* curfile = gtfs->open_files[entry.filename];

        // Read data block of 'length' bytes
        char *data_buf = new char[entry.length];
        char ch[1];
        iss.read(ch,1);
        iss.read(data_buf, entry.length);
        if(iss.gcount() != entry.length){
            std::cerr << "Malformed log entry: " << line << "\n";
            continue;
        }
        // if its a begin
        if (entry.action == 'W') {//write

            // VERBOSE_PRINT(do_verbose,"IN RECOVERY, read from log: W: "<< data_buf);
            
    
            write_t* w = new write_t(gtfs, gtfs->open_files[entry.filename], entry.offset, entry.length, data_buf, entry.write_id);

            curfile->pending_writes.push_back(w);      

            if (entry.write_id >= gtfs->next_write_id) {
                gtfs->next_write_id = entry.write_id + 1;
            }
        } else if (entry.action == 'S') {//syncs
            for(int i = 0; i < curfile->pending_writes.size(); i++){
                if(curfile->pending_writes[i]->write_id == entry.write_id){
                    gtfs_sync_write_file(curfile->pending_writes[i]);
                    break;
                }
            }
        } else if (entry.action == 'A') {//abort

            for(int i = 0; i < curfile->pending_writes.size(); i++){
                if(curfile->pending_writes[i]->write_id == entry.write_id){
                    gtfs_abort_write_file(curfile->pending_writes[i]);
                    break;
                }
            }
        } else if(entry.action == 'R'){
            file_t to_remove(entry.filename,entry.length);
            gtfs_remove_file(gtfs, &to_remove);
        }else {
            std::cerr << "Unknown action in log: " << entry.action << "\n";
            continue;
        }
        
    }
    log_file_in.close();
    gtfs_clean(gtfs);
    gtfs->open_files.clear();
    // close all open files

    return 0;
}
string generate_log_entry(log_entry_t entry) {
    stringstream ss;
    ss << entry.action << " " << entry.write_id << " " << entry.filename << " "
       << entry.offset << " " << entry.length << " " << entry.data << "\n";
    //   cout << entry.action << " " << entry.write_id << " " << entry.filename << " "
    //    << entry.offset << " " << entry.length << " " << entry.data;
    string binrep = string_to_binary(ss.str())+"\n";
    return binrep;
}

int write_log_entry(gtfs_t *gtfs, log_entry_t &entry) {
    string log_entry_str = generate_log_entry(entry);
    gtfs->log_file << log_entry_str;
    gtfs->log_file.flush();  // Ensure the log entry is written to disk
    return 0;
}

void flush_log_file(gtfs_t *gtfs) {
    gtfs->log_file.flush();
}

int gtfs_clean(gtfs_t *gtfs) {
    int ret = -1;
    if (gtfs) {
        VERBOSE_PRINT(do_verbose, "Cleaning up GTFileSystem inside directory " << gtfs->dirname << "\n");

        // Loop through all open files and abort any pending writes
        for (auto& file_pair : gtfs->open_files) {
            file_t* file = file_pair.second;
            VERBOSE_PRINT(do_verbose, "Aborting pending writes for file: " << file->filename << "\n");
            file->pending_writes.clear();
        }

        // Close the log file if it’s open
        if (gtfs->log_file.is_open()) {
            gtfs->log_file.close();
        }

        // Reopen the log file in truncation mode
        std::ofstream clear_file(gtfs->log_filename.c_str(), std::ios::out | std::ios::trunc);
        clear_file.close(); // Close after truncating to ensure no file locks

        // Check the file size
        struct stat st;
        if (stat(gtfs->log_filename.c_str(), &st) == 0 && st.st_size != 0) {
            std::cerr << "Log file truncation failed\n";
            return -1;
        }

        ret = 0;
    } else {
        std::cerr << "GTFileSystem does not exist\n";
        return ret;
    }

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns 0.

    return ret;
}

file_t* gtfs_open_file(gtfs_t* gtfs, string filename, int file_length) {
    file_t *fl = NULL;
    if (gtfs) {
        VERBOSE_PRINT(do_verbose, "Opening file " << filename << " inside directory " << gtfs->dirname << "\n");

        // Check if filename length is up to MAX_FILENAME_LEN
        if (filename.length() > MAX_FILENAME_LEN) {
            std::cerr << "Filename too long\n";
            return NULL;
        }
        VERBOSE_PRINT(do_verbose, "NUM OPEN FILES:"<<gtfs->open_files.size()<<endl);
        // Check if the file is already open
        if (gtfs->open_files.find(filename) != gtfs->open_files.end()) {
    
            std::cerr << "File is already open\n";
            return NULL;
        }




        // Check if the number of files in the directory exceeds MAX_NUM_FILES_PER_DIR
        // We can count the number of files in the directory
        DIR *dir;
        struct dirent *ent;
        int file_count = 0;
        string filepath = gtfs->dirname + "/" + filename;
        // Check if the file exists
        struct stat sb;

        if (!(stat(filepath.c_str(), &sb) == 0 && S_ISREG(sb.st_mode))){
            if ((dir = opendir(gtfs->dirname.c_str())) != NULL) {
                while ((ent = readdir(dir)) != NULL) {
                    // Skip . and ..
                    if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                        continue;
                    // Skip the .gtfs_log file
                    if (strcmp(ent->d_name, ".gtfs_log") == 0)
                        continue;
                    file_count++;
                }
                closedir(dir);
            } else {
                // Could not open directory
                perror("opendir");
                return NULL;
            }

            if (file_count >= MAX_NUM_FILES_PER_DIR) {
                std::cerr << "Too many files in directory\n";
                return NULL;
            }
        }
    
        if(gtfs->closed_files.find(filename)!=gtfs->closed_files.end()){
            fl = gtfs->closed_files[filename];
            fl->file_length = file_length;
            gtfs->closed_files.erase(filename);
        }
        else{
            // Create the file_t instance
            fl = new file_t(filename,file_length);
        }

        // // Construct the full path to the file
        // string filepath = gtfs->dirname + "/" + filename;

        if (stat(filepath.c_str(), &sb) == 0 && S_ISREG(sb.st_mode)) {
            // File exists
            // Check its length
            ifstream infile(filepath.c_str(), ios::ate);
            if (!infile) {
                std::cerr << "Failed to open existing file\n";
                delete fl;
                return NULL;
            }
            int existing_length = infile.tellg();
            infile.close();
            if (existing_length <= file_length) {
                // Extend the file
                ofstream outfile(filepath.c_str(), ios::app);
                if (!outfile) {
                    std::cerr << "Failed to open existing file for appending\n";
                    delete fl;
                    return NULL;
                }
                // Extend the file by writing zeros
                int extend_length = file_length - existing_length;
                char *zeros = new char[extend_length]();
                outfile.write(zeros, extend_length);
                delete[] zeros;
                outfile.close();
            } else if (existing_length > file_length) {
                // Operation not permitted
                std::cerr << "Existing file length is larger than specified file_length\n";
                delete fl;
                return NULL;
            }
        } else {
            // File does not exist, create it
            ofstream outfile(filepath.c_str());
            if (!outfile) {
                std::cerr << "Failed to create new file\n";
                delete fl;
                return NULL;
            }
            // Initialize the file with zeros
            char *zeros = new char[file_length]();
            outfile.write(zeros, file_length);
            delete[] zeros;
            outfile.close();
        }

        // Now, add the file to the open_files map
        gtfs->open_files[filename] = fl;

    } else {
        std::cerr << "GTFileSystem does not exist\n";
        return NULL;
    }

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns non NULL.
    return fl;
}

int gtfs_close_file(gtfs_t* gtfs, file_t* fl) {
    int ret = -1;
    if (gtfs && fl) {
        VERBOSE_PRINT(do_verbose, "Closing file " << fl->filename << " inside directory " << gtfs->dirname << "\n");

        // Check if the file is in gtfs->open_files
        unordered_map<string, file_t*>::iterator it = gtfs->open_files.find(fl->filename);
        if (it != gtfs->open_files.end()) {
            // Ensure all pending writes are either committed or aborted
            if (!fl->pending_writes.empty()) {
                std::cerr << "Cannot close file with pending writes\n";
                return -1;
            }
            // Remove the file from open_files
            gtfs->open_files.erase(fl->filename);
            gtfs->closed_files[fl->filename]=fl;
            // Clean up the file_t structure
            ret = 0;
        } else {
            std::cerr << "File is not open\n";
            ret = -1;
        }
    } else {
        std::cerr <<" in close file： GTFileSystem or file does not exist\n";
        ret = -1;
    }
    
    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns 0.
    return ret;
}

int gtfs_remove_file(gtfs_t* gtfs, file_t* fl) {
    int ret = -1;
    if (gtfs && fl) {
        VERBOSE_PRINT(do_verbose, "Removing file " << fl->filename << " inside directory " << gtfs->dirname << "\n");

        // Check if the file is in gtfs->open_files
        unordered_map<std::string, file_t*>::iterator it = gtfs->open_files.find(fl->filename);
        if (it != gtfs->open_files.end()) {
            std::cerr << "Cannot remove an open file\n";
            return -1;
        }
        

        // Log the remove operation
        log_entry_t entry;
        entry.action = 'R';
        entry.filename = fl->filename;
        entry.offset = 0;
        entry.length = 2;
        entry.data = "NA";
        entry.write_id = gtfs->next_write_id++;

        if (write_log_entry(gtfs, entry) != 0) {
            std::cerr << "Failed to write log entry for remove\n";
            return -1;
        }

        flush_log_file(gtfs);

        // Remove the file from the directory
        string filepath = gtfs->dirname + "/" + fl->filename;
        if (remove(filepath.c_str()) != 0) {
            perror("remove");
            std::cerr << "Failed to remove file\n";
            return -1;
        }

        ret = 0;

    } else {
        std::cerr << "in remove file: GTFileSystem or file does not exist\n";
        ret = -1;
    }

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns 0.
    return ret;
}

char* gtfs_read_file(gtfs_t* gtfs, file_t* fl, int offset, int length) {
    char* ret_data = NULL;
    if (gtfs && fl) {
        VERBOSE_PRINT(do_verbose, "Reading " << length << " bytes starting from offset " << offset << " inside file " << fl->filename << "\n");

        // Check if offset and length are valid
        if (offset < 0 || length < 0 || offset + length > fl->file_length) {
        std::cerr << "Invalid offset or length\n";
            return NULL;
        }

        // Read the data from the file
        std::string filepath = gtfs->dirname + "/" + fl->filename;
        std::ifstream infile(filepath.c_str());
        if (!infile) {
            std::cerr << "Failed to open file for reading\n";
            return NULL;
        }

        char *data = new char[fl->file_length];
        infile.read(data, fl->file_length);
        infile.close();

        // Apply any pending writes
        for (std::vector<write_t*>::iterator it = fl->pending_writes.begin(); it != fl->pending_writes.end(); ++it) {
            write_t* write_op = *it;
            int write_offset = write_op->offset;
            int write_length = write_op->length;
            const char *write_data = write_op->data;
            int overlap_start = std::max(offset, write_offset);
            int overlap_end = std::min(offset + length, write_offset + write_length);
            if (overlap_end > overlap_start) {
                int copy_offset = overlap_start - write_offset;
                int data_offset = overlap_start - offset;
                int copy_length = overlap_end - overlap_start;
                std::memcpy(data + overlap_start, write_data + copy_offset, copy_length);
            }
        }

        // Extract the requested portion
        ret_data = new char[length + 1];
        std::memcpy(ret_data, data + offset, length);
        ret_data[length] = '\0';

        delete[] data;

    } else {
        std::cerr << "GTFileSystem or file does not exist\n";
        return NULL;
    }

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns pointer to data read.
    return ret_data;
}


write_t* gtfs_write_file(gtfs_t* gtfs, file_t* fl, int offset, int length, const char* data) {
    write_t *write_op = NULL;
    if (gtfs && fl) {
        VERBOSE_PRINT(do_verbose, "Writing " << length << " bytes starting from offset " << offset << " inside file " << fl->filename << "\n");

        // Check if offset and length are valid
        if (offset < 0 || length < 0 || offset + length > fl->file_length) {
            std::cerr <<"Invalid offset or length\n";
            return NULL;
        }

        // Create a new write_t
        write_op = new write_t(gtfs,fl,offset,length,new char[length],gtfs->next_write_id);
        VERBOSE_PRINT(do_verbose, "THIS IS WRITE's DATA itself: " << data<<", with length "<<strlen(data)<<" want length "<<length<<" lOOK HERE\n!");
        memcpy(write_op->data, data, length);
        VERBOSE_PRINT(do_verbose, "THIS IS WRITE's DATA after memcpy: " << write_op->data<<" lOOK HERE\n!");

        // Add the write to fl->pending_writes
        fl->pending_writes.push_back(write_op);
    

        // Log the remove operation
        log_entry_t entry;
        entry.action = 'W';
        entry.filename = fl->filename;
        entry.offset = offset;
        entry.length = length;
        entry.data = std::string(write_op->data,length);
        // entry.data.resize(length); // Resize to hold `length` elements
        // std::copy(write_op->data, write_op->data + length, entry.data.begin());
        entry.write_id = gtfs->next_write_id++;

        if (write_log_entry(gtfs, entry) != 0) {
            std::cerr << "Failed to write log entry for write\n";
            return NULL;
        }

        flush_log_file(gtfs);

    } else {
        std::cerr << "GTFileSystem or file does not exist\n";
        return NULL;
    }

    VERBOSE_PRINT(do_verbose, "Success, written:"<<write_op->data); //On success returns non NULL.

    return write_op;
}

int gtfs_sync_write_file(write_t* write_op) {
    int ret = -1;
    if (write_op) {
        VERBOSE_PRINT(do_verbose, "Persisting write of " << write_op->length << " bytes starting from offset " << write_op->offset << " inside file " << write_op->file->filename << "\n");

        gtfs_t *gtfs = write_op->gtfs;
        file_t *fl = write_op->file;

        if(gtfs->mode == 'N'){
            // Log the write operation
            log_entry_t entry;
            entry.action = 'S';
            entry.filename = fl->filename;
            entry.offset = write_op->offset;
            entry.length = write_op->length;
            entry.data = write_op->data;
            entry.write_id = write_op->write_id;
            VERBOSE_PRINT(do_verbose, "WROTE in sync to log: "<< entry.data<<"end");
            if (write_log_entry(gtfs, entry) != 0) {
                std::cerr << "Failed to write log entry for write\n";
                return -1;
            }

            flush_log_file(gtfs);
        }

        // Construct the file path
        std::string filepath = gtfs->dirname + "/" + fl->filename;

        // Open the file for writing
        std::fstream outfile(filepath.c_str(), std::ios::in | std::ios::out);
        if (!outfile) {
            std::cerr << "Failed to open file "<<fl->filename<<" for writing\n";
            return -1;
        }

        // Seek to the offset
        outfile.seekp(write_op->offset, std::ios::beg);
        if (outfile.fail()) {
            std::cerr << "Failed to seek in file\n";
            outfile.close();
            return -1;
        }

        // Write the data
        VERBOSE_PRINT(do_verbose, "WRITTEN "<<write_op->data<<" of length "<<write_op->length);
        outfile.write(write_op->data, write_op->length);
        if (outfile.fail()) {
            std::cerr << "Failed to write to file\n";
            outfile.close();
            return -1;
        }

        // Flush and close the file
        outfile.flush();
        outfile.close();

        // Remove the write from the pending_writes of the file
        std::vector<write_t*>& pending_writes = fl->pending_writes;
        pending_writes.erase(std::remove(pending_writes.begin(), pending_writes.end(), write_op), pending_writes.end());


        ret = write_op->length;

    } else {
        std::cerr << "Write operation does not exist\n";
        return -1;
    }

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns number of bytes written.
    return ret;
}

int gtfs_abort_write_file(write_t* write_op) {
    int ret = -1;
    if (write_op) {
        VERBOSE_PRINT(do_verbose, "Aborting write of " << write_op->length << " bytes starting from offset " << write_op->offset << " inside file " << write_op->file->filename << "\n");

        file_t *fl = write_op->file;
        gtfs_t *gtfs = write_op->gtfs;

        if(gtfs->mode == 'N'){
            // Log the write operation
            log_entry_t entry;
            entry.action = 'A';
            entry.filename = fl->filename;
            entry.offset = write_op->offset;
            entry.length = write_op->length;
            entry.data = write_op->data;
            entry.write_id = write_op->write_id;
            VERBOSE_PRINT(do_verbose, "WROTE in sync to log: "<< entry.data<<"end");
            if (write_log_entry(gtfs, entry) != 0) {
                std::cerr << "Failed to write log entry for write\n";
                return -1;
            }

            flush_log_file(gtfs);
        }

        // Remove the write from the pending_writes of the file
        std::vector<write_t*> &pending_writes = fl->pending_writes;
        pending_writes.clear();

        ret = 0;

    } else {
        std::cerr << "Write operation does not exist\n";
        return -1;
    }

    VERBOSE_PRINT(do_verbose, "Success.\n"); //On success returns 0.
    return ret;
}


int clean_characters_from_end(const std::string &filename, std::streamsize num_chars) {
    // Open the file in binary mode to manipulate the file's raw bytes
    std::fstream file(filename, std::ios::in | std::ios::out | std::ios::binary);
    
    if (!file.is_open()) {
        std::cerr << "Error opening file." << std::endl;
        return -1;
    }

    // Seek to the end of the file to determine its size
    file.seekg(0, std::ios::end);
    std::streamsize file_size = file.tellg(); // Get the current size of the file

    // Check if num_chars to clean is less than the current file size
    if (num_chars > file_size) {
        std::cerr << "Error: Number of characters to clean exceeds file size." << std::endl;
        file.close();
        return -1;
    }

    // Calculate the new file size after cleaning
    std::streamsize new_size = file_size - num_chars;

    // Truncate the file
    // Create a new file stream to truncate the file
    std::ofstream ofs(filename, std::ios::out | std::ios::binary | std::ios::trunc);
    if (!ofs.is_open()) {
        std::cerr << "Error opening file for truncating." << std::endl;
        file.close();
        return -1;
    }

    // Write the first new_size bytes back to the file
    file.seekg(0); // Go back to the start of the original file
    char *buffer = new char[new_size]; // Allocate buffer to hold the data
    file.read(buffer, new_size); // Read up to new_size bytes
    ofs.write(buffer, new_size); // Write the buffer to the new file
    delete[] buffer; // Clean up the allocated memory

    // Close the file streams
    file.close();
    ofs.close();

    std::cout << "Cleaned " << num_chars/8 << " characters from the end of the file." << std::endl;
    return 0;
}

// BONUS: Implement below API calls to get bonus credits

int gtfs_clean_n_bytes(gtfs_t *gtfs, int bytes){
    int ret = -1;
    if (gtfs) {
        VERBOSE_PRINT(do_verbose, "Cleaning up [ " << bytes << " bytes ] GTFileSystem inside directory " << gtfs->dirname << "\n");
        // Implement partial log cleaning by truncating the log after applying operations
        // For simplicity, assuming full log cleaning
        int cleaned_binary_bytes = bytes * 8 ;
            // Open the file in binary mode
        int ret = 0;
        if(gtfs->log_file.is_open()){
            gtfs->log_file.close();
            ret = clean_characters_from_end(gtfs->log_filename,cleaned_binary_bytes);
            gtfs->log_file.open(gtfs->log_filename.c_str(), std::ios::out | std::ios::app| std::ios::binary);
        }
        else{
            ret = clean_characters_from_end(gtfs->log_filename,cleaned_binary_bytes);
        }

    } else {
        std::cerr << "GTFileSystem does not exist\n";
        return ret;
    }

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns 0.
    return ret;
}

int gtfs_sync_write_file_n_bytes(write_t* write_op, int bytes){
    int ret = 0;
    if (write_op) {
        VERBOSE_PRINT(do_verbose, "Persisting [ " << bytes << " bytes ] write of " << write_op->length << " bytes starting from offset " << write_op->offset << " inside file " << write_op->file->filename << "\n");
        // Implement partial write synchronization
        // For simplicity, assuming full write synchronization
        gtfs_t *gtfs = write_op->gtfs;
        // Construct the file path
        std::string filepath = gtfs->dirname + "/" + write_op->file->filename;
        if(bytes > write_op->length){
            cerr<<"provided bytes longer than data"<<endl;
            return -1;
        }

        // Open the file for writing
        std::fstream outfile(filepath.c_str(), std::ios::in | std::ios::out);
        if (!outfile) {
            std::cerr << "Failed to open file for writing\n";
            return -1;
        }

        // Seek to the offset
        outfile.seekp(write_op->offset, std::ios::beg);
        if (outfile.fail()) {
            std::cerr << "Failed to seek in file\n";
            outfile.close();
            return -1;
        }

        // Write the data
        VERBOSE_PRINT(do_verbose, "WRITTEN "<<write_op->data<<" of length "<<bytes);
        outfile.write(write_op->data, bytes);
        if (outfile.fail()) {
            std::cerr << "Failed to write to file\n";
            outfile.close();
            return -1;
        }

        // Flush and close the file
        outfile.flush();
        outfile.close();

    } else {
        std::cerr << "Write operation does not exist\n";
        return ret;
    }

    VERBOSE_PRINT(do_verbose, "Success\n"); //On success returns 0.
    return ret;
}
