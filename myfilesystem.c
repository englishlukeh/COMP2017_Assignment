#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <pthread.h>

#include "myfilesystem.h"

// helper function to truncate filenames
static void truncate(char * filename){
	if (strlen(filename) > 63)
		filename[63] = '\0';
}

// define node for offset sorted list
typedef struct offset_node{
    int offset;
	int length;
	int file_index;
	char filename[64];
    struct offset_node * next;
} offset_node;

// define helper node which points to headers for offset sorted list, hash tree within virtual memory, three FILEs and data about file system
typedef struct helper_node{
	offset_node * offset_node;
	
	uint8_t * hash_tree;
	int number_of_blocks;
	int max_depth;
	
	FILE * file_data;
	FILE * directory_table;
	FILE * hash_data;
	
	size_t total_space;
	size_t filled_space;
	pthread_mutex_t list_lock;
	
} helper_node;

// recursive helper method to verify hash data
// returns the total number of node within hash tree that are incorrect
// (i.e. returns 0 if hash tree is correct
static int verify_hash_block_rec(void * helper, int offset, int depth){
	helper_node * node_pointer = helper;
	
	if (depth == 0){
		return 0;
	}
	
	else if (depth == node_pointer->max_depth){
		// read in data from block in file_data and calculate fletcher
		void * tmp_file_data = malloc(256);
		int start_offset = (pow(2, node_pointer->max_depth + 1) - 1) - node_pointer->number_of_blocks;
		int file_data_offset = (offset - start_offset) * 256;
		
		uint8_t * buffercalc = malloc(16);
		uint8_t * bufferread = malloc(16);
		
		fseek(node_pointer->file_data, file_data_offset, SEEK_SET);
		fread(tmp_file_data, 256, 1, node_pointer->file_data);
		
		fletcher(tmp_file_data, 256, buffercalc);
		
		fseek(node_pointer->hash_data, offset * 16, SEEK_SET);
		fread(bufferread, 16, 1, node_pointer->hash_data);
		
		// flush buffers for multithreading
		fflush(node_pointer->hash_data);
		fflush(node_pointer->file_data);
		free(tmp_file_data);
				
		if (memcmp(buffercalc, bufferread, 16) != 0){
			free(buffercalc);
			free(bufferread);
			return 1 + verify_hash_block_rec(helper, (int)((offset-1)/2), depth-1);
		}
		else{
			free(buffercalc);
			free(bufferread);
			return verify_hash_block_rec(helper, (int)((offset-1)/2), depth-1);
		}
	}
	
	else{
		//fletcher block n + n+1
		// then go up
		
		void * tmp_file_data = malloc(32);
		
		uint8_t * buffercalc = malloc(16);
		uint8_t * bufferread = malloc(16);
		
		fseek(node_pointer->hash_data, offset * 16, SEEK_SET);
		fread(bufferread, 16, 1, node_pointer->hash_data);
		
		fseek(node_pointer->hash_data, (16 * ((offset * 2) + 1)), SEEK_SET);
		fread(tmp_file_data, 32, 1, node_pointer->hash_data);
		
		fletcher(tmp_file_data, 32, buffercalc);
		free(tmp_file_data);
		
		if (memcmp(buffercalc, bufferread, 16) != 0){
			free(buffercalc);
			free(bufferread);
			return 1 + verify_hash_block_rec(helper, (int)((offset-1)/2), depth-1);
		}
		else{
			free(buffercalc);
			free(bufferread);
			return verify_hash_block_rec(helper, (int)((offset-1)/2), depth-1);
		}
	}
	
	// should never reach here
	return 0;
}

// helper function to verify hash tree from starting block
// calls recursive helper function for verification of tree
// returns the total number of node within hash tree that are incorrect
// (i.e. returns 0 if hash tree is correct
static int verify_hash_block(size_t block_offset, void * helper) {
	helper_node * node_pointer = helper;
	int start_offset = (pow(2, node_pointer->max_depth + 1) - 1) - node_pointer->number_of_blocks;
	
    return verify_hash_block_rec(helper, start_offset + block_offset, node_pointer->max_depth);
}

// helper function to see if filename exists
// returns 0 if file exists, 1 if it doesn't exist
static int does_filename_exist(void * helper, char * filename){
	
	helper_node * node_pointer = helper;
	offset_node * offset_tmp_pointer = node_pointer->offset_node;
	
	while(offset_tmp_pointer->next != NULL){
	if (strncmp(offset_tmp_pointer->next->filename, filename, 64) == 0){
		// file exists
		return 0;
	}
		
		offset_tmp_pointer = offset_tmp_pointer->next;
	}	
	
	// file doesn't exist
	return 1;
}

// recursive helper method to calculate hash of node in hash tree
// always returns 0
static int calculate_hash(void * helper, int offset, int depth){
	
	helper_node * node_pointer = helper;
	//printf("Max depth: %d, Depth: %d, Offset: %d\n", node_pointer->max_depth, depth, offset);
	
	if (depth == node_pointer->max_depth){
		// read in data from block in file_data and calculate fletcher
		void * tmp_file_data = malloc(256);
		int start_offset = (pow(2, node_pointer->max_depth + 1) - 1) - node_pointer->number_of_blocks;
		int file_data_offset = (offset - start_offset) * 256;
		
		fseek(node_pointer->file_data, file_data_offset, SEEK_SET);
		fread(tmp_file_data, 256, 1, node_pointer->file_data);
		
		fletcher(tmp_file_data, 256, node_pointer->hash_tree + (offset * 16));
	
		fseek(node_pointer->hash_data, offset * 16, SEEK_SET);
		fwrite(node_pointer->hash_tree + (offset * 16), 16, 1, node_pointer->hash_data);
		fflush(node_pointer->hash_data);
		free(tmp_file_data);
		return 0;
	}
	
	else{
		// calculate left node, then right node and then fletcher the concatenation of the two
		calculate_hash(helper, (offset * 2) + 1, depth + 1);
		calculate_hash(helper, (offset * 2) + 2, depth + 1);
		fletcher(node_pointer->hash_tree + (16 * ((offset * 2) + 1)), 32, node_pointer->hash_tree + (offset * 16));
		
		// update hash_data file
		fseek(node_pointer->hash_data, offset * 16, SEEK_SET);
		fwrite(node_pointer->hash_tree + (offset * 16), 16, 1, node_pointer->hash_data);
		fflush(node_pointer->hash_data);
		return 0;
	}
}

// computes hash tree of file_data and stores it in hash_data
// calls recursive hash calculation function
void compute_hash_tree(void * helper) {
	helper_node * node_pointer = helper;
	pthread_mutex_lock(&node_pointer->list_lock);
	calculate_hash(helper, 0, 0);
	
	// flush buffers for multithreading
	fflush(node_pointer->file_data);
	fflush(node_pointer->directory_table);
	fflush(node_pointer->hash_data);		
	
	pthread_mutex_unlock(&node_pointer->list_lock);
    return;
}

// helper method for repacking
// returns 1 if no files exist
// returns 0 if files exist and has repacked successfully
static int repack_helper(void * helper){

	helper_node * node_pointer = helper;
	offset_node * offset_tmp_pointer = node_pointer->offset_node;	
	int last_free_offset = 0;
	void * tmp_memory = NULL;
	
	if (offset_tmp_pointer->next == NULL){ //no files exist
		return 1;
	}
	else{
		offset_tmp_pointer = offset_tmp_pointer->next;
	}
	
	while (offset_tmp_pointer != NULL){
		if (offset_tmp_pointer->offset >= last_free_offset){ //file can be shifted to the left
			tmp_memory = malloc(offset_tmp_pointer->length);
			
			//read file data to tmp_memory
			fseek(node_pointer->file_data, offset_tmp_pointer->offset, SEEK_SET);
			fread(tmp_memory, offset_tmp_pointer->length, 1, node_pointer->file_data);
			
			//write file data from tmp_memory to new offset
			fseek(node_pointer->file_data, last_free_offset, SEEK_SET);
			fwrite(tmp_memory, offset_tmp_pointer->length, 1, node_pointer->file_data);
			
			//write data directory
			fseek(node_pointer->directory_table, (offset_tmp_pointer->file_index) + 64, SEEK_SET);
			fwrite(&last_free_offset, 4, 1, node_pointer->directory_table);
			
			// flush buffers for multithreading
			fflush(node_pointer->file_data);
			fflush(node_pointer->directory_table);
			fflush(node_pointer->hash_data);		
			
			//adjust offset in both sorted lists
			free(tmp_memory);
			offset_tmp_pointer->offset = last_free_offset;
			last_free_offset = last_free_offset + offset_tmp_pointer->length;
		}
		
		offset_tmp_pointer = offset_tmp_pointer->next;
	}
    return 0;
}

// helper method to find free file_index in directory_table
// returns the first free file index in directory_table
static int find_free_file_index(void * helper){
	
	helper_node * node_pointer = helper;
	
	fseek(node_pointer->directory_table, 0, SEEK_END);
	int file_directory_table_size = ftell(node_pointer->directory_table);
	int file_index = 0;

	fseek(node_pointer->directory_table, 0, SEEK_SET);
	
	char buff = '\0';
	char null_byte = '\0';
	
	while(file_index < file_directory_table_size){
		fread(&buff, 1, 1, node_pointer->directory_table);
		if (memcmp(&buff, &null_byte, 1) == 0){
			return file_index;
		}
		file_index += 72;
		fseek(node_pointer->directory_table, 71, SEEK_CUR);
	}
	
	// no free file indexes
	return -1;
	
}

// initializes linked list for file nodes
// nodes are stored in a singly linked lists:
// list stores offset, length, file index (in directory_data) and filename and is sorted based on offset
static void * init_list(void){
	
	helper_node * node_pointer = malloc(sizeof(helper_node));
	if(node_pointer == NULL) // malloc error
		return NULL;

	// create offset sorted list header
	offset_node * offset_header = calloc(1, sizeof(offset_node));
	if(offset_header == NULL) // malloc error
		return NULL;
	offset_header->offset = -1;
	offset_header->length = -1;
	offset_header->file_index = -1;

	// attach helper node to both header nodes
	node_pointer->offset_node = offset_header;
	
	node_pointer->hash_tree = NULL;
	return (void *) node_pointer;
}

// when nodes are added, insert into correct location to keep sorted
// returns 0 if successful,
// returns 1 if unsucessful (malloc error)
static int add_node(void * helper, char * filename, int offset, int length, int file_index){
	
	helper_node * node_pointer = helper;
	
	// create entry in offset sorted list
	offset_node * offset_tmp_pointer = node_pointer->offset_node;
		
	// loop through list until next node is either NULL or offset is less than the offset to be inserted
	while(offset_tmp_pointer->next != NULL && offset > offset_tmp_pointer->next->offset){
		offset_tmp_pointer = offset_tmp_pointer->next;
	}

	// add node to offset sorted list
	offset_node * offset_node_pointer = malloc(sizeof(offset_node));
	if (offset_node_pointer == NULL){ //malloc error
		return 1;
	}
	offset_node_pointer->offset = offset;
	offset_node_pointer->length = length;
	strncpy(offset_node_pointer->filename, filename, 64);
	offset_node_pointer->file_index = file_index;
	offset_node_pointer->next = offset_tmp_pointer->next;
	offset_tmp_pointer->next = offset_node_pointer;

	node_pointer->filled_space += length;
	
	return 0;
}

// removes node with filename from sorted list
// returns 0 if successful, returns 1 if filename doesn't exist
static int remove_node(void * helper, char * filename){	
	
	helper_node * node_pointer = helper;
	
	// remove from offset sorted list
	offset_node * offset_tmp_pointer = node_pointer->offset_node;
	
	while(offset_tmp_pointer->next != NULL){
		if (strncmp(offset_tmp_pointer->next->filename, filename, 64) == 0){
			offset_node * tmp_offset_node = offset_tmp_pointer->next;
			offset_tmp_pointer->next = offset_tmp_pointer->next->next;
			
			// subtract file size from filled space
			int file_size = tmp_offset_node->length;
			node_pointer->filled_space -= file_size;
			
			free(tmp_offset_node);
			return 0;
		}
		
		offset_tmp_pointer = offset_tmp_pointer->next;
	}	
	return 1;
}

// helper method to get node in offset sorted list from filename
static offset_node * get_offset_node(void * helper, char * filename){
	
	helper_node * node_pointer = helper;
	
	offset_node * offset_tmp_pointer = node_pointer->offset_node;
	
	while(offset_tmp_pointer->next != NULL){
		if (strncmp(offset_tmp_pointer->next->filename, filename, 64) == 0){
			return offset_tmp_pointer->next;
		}
		offset_tmp_pointer = offset_tmp_pointer->next;
	}

	return NULL;
}

// helper function to delete files
// returns 1 if file doesn't exist
// returns 0 if file deleted successfully
static int delete_file_helper(char * filename, void * helper){
	helper_node * node_pointer = helper;
	offset_node * tmp = get_offset_node(helper, filename);
	char null_byte = '\0';
	
	if (tmp != NULL){ //file exists
		fseek(node_pointer->directory_table, tmp->file_index, SEEK_SET);
		fwrite(&null_byte, 1, 1, node_pointer->directory_table);
		// flush buffers for multithreading
		fflush(node_pointer->directory_table);
		remove_node(helper, filename);
		return 0;
	}
	else{ //file doesn't exist
		return 1;
	}
}

// function to initialize all data structures from three files
// returns pointer to helper node memory address
// returns NULL if an error is experienced during initialization
void * init_fs(char * f1, char * f2, char * f3, int n_processors) {
	
	int int_bytes = sizeof(int);
	
	//truncate filenames if necessary
	truncate(f1);
	truncate(f2);
	truncate(f3);
	
	//return error if any duplicate names
	if (strcmp(f1, f2) == 0 || strcmp(f1, f3) == 0 || strcmp(f2, f3) == 0){
		printf("Error: duplicate filenames\n");
		return NULL;
	}
	
	FILE * file_data_pointer;
	FILE * directory_table_pointer;
	FILE * hash_data_pointer;
	
	//open files
	file_data_pointer = fopen(f1, "r+");
	if (file_data_pointer == NULL){
		perror("Error");
		return NULL;
	}
		
	directory_table_pointer = fopen(f2, "r+");
	if (directory_table_pointer == NULL){
		perror("Error");
		return NULL;
	}
	
	hash_data_pointer = fopen(f3, "r+");
	if (hash_data_pointer == NULL){
		perror("Error");
		return NULL;
	}
    
	// calculate total space;
	int file_data_size = 0;
	fseek(file_data_pointer, 0, SEEK_END);
	file_data_size = ftell(file_data_pointer);
	
	//allocate memory for sorted array of file information stored in virtual memory
	void * helper_address = init_list();
	helper_node * helper = helper_address;
	
	//Read 72 bytes until -1 is returned (i.e. end of file)
	void * tmp = malloc(72);
	int file_index = 0;
	
	int tmp_offset = 0;
	int tmp_length = 0;
	
	helper->file_data = file_data_pointer;
	helper->directory_table = directory_table_pointer;
	helper->hash_data = hash_data_pointer;
	
	int filled_space = 0;
	
	char null_byte = '\0';
	while(fread(tmp, 72, 1, directory_table_pointer) == 1){
		//skip over null-starting entries
		if (memcmp(tmp, &null_byte, 1) == 0){
			file_index++;
			continue;
		}	
		
		memcpy(&tmp_offset, tmp + 64, int_bytes);
		memcpy(&tmp_length, tmp + 68, int_bytes);
		
		// calculate filled_space
		filled_space += tmp_length;
		
		if (add_node(helper, tmp, tmp_offset, tmp_length, file_index*72) != 0){
			printf("Error adding node\n");
			return NULL;
		}
		
		file_index++;
	}
	
	// calculate total space;
	int hash_data_size = 0;
	fseek(hash_data_pointer, 0, SEEK_END);
	hash_data_size = ftell(file_data_pointer);
	
	// alloc virtual memory to hold hash_data
	void * tmp_hash = malloc(hash_data_size);
	fread(tmp_hash, hash_data_size, 1, hash_data_pointer);
	
	helper->number_of_blocks = file_data_size/256;
	helper->hash_tree = tmp_hash;
	
	helper->max_depth = (int)log2((file_data_size/256));
	
	// write file space to helper node
	helper->total_space = file_data_size;
	helper->filled_space = filled_space;
	
	// init mutex
	pthread_mutex_init(&helper->list_lock, NULL);
	
	free(tmp);
	
	return helper_address;
}

// function to close all files and free all memory
void close_fs(void * helper) {
	helper_node * node_pointer = helper;
	
	fseek(node_pointer->file_data, 0, SEEK_END);
	
	fclose(node_pointer->file_data);
	fclose(node_pointer->directory_table);
	fclose(node_pointer->hash_data);
	
	// free offset sorted linked list
	offset_node * prev_offset_node = node_pointer->offset_node;
	offset_node * next_offset_node = NULL;
	
	while (prev_offset_node->next != NULL){
		next_offset_node = prev_offset_node->next;
		free(prev_offset_node);
		prev_offset_node = next_offset_node;
	}
	free(prev_offset_node);
	free(node_pointer->hash_tree);
	free(helper);
    return;
}

// helper function for creating files
static void create_file_helper(void * helper, char * filename, size_t length, int previous_free_offset){
	void * buff = calloc(1, length);
			
	helper_node * node_pointer = helper;
			
	// create record in directory_table
	char directory_table_record[72] = {0};
	strncpy(directory_table_record, filename, 64);
	memcpy(directory_table_record + 64, &previous_free_offset, 4);
	memcpy(directory_table_record + 68, &length, 4);
		
	// write to file_data
	fseek(node_pointer->file_data, previous_free_offset, SEEK_SET);
	fwrite(buff, length, 1, node_pointer->file_data);
			
	// write to directory_table
	int file_index = find_free_file_index(helper);
	fseek(node_pointer->directory_table, file_index, SEEK_SET);
	fwrite(directory_table_record, 72, 1, node_pointer->directory_table);
			
	// flush buffers for multithreading
	fflush(node_pointer->file_data);
	fflush(node_pointer->directory_table);
	fflush(node_pointer->hash_data);			
			
	free(buff);
	add_node(helper, filename, previous_free_offset, length, file_index);
}

// function to create files
// returns 0 if file is created successfully
// returns 1 if filename already exists
// returns 2 if there is insufficient space in the virtual disk overall
int create_file(char * filename, size_t length, void * helper) {
	// search for contiguous memory space >= length
	helper_node * node_pointer = helper;
	
	pthread_mutex_lock(&(node_pointer->list_lock));
	
	truncate(filename);
	
	if (does_filename_exist(helper, filename) == 0){
		pthread_mutex_unlock(&(node_pointer->list_lock));
		return 1;
	}
	
	offset_node * offset_tmp_pointer = node_pointer->offset_node;

	if (offset_tmp_pointer->next == NULL){ // no files exist
	
		if (length <= (node_pointer->total_space - node_pointer->filled_space)){ // create first file if space available	
			create_file_helper(helper, filename, length, 0);
			calculate_hash(helper, 0, 0);
			
			// flush buffers for multithreading
			fflush(node_pointer->file_data);
			fflush(node_pointer->directory_table);
			fflush(node_pointer->hash_data);			
			
			pthread_mutex_unlock(&(node_pointer->list_lock));
			return 0;	
		}
		else{ // insufficient space in file_data
			pthread_mutex_unlock(&(node_pointer->list_lock));
			return 2;
		}
	}

	int previous_free_offset = 0;
	int contiguous_space = 0;
	
	offset_tmp_pointer = offset_tmp_pointer->next;
	contiguous_space = offset_tmp_pointer->offset - previous_free_offset;
	
	while (offset_tmp_pointer->next != NULL){
		if (length <= contiguous_space){ // add file
			create_file_helper(helper, filename, length, previous_free_offset);
			calculate_hash(helper, 0, 0);
			
			// flush buffers for multithreading
			fflush(node_pointer->file_data);
			fflush(node_pointer->directory_table);
			fflush(node_pointer->hash_data);			
			
			pthread_mutex_unlock(&(node_pointer->list_lock));
			return 0;
		}
		previous_free_offset = offset_tmp_pointer->offset + offset_tmp_pointer->length;
		offset_tmp_pointer = offset_tmp_pointer->next;
		contiguous_space = offset_tmp_pointer->offset - previous_free_offset;
	}
	
	previous_free_offset = offset_tmp_pointer->offset + offset_tmp_pointer->length;
	contiguous_space = node_pointer->total_space - previous_free_offset;
	
	if (length <= contiguous_space){ // add file
		create_file_helper(helper, filename, length, previous_free_offset);
		calculate_hash(helper, 0, 0);
		
		// flush buffers for multithreading
		fflush(node_pointer->file_data);
		fflush(node_pointer->directory_table);
		fflush(node_pointer->hash_data);
		
		pthread_mutex_unlock(&(node_pointer->list_lock));
		return 0;
	}

	// if space exists after repack
	if (node_pointer->total_space - node_pointer->filled_space >= length){
		repack_helper(helper);
		
		offset_tmp_pointer = node_pointer->offset_node;
	
		previous_free_offset = 0;
		contiguous_space = 0;
	
		offset_tmp_pointer = offset_tmp_pointer->next;
		contiguous_space = offset_tmp_pointer->offset - previous_free_offset;
	
		while (offset_tmp_pointer->next != NULL){
			if (length <= contiguous_space){ // add file
				create_file_helper(helper, filename, length, previous_free_offset);
				calculate_hash(helper, 0, 0);
				
				// flush buffers for multithreading
				fflush(node_pointer->file_data);
				fflush(node_pointer->directory_table);
				fflush(node_pointer->hash_data);
				
				pthread_mutex_unlock(&(node_pointer->list_lock));
				return 0;
			}
			previous_free_offset = offset_tmp_pointer->offset + offset_tmp_pointer->length;
			offset_tmp_pointer = offset_tmp_pointer->next;
			contiguous_space = offset_tmp_pointer->offset - previous_free_offset;
		}
	
		previous_free_offset = offset_tmp_pointer->offset + offset_tmp_pointer->length;
		contiguous_space = node_pointer->total_space - previous_free_offset;
	
		if (length <= contiguous_space){ // add file
			create_file_helper(helper, filename, length, previous_free_offset);
			calculate_hash(helper, 0, 0);
			
			// flush buffers for multithreading
			fflush(node_pointer->file_data);
			fflush(node_pointer->directory_table);
			fflush(node_pointer->hash_data);
			
			pthread_mutex_unlock(&(node_pointer->list_lock));
			return 0;
		}
	}
	else{
		pthread_mutex_unlock(&(node_pointer->list_lock));
		return 2;
	}
	
	// should never reach here
	pthread_mutex_unlock(&(node_pointer->list_lock));
	return -1;
}

// helper method for resizing file
// returns 1 if file does not exist
// returns 2 if not enough space in file system
// returns 0 if file resized successfully
static int resize_file_helper(char * filename, size_t length, void * helper){

	helper_node * node_pointer = helper;
	
	offset_node * offset_tmp_node = get_offset_node(helper, filename);
	
	if (offset_tmp_node == NULL){ // file does not exist
		return 1;
	}
	
	if (node_pointer->filled_space - offset_tmp_node->length + length > node_pointer->total_space){ // if not enough space in disk
		return 2;
	}
	
	offset_node * offset_next_node = offset_tmp_node->next;
	if (offset_next_node == NULL){ // file is at last offset
		if (offset_tmp_node->offset + length <= node_pointer->total_space){ // space exists
			if (length > offset_tmp_node->length){ // pad with zeros
				int num_bytes = length - offset_tmp_node->length;
			
				node_pointer->filled_space += length - offset_tmp_node->length;
		
				// update file_data file
				void * buffer = calloc(1, num_bytes);
				fseek(node_pointer->file_data, offset_tmp_node->offset + offset_tmp_node->length, SEEK_SET);
				fwrite(buffer, num_bytes, 1, node_pointer->file_data);
			
				offset_tmp_node->length = length;
			
				//update directory_table
				fseek(node_pointer->directory_table, offset_tmp_node->file_index + 68, SEEK_SET);
				fwrite(&length, 4, 1, node_pointer->directory_table);
				free(buffer);
				// flush buffers for multithreading
				fflush(node_pointer->file_data);
				fflush(node_pointer->directory_table);
				fflush(node_pointer->hash_data);

				return 0;
			}
			else{ // truncate file
				//update directory_table
				
				node_pointer->filled_space += length - offset_tmp_node->length;
				
				fseek(node_pointer->directory_table, offset_tmp_node->file_index + 68, SEEK_SET);
				fwrite(&length, 4, 1, node_pointer->directory_table);
				
				offset_tmp_node->length = length;
				
				fflush(node_pointer->directory_table);
				return 0;
			}
		}
	}
	
	if (offset_next_node != NULL && (offset_next_node->offset - offset_tmp_node->offset) >= length){ // space exists so just resize
		if (length > offset_tmp_node->length){ // pad with zeros
			int num_bytes = length - offset_tmp_node->length;
		
			node_pointer->filled_space += length - offset_tmp_node->length;
		
			// update file_data file
			void * buffer = calloc(1, num_bytes);
			fseek(node_pointer->file_data, offset_tmp_node->offset + offset_tmp_node->length, SEEK_SET);
			fwrite(buffer, num_bytes, 1, node_pointer->file_data);
			
			//update directory_table
			fseek(node_pointer->directory_table, offset_tmp_node->file_index + 68, SEEK_SET);
			fwrite(&length, 4, 1, node_pointer->directory_table);
			
			offset_tmp_node->length = length;
			
			free(buffer);
			fflush(node_pointer->directory_table);
			return 0;
		}
		else{ // truncate file
			//update directory_table
			
			node_pointer->filled_space += length - offset_tmp_node->length;
			
			fseek(node_pointer->directory_table, offset_tmp_node->file_index + 68, SEEK_SET);
			fwrite(&length, 4, 1, node_pointer->directory_table);
			fflush(node_pointer->directory_table);
			
			offset_tmp_node->length = length;
			
			return 0;
		}
	}

	// take node out of list
	
	int original_size = offset_tmp_node->length;
	//assuming we keep the file index the same
	int original_file_index = offset_tmp_node->file_index;
	
	void * file_data_buffer = malloc(original_size);
	fseek(node_pointer->file_data, offset_tmp_node->offset, SEEK_SET);
	fread(file_data_buffer, original_size, 1, node_pointer->file_data);
	
	delete_file_helper(filename, helper);
	
	//repack
	repack_helper(helper);
	
	offset_tmp_node = node_pointer->offset_node;
	while (offset_tmp_node->next != NULL){
		offset_tmp_node = offset_tmp_node->next;
	}
	
	int new_offset = offset_tmp_node->offset + offset_tmp_node->length;

	if (length > original_size){
		// pad the memory with 0s
		void * new_buffer = calloc(1, length);
		memcpy(new_buffer, file_data_buffer, original_size);
		free(file_data_buffer);
		file_data_buffer = new_buffer;		
	}
	
	// add the node
	add_node(helper, filename, new_offset, length, original_file_index);
	
	// add the file data
	fseek(node_pointer->file_data, new_offset, SEEK_SET);
	fwrite(file_data_buffer, length, 1, node_pointer->file_data);
			
	//update directory_table
	fseek(node_pointer->directory_table, original_file_index, SEEK_SET);
	fwrite(filename, strnlen(filename, 64), 1, node_pointer->directory_table);
	
	fseek(node_pointer->directory_table, original_file_index + 64, SEEK_SET);
	fwrite(&new_offset, 4, 1, node_pointer->directory_table);	
	
	fseek(node_pointer->directory_table, original_file_index + 68, SEEK_SET);
	fwrite(&length, 4, 1, node_pointer->directory_table);
	
	
	free(file_data_buffer);	
    return 0;
}

// function to resize a file
// returns 0 if file is successfully resized
// returns 1 if the file does not exist
// returns 2 if there is insufficient space in the virtual disk overall for the new file size
int resize_file(char * filename, size_t length, void * helper) {
	truncate(filename);
	helper_node * node_pointer = helper;
	
	pthread_mutex_lock(&(node_pointer->list_lock));
	int return_value = resize_file_helper(filename, length, helper);
	calculate_hash(helper, 0, 0);
	
	// flush buffers for multithreading
	fflush(node_pointer->file_data);
	fflush(node_pointer->directory_table);
	fflush(node_pointer->hash_data);
	
	pthread_mutex_unlock(&(node_pointer->list_lock));
	return return_value;
};

// function to repack the files in the file system
void repack(void * helper) {
	helper_node * node_pointer = helper;
	pthread_mutex_lock(&(node_pointer->list_lock));
	repack_helper(helper);
	calculate_hash(helper, 0, 0);
	
	// flush buffers for multithreading
	fflush(node_pointer->file_data);
	fflush(node_pointer->directory_table);
	fflush(node_pointer->hash_data);
	
	pthread_mutex_unlock(&(node_pointer->list_lock));
	return;
}

// function to delete files from file system
// returns 0 if file is susccessfull deleted
// returns 1 if error occurs, such as file not existing
int delete_file(char * filename, void * helper) {
	truncate(filename);
	
	helper_node * node_pointer = helper;
	pthread_mutex_lock(&(node_pointer->list_lock));
	int return_value = delete_file_helper(filename, helper);
	
	// flush buffers for multithreading
	fflush(node_pointer->file_data);
	fflush(node_pointer->directory_table);
	fflush(node_pointer->hash_data);	
	
	pthread_mutex_unlock(&(node_pointer->list_lock));
	return return_value;
	
}

// function to rename a file
// returns 0 if file is successfully renamed
// returns 1 if error occurs, such as file not existing
int rename_file(char * oldname, char * newname, void * helper) {
	helper_node * node_pointer = helper;
	pthread_mutex_lock(&(node_pointer->list_lock));
	truncate(newname);
	int newname_length = strlen(newname) + 1;
	
	if (does_filename_exist(helper, newname) == 0){ //if the newname already exists
		pthread_mutex_unlock(&(node_pointer->list_lock));
		return 1;
	}
	
	if (does_filename_exist(helper, oldname) != 0){ //if the oldname doesn't exist
		pthread_mutex_unlock(&(node_pointer->list_lock));
		return 1;
	}
	
	// thread this plz its gonna be so slow
	offset_node * tmp_offset_node = get_offset_node(helper, oldname);
	
	if (tmp_offset_node == NULL){
		pthread_mutex_unlock(&(node_pointer->list_lock));
		return 1;
	}
	
	strncpy(tmp_offset_node->filename, newname, 64);
	fseek(node_pointer->directory_table, tmp_offset_node->file_index, SEEK_SET);
	fwrite(newname, newname_length, 1, node_pointer->directory_table);
	
	// flush buffers for multithreading
	fflush(node_pointer->file_data);	
	fflush(node_pointer->directory_table);
	fflush(node_pointer->hash_data);
	
    pthread_mutex_unlock(&(node_pointer->list_lock));
	return 0;
}

// function to read file data into buffer
// returns 0 if successfully completed
// returns 1 if file does not exist
// returns 2 if the provided offset makes it impossible to read count bytes given the file size
// returns 3 if hash verification fails
int read_file(char * filename, size_t offset, size_t count, void * buf, void * helper) {
	
	helper_node * node_pointer = helper;
	pthread_mutex_lock(&(node_pointer->list_lock));
    truncate(filename);	
	
	offset_node * tmp = get_offset_node(helper, filename);
	if (tmp != NULL){
		
		int start_block = floor((tmp->offset)/256);
		int end_block = floor((tmp->offset + tmp->length)/256);
		int hash_fails = 0;
		
		for (int i = start_block; i <= end_block; i++){
			hash_fails += verify_hash_block(i, helper);
		}
		
		if (hash_fails != 0){
			return 3;
		}
		
		 if ((offset + count) > tmp->length){
			pthread_mutex_unlock(&(node_pointer->list_lock));
			return 2;
		 }
		 else{
			 fseek(node_pointer->file_data, ((tmp->offset) + offset), SEEK_SET);
			 fread(buf, 1, count, node_pointer->file_data);
			 
			// flush buffers for multithreading
			fflush(node_pointer->file_data);
			fflush(node_pointer->directory_table);
			fflush(node_pointer->hash_data);
			 
			 pthread_mutex_unlock(&(node_pointer->list_lock));
			 return 0;
		 }
	}
	else{
		pthread_mutex_unlock(&(node_pointer->list_lock));
		return 1;
	}
}

// function to write to file
// returns 0 if file is successfully written to
// returns 1 if file does not exist
// returns 2 if offset is greater than the current size of the file
// returns 3 if insufficient space exists in the virtual disk overall
int write_file(char * filename, size_t offset, size_t count, void * buf, void * helper) {
	helper_node * node_pointer = helper;
	pthread_mutex_lock(&(node_pointer->list_lock));

	offset_node * tmp_offset_node = get_offset_node(helper, filename);
	if (tmp_offset_node != NULL) { //node exists
	
		if (offset > tmp_offset_node->length){
			pthread_mutex_unlock(&(node_pointer->list_lock));
			return 2;
		}
	
		if ((tmp_offset_node->offset + offset + count) > (tmp_offset_node->offset + tmp_offset_node->length)){ // need to resize
			int resized = resize_file_helper(filename, (offset + count), helper);
			if (resized == 2){
				pthread_mutex_unlock(&(node_pointer->list_lock));
				return 3;
			}
			else{
				tmp_offset_node = get_offset_node(helper, filename);
				fseek(node_pointer->file_data, (tmp_offset_node->offset + offset), SEEK_SET);
				fwrite(buf, count, 1, node_pointer->file_data);
				
				fseek(node_pointer->file_data, (tmp_offset_node->offset + offset), SEEK_SET);
				calculate_hash(helper, 0, 0);
				pthread_mutex_unlock(&(node_pointer->list_lock));
				
				// flush buffers for multithreading
				fflush(node_pointer->file_data);
				fflush(node_pointer->directory_table);
				fflush(node_pointer->hash_data);
				
				/**
				int buffer;
				fseek(node_pointer->directory_table, 140, SEEK_SET);
				fread(&buffer, sizeof(int), 1, node_pointer->directory_table);
				printf("%d ", buffer);**/
				return 0;
			}
		}
		else{ //don't need to resize
			fseek(node_pointer->file_data, (tmp_offset_node->offset + offset), SEEK_SET);
			fwrite(buf, count, 1, node_pointer->file_data);
			calculate_hash(helper, 0, 0);
			pthread_mutex_unlock(&(node_pointer->list_lock));
			
			// flush buffers for multithreading
			fflush(node_pointer->file_data);
			fflush(node_pointer->directory_table);
			fflush(node_pointer->hash_data);
			return 0;
		}
	}
	else{ //file doesn't exist
		pthread_mutex_unlock(&(node_pointer->list_lock));
		return 1;
	}
}

// returns file size of the file with the given filename
// returns -1 if there is an error, such as the file not existing
ssize_t file_size(char * filename, void * helper) {
	helper_node * node_pointer = helper;
	pthread_mutex_lock(&(node_pointer->list_lock));
	
	truncate(filename);
	offset_node * tmp = get_offset_node(helper, filename);
	if (tmp != NULL){
		pthread_mutex_unlock(&(node_pointer->list_lock));
		return(tmp->length);
	}
	else{
		pthread_mutex_unlock(&(node_pointer->list_lock));
		return -1;
	}
}

// function to calculate fletcher hash of given buffer
// outputs exactly 16 bytes to output
void fletcher(uint8_t * buf, size_t length, uint8_t * output) {
	
	int overflow = length % 4;
	int length_4_bytes;
	
	uint32_t * data;
	
	if (overflow != 0){
		data = calloc(length + 4 - overflow, 1);
		length_4_bytes = (length + 4 - overflow)/4;
	}
	else{
		data = calloc(length, 1);
		length_4_bytes = length/4;
	}
	
	memcpy(data, buf, length);
	
	uint64_t a = 0;
	uint64_t b = 0;
	uint64_t c = 0;
	uint64_t d = 0;
	
	for (int i = 0; i < length_4_bytes; i++){
		a = (a + data[i])%((uint64_t)(pow(2, 32)-1));
		b = (b + a)%((uint64_t)(pow(2, 32)-1));
		c = (c + b)%((uint64_t)(pow(2, 32)-1));
		d = (d + c)%((uint64_t)(pow(2, 32)-1));
	}
	
	memcpy(output, &a, 4);
	memcpy(output+4, &b, 4);
	memcpy(output+8, &c, 4);
	memcpy(output+12, &d, 4);
	
	free(data);
	
    return;
}

// recursive helper function to calculate hash block which traverses up the hash tree
static int calculate_hash_block_rec(void * helper, int offset, int depth){
	helper_node * node_pointer = helper;
	
	// if only one node, just calculate, no recursion
	if (node_pointer->number_of_blocks == 1){
			// read in data from block in file_data and calculate fletcher
			void * tmp_file_data = malloc(256);
			int start_offset = (pow(2, node_pointer->max_depth + 1) - 1) - node_pointer->number_of_blocks;
			int file_data_offset = (offset - start_offset) * 256;
		
			fseek(node_pointer->file_data, file_data_offset, SEEK_SET);
			fread(tmp_file_data, 256, 1, node_pointer->file_data);
		
			fletcher(tmp_file_data, 256, node_pointer->hash_tree + (offset * 16));
	
			fseek(node_pointer->hash_data, offset * 16, SEEK_SET);
			fwrite(node_pointer->hash_tree + (offset * 16), 16, 1, node_pointer->hash_data);
		
			// flush buffers for multithreading
			fflush(node_pointer->hash_data);
		
			free(tmp_file_data);
			return 0;
		}
	
	if (depth == 0){
		// perform final fletcher
		fletcher(node_pointer->hash_tree + 16, 32, node_pointer->hash_tree);
		fseek(node_pointer->hash_data, 0, SEEK_SET);
		fwrite(node_pointer->hash_tree, 16, 1, node_pointer->hash_data);
		// flush buffers for multithreading
		fflush(node_pointer->hash_data);
		return 0;
	}
	
	else if (depth == node_pointer->max_depth){
		// read in data from block in file_data and calculate fletcher
		void * tmp_file_data = malloc(256);
		int start_offset = (pow(2, node_pointer->max_depth + 1) - 1) - node_pointer->number_of_blocks;
		int file_data_offset = (offset - start_offset) * 256;
		
		fseek(node_pointer->file_data, file_data_offset, SEEK_SET);
		fread(tmp_file_data, 256, 1, node_pointer->file_data);
		
		fletcher(tmp_file_data, 256, node_pointer->hash_tree + (offset * 16));
	
		fseek(node_pointer->hash_data, offset * 16, SEEK_SET);
		fwrite(node_pointer->hash_tree + (offset * 16), 16, 1, node_pointer->hash_data);
		// flush buffers for multithreading
		fflush(node_pointer->hash_data);
		free(tmp_file_data);
		calculate_hash_block_rec(helper, (int)((offset-1)/2), depth-1);
		return 0;
	}
	else{
		//fletcher block n + n+1
		// then go up
		fletcher(node_pointer->hash_tree + (16 * ((offset * 2) + 1)), 32, node_pointer->hash_tree + (offset * 16));
		
		// update hash_data file
		fseek(node_pointer->hash_data, offset * 16, SEEK_SET);
		fwrite(node_pointer->hash_tree + (offset * 16), 16, 1, node_pointer->hash_data);
		// flush buffers for multithreading
		fflush(node_pointer->hash_data);
		calculate_hash_block_rec(helper, (int)((offset-1)/2), depth-1);
	}
	return 0;
}

// function to calculate the hashes for a given block offset and update all affected hashes in the Merkle hash tree
void compute_hash_block(size_t block_offset, void * helper) {
	helper_node * node_pointer = helper;
	pthread_mutex_lock(&node_pointer->list_lock);
	int start_offset = (pow(2, node_pointer->max_depth + 1) - 1) - node_pointer->number_of_blocks;
	calculate_hash_block_rec(helper, start_offset + block_offset, node_pointer->max_depth);
	pthread_mutex_unlock(&node_pointer->list_lock);
	
    return;
}

