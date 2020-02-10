#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TEST(x) test(x, #x)
#include "myfilesystem.h"

// code modified from https://www.geeksforgeeks.org/c-program-compare-two-files-report-mismatches/
static int compareFiles(FILE *fp1, FILE *fp2) 
{ 
    // fetching character of two file 
    // in two variable ch1 and ch2 
    char ch1 = getc(fp1); 
    char ch2 = getc(fp2); 
  
    // error keeps track of number of errors 
    int error = 0;
  
    // iterate loop till end of file 
    while (ch1 != EOF && ch2 != EOF) 
    { 
  
        // if fetched data is not equal then 
        // error is incremented 
        if (ch1 != ch2) 
        { 
            error++; 
        } 
  
        // fetching character until end of file 
        ch1 = getc(fp1); 
        ch2 = getc(fp2); 
    } 
  
    return error;
} 

/* You are free to modify any part of this file. The only requirement is that when it is run, all your tests are automatically executed */

/* Some example unit test functions */
int success() {
    return 0;
}

int failure() {
    return 1;
}

int no_operation() {
    void * helper = init_fs("file_data1.bin", "directory_table1.bin", "hash_data1.bin", 1); // Remember you need to provide your own test files and also check their contents as part of testing
    close_fs(helper);
    return 0;
}

int create_file_test() {
	int return_value = 0;
	void * helper = init_fs("file_data1.bin", "directory_table1.bin", "hash_data1.bin", 1);
	return_value += create_file("newfile1", 10, helper);
	return_value += create_file("newfile2", 100, helper);
	return_value += create_file("newfile3", 300, helper);
	if (create_file("newfile1", 1, helper) != 1){	// test filename already exists
		return_value ++;
	}
	if (create_file("newfile4", 300, helper) != 2){ // test not enough space in disk
		return_value++;
	}
	if (create_file("newfile4", 101, helper) != 2){ // test not enough space in disk (edge case)
		return_value++;
	}
	return_value += create_file("newfile4", 100, helper); // test edge case with just enough space (edge case)
	close_fs(helper);
	return return_value;
}

int delete_file_test(){
	int return_value = 0;
	void * helper = init_fs("file_data1.bin", "directory_table1.bin", "hash_data1.bin", 1);
	return_value += delete_file("file1", helper);
	return_value += delete_file("file2", helper);
	
	if (delete_file("file1", helper) != 1){ // test filename used to exist
		return_value++;
	}
	
	if (delete_file("nonexistant", helper) != 1){ // test filename never existed
		return_value++;
	}
	
	close_fs(helper);
	return return_value;
	
}

int resize_file_test(){
	int return_value = 0;
	void * helper = init_fs("file_data2.bin", "directory_table2.bin", "hash_data2.bin", 1);
	return_value += resize_file("file1", 100, helper);
	return_value += resize_file("file2", 400, helper);
	if (resize_file("file3", 10, helper) != 1){ // test filename doesn't exist
		return_value ++;
	}
	if (resize_file("file1", 113, helper) != 2) { // test edge case for not enough file space
		return_value++;
	}
	return_value += resize_file("file1", 112, helper); // test edge case for just enough space
	close_fs(helper);
	return return_value;
}

int file_size_test(){
	int return_value = 0;
	void * helper = init_fs("file_data2.bin", "directory_table2.bin", "hash_data2.bin", 1);
	if (file_size("file2", helper) != 400){
		return_value ++;
	}
	if (file_size("file1", helper) != 112){
		return_value ++;
	}
	if (file_size("nonexistant", helper) != -1){ //test filename doesn't exist
		return_value ++;
	}
	
	close_fs(helper);
	return return_value;
}

int rename_file_test(){
	int return_value = 0;
	void * helper = init_fs("file_data1.bin", "directory_table1.bin", "hash_data1.bin", 1);
	return_value += rename_file("newfile1", "evennewerfile1", helper);
	return_value += rename_file("newfile2", "evennewerfile2", helper);
	
	if (rename_file("newfile1", "nonamehere", helper) != 1){ // test for old filename
		return_value ++;
	}
	if (rename_file("nonexistant", "existant", helper) != 1){ // test for file that never existed
		return_value ++;
	}
	close_fs(helper);
	return return_value;
}

int repack_test(){
	int return_value = 0;
	void * helper = init_fs("file_data3.bin", "directory_table3.bin", "hash_data3.bin", 1);
	repack(helper);
	FILE * file_data = fopen("file_data3.bin", "r");
	FILE * directory_table = fopen("directory_table3.bin", "r");
	FILE * hash_data = fopen("hash_data3.bin", "r");
	
	FILE * correct_file_data = fopen("file_data4.bin", "r");
	FILE * correct_directory_table = fopen("directory_table4.bin", "r");
	FILE * correct_hash_data = fopen("hash_data4.bin", "r");
	
	return_value += compareFiles(file_data, correct_file_data);
	return_value += compareFiles(directory_table, correct_directory_table);
	return_value += compareFiles(hash_data, correct_hash_data);
	close_fs(helper);
	fclose(file_data);
	fclose(directory_table);
	fclose(hash_data);
	fclose(correct_file_data);
	fclose(correct_directory_table);
	fclose(correct_hash_data);
	return return_value;
}

int read_file_test(){
	void * helper = init_fs("file_data5.bin", "directory_table5.bin", "hash_data5.bin", 1);
	int return_value = 0;
	void * buffer1 = malloc(10);
	void * buffer2 = malloc(10);
	
	compute_hash_tree(helper);
	
	read_file("file1", 0, 5, buffer1, helper);
	read_file("file6", 0, 4, buffer2, helper);
	
	return_value += memcmp(buffer1, "pizza", 5);
	return_value += memcmp(buffer2, "time", 4);
	
	free(buffer1);
	free(buffer2);
	close_fs(helper);
	
	return return_value;
	
}

int  write_file_test(){
	void * helper = init_fs("file_data5.bin", "directory_table5.bin", "hash_data5.bin", 1);
	int return_value = 0;
	void * buffer1 = malloc(10);
	void * buffer2 = malloc(10);
	
	compute_hash_tree(helper);
	
	write_file("file1", 0, 5, "pasta", helper);
	write_file("file6", 0, 4, "mime", helper);
	
	read_file("file1", 0, 5, buffer1, helper);
	read_file("file6", 0, 4, buffer2, helper);
	
	return_value += memcmp(buffer1, "pasta", 5);
	return_value += memcmp(buffer2, "mime", 4);
	
	free(buffer1);
	free(buffer2);
	close_fs(helper);
	
	return return_value;
}

int  compute_hash_tree_test(){
	int return_value = 0;
	void * helper = init_fs("file_data6.bin", "directory_table6.bin", "hash_data6.bin", 1);
	compute_hash_tree(helper);
	FILE * hash_data = fopen("hash_data6.bin", "r");
	
	FILE * correct_hash_data = fopen("hash_data6-1.bin", "r");
	
	return_value += compareFiles(hash_data, correct_hash_data);
	close_fs(helper);
	fclose(hash_data);
	fclose(correct_hash_data);
	return return_value;	
}

int compute_hash_block_test(){
	int return_value = 0;
	void * helper = init_fs("file_data6.bin", "directory_table6.bin", "hash_data6-3.bin", 1);
	compute_hash_block(0, helper);
	FILE * hash_data = fopen("hash_data6.bin", "r");
	
	FILE * correct_hash_data = fopen("hash_data6-1.bin", "r");
	
	return_value += compareFiles(hash_data, correct_hash_data);
	close_fs(helper);
	fclose(hash_data);
	fclose(correct_hash_data);
	return return_value;
}

int fletcher_test(){
	int return_value = 0;
	void * helper = init_fs("file_data7.bin", "directory_table7.bin", "hash_data7.bin", 1);
	uint8_t numbers[1024] = {1};
	uint8_t * buffer = malloc(16);
	fletcher(numbers, 1024, buffer);
	int correct_num = 1;
	
	return_value += memcmp(buffer, &correct_num, 4);
	free(buffer);
	close_fs(helper);
	return return_value;
}

/****************************/

/* Helper function */
void test(int (*test_function) (), char * function_name) {
    int ret = test_function();
    if (ret == 0) {
        printf("Passed %s\n", function_name);
    } else {
        printf("Failed %s returned %d\n", function_name, ret);
    }
}
/************************/

int main(int argc, char * argv[]) {
    
    // You can use the TEST macro as TEST(x) to run a test function named "x"
    TEST(success);
    TEST(failure);
    TEST(no_operation);
	TEST(create_file_test);
	TEST(resize_file_test);
	TEST(delete_file_test);
	TEST(file_size_test);
	TEST(rename_file_test);
	TEST(repack_test);
	TEST(read_file_test);
	TEST(write_file_test);
	TEST(compute_hash_tree_test);
	TEST(compute_hash_block_test);
	TEST(fletcher_test);
    // Add more tests here

    return 0;
}