# OS选做实验Map_Reduce报告

## 一、实验目标

本实验需要在Linux环境下构建一个简单的mapreduce框架，并且给出了一个简单的wordcount.c程序，需要使用该框架进行词频统计，从而验证框架正确性。

## 二、实验思路

一个 MapReduce 作业通常会把输入的数据集切分为若干独立的数据块，由 Map 任务以完全并行的方式去处理它们

### 1.MapReduce 框架组成

MapReduce的基本原理包括两个阶段：Map和Reduce。

1、Map阶段

Map阶段的作用是将原始输入数据分解成一组键值对，以便后续的处理。在Map阶段中，开发者需要定义一个Map函数来完成具体的数据处理工作。Map函数的输入参数是一组键值对，包括输入数据的键和值。Map函数的输出结果也是一组键值对，其中键是经过处理后的值，而值则是与该键相关的计数器。

2、Reduce阶段

Reduce阶段的作用是将Map阶段输出的大量中间结果进行归并，得到最终的输出结果。在Reduce阶段中，开发者需要定义一个Reduce函数来完成具体的数据处理工作。Reduce函数的输入参数是一组键值对，其中键是之前Map阶段输出的键值对中的键，而值则是之前Map阶段输出的键值对中与该键相关的计数器。Reduce函数的输出结果可以是任何类型的数据，用于满足特定的业务需求。

![image-20240519172751178](C:\Users\李宗辉\AppData\Roaming\Typora\typora-user-images\image-20240519172751178.png)

![image-20240519172758021](C:\Users\李宗辉\AppData\Roaming\Typora\typora-user-images\image-20240519172758021.png)

### 2.已知文件：mapreduce.h，wordcount.c

根据给的mapreduce.h文件，可知需要实现给定函数即可。

根据MapReduce框架的结构，可知主要有三个部分：Map、Partition、Reduce。

其中Map主要接受输入并且形成键值对；

PartitionPartition会接受Map的输入，将键插入所得的分区，然后每个分区会对键进行排序。

Reduce对每个分区进行遍历，并对分区包含的值进行相加，得到词频。

```c
//mapreduce.h
#ifndef __mapreduce_h__
#define __mapreduce_h__

// Different function pointer types used by MR
typedef char* (*Getter)(char* key, int partition_number);
typedef void (*Mapper)(char* file_name);
typedef void (*Reducer)(char* key, Getter get_func, int partition_number);
typedef unsigned long (*Partitioner)(char* key, int num_partitions);

// External functions: these are what you must define
void MR_Emit(char* key, char* value);

unsigned long MR_DefaultHashPartition(char* key, int num_partitions);

void MR_Run(int argc, char* argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition);

#endif  // __mapreduce_h__#pragma once

```

```c
//wordcount.c
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mapreduce.h"

void Map(char *file_name) {
    FILE *fp = fopen(file_name, "r");
    assert(fp != NULL);

    char *line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1) {
        char *token, *dummy = line;
        while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
            MR_Emit(token, "1");
        }
    }
    free(line);
    fclose(fp);
}

void Reduce(char *key, Getter get_next, int partition_number) {
    int count = 0;
    char *value;
    while ((value = get_next(key, partition_number)) != NULL)
        count++;
    printf("%s %d\n", key, count);
}

int main(int argc, char *argv[]) {
    MR_Run(argc, argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
}
```

由于因为wordcount.c已经给出了Map function和Reduce function，可知需要完成的的函数有：

Getter类型的应用在Reduce的get_next()函数；

unsigned long MR_DefaultHashPartition(char* key, int num_partitions);

void MR_Emit(char* key, char* value);

void MR_Run(int argc, char* argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition);



MR_DefaultHashPartition函数采用Readme文件中给的默认函数，这个函数主要用于键分区，并且返回分区的下标，需要注意的是，同一key是会被分到同一分区中的。

```c
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}
```

### 3.考虑适合的数据结构

由于需要内置的数据结构进行从mapper到partition的传递键值对（相关参数），容易考虑到使用链表，用链表结构，可以动态地添加和删除数据节点，从而灵活地处理不同数量和大小的数据，在这个框架下使用链表可以较为容易地对键和值进行存取。

又考虑到对于分区需要遍历，对于分区中的键需要遍历，对于每个键对应的值也需要构建链表，也就是说，分区是通过哈希函数返回的下标访问，而每个分区中有key的链表结构，包含分到该分区的所有key，而每一个key节点上又有一个value的链表结构，表示对不同文件进行map后发送到分区的结果。

构建基本结构如下：

```c
struct data_node_t {		//值的结构
    int proceed;			//表示是否被计算过，便于遍历
    char* value;
    struct data_node_t* next;
};

struct info_node_t {		//键的结构
    int proceed;			//表示是否被计算过，便于遍历
    char* info;
    struct data_node_t* data;//每个键对应的值，可能有很多mapper，导致一个键有多个值
    struct info_node_t* next;
};

struct partition_t {
    struct info_node_t* info_head;//指向键头
};
```

对于上述结构体，还需要建立对应的插入键和值的函数：

```c
void insert_info(struct partition_t* part, char* key) {   //插入新键   管理每个分区中的信息节点链表
    struct info_node_t* new_info = (struct info_node_t*)malloc(sizeof(struct info_node_t));
	if (new_info == NULL) {
        // 处理内存分配失败的情况
        printf("Memory allocation failed ");
        return;
    }

    new_info->info = (char*)malloc(sizeof(char) * (strlen(key) + 1));
    new_info->proceed = 0;
    new_info->data = NULL;                  //分配内存空间+初始化
    new_info->next = NULL;
    
    strcpy(new_info->info, key);            //拷贝新键的数据
    new_info->next = part->info_head;       //新的信息节点插入到当前分区的信息节点链表的
    part->info_head = new_info;             //新节点变成头部
}

void insert_data(struct info_node_t* info, char* value) {   //根据键的结点上插入新值    管理每个信息节点中的数据节点链表
    struct data_node_t* new_node = (struct data_node_t*)malloc(sizeof(struct data_node_t));
    if (new_node == NULL) {
        // 处理内存分配失败的情况
        printf("Memory allocation failed ");
        return;
    }
    new_node->value = (char*)malloc(sizeof(char) * (strlen(value) + 1));
    new_node->proceed = 0;
    strcpy(new_node->value, value);
    new_node->next = info->data;
    info->data = new_node;           //将新的数据节点插入到信息节点的数据节点链表的头部。
}
```

### 4.基本的变量设置

```c
pthread_mutex_t* partition_locks;
pthread_t* pthreads;   //0代表可以使用 available
struct partition_t* partitions;
int num_partitions;
Mapper mapper;		//函数指针，指向 Map 函数，调用会执行函数
Reducer reducer;	//函数指针，执行Reduce函数，调用会执行函数
//mapper传参 便于线程创建初始化参数
struct mapper_arg {
	int id;		//线程id
	char* arg;	//wordcount中主要是txt文本名
};
//reducer传参 便于线程创建初始化参数
struct reducer_arg {
	int id;		//线程id
	char* key;
	int partition_id;
};
```

1. `partition_locks`：指向互斥锁数组的指针，用于对每个分区进行加锁操作，防止多线程同时访问某分区。
2. `partitions`：指向分区数组的指针，每个分区包含一个指向info（键）节点链表的指针。在 MapReduce 中，每个分区存储了归约函数Reducer需要的数据信息。
3. `num_partitions`：分区的数量，用于确定需要创建多少个互斥锁和分区。
4. `pthreads`：指向线程数组的指针，用于存储每个线程的信息。0表示该线程可用，1表示该线程被使用。
5. `mapper` 和 `reducer`：指向映射函数和归约函数的指针。
6. `struct mapper_arg` 和 `struct reducer_arg `：这两个结构体定义了传递给映射函数和归约函数的参数。`mapper_arg` 结构体包含了线程的 ID 和映射函数的参数，而 `reducer_arg` 结构体包含了线程的 ID、键值对的键以及分区的 ID。

### 5.MR_Emit(char* key, char* value)

```c
void MR_Emit(char* key, char* value) {
	unsigned long partition_index = MR_DefaultHashPartition(key, num_partitions);
	pthread_mutex_lock(&partition_locks[partition_index]);
	struct info_node_t* info_ptr = partitions[partition_index].info_head;
	while(info_ptr!=NULL) {
		if (strcmp(info_ptr->info, key) == 0) {
			insert_data(info_ptr, value);
			pthread_mutex_unlock(&partition_locks[partition_index]);
			return;
		}
        info_ptr=info_ptr->next;
	}
	insert_info(&partitions[partition_index], key);
	insert_data(partitions[partition_index].info_head, value);
	pthread_mutex_unlock(&partition_locks[partition_index]);
} 
```

此函数主要是将统计到的key发送到对应到分区中，并且赋值value。

逻辑如下：先用默认的哈希分区函数得到分区的下标；然后使用互斥锁访问该分区；对于该分区，如果能找到对应的key，就把这个value值插入key中，如果找不到对应的key，就在此分区中重新插入新的key，并在key上插入value。

### 6.get_next()函数

```c
char* MR_getnext(char* key, int partition_number) {
	struct info_node_t* info_ptr = partitions[partition_number].info_head;
	 while (info_ptr != NULL) {
		if (strcmp(info_ptr->info, key) == 0) {					//找到分区是否有对应的key键，
			if (info_ptr->proceed == 1)							//该键是否被处理过
				return NULL;
			struct data_node_t* data_ptr = info_ptr->data;
			while (data_ptr != NULL) {		
				if (data_ptr->proceed == 0) {
					data_ptr->proceed = 1;
					return data_ptr->value;
				}
                data_ptr = data_ptr->next;
			}
			info_ptr->proceed = 1;
			return NULL;
		}
         info_ptr = info_ptr->next;
	}
	return NULL;
}
```

get_next()函数接受两个参数，一个是key，一个是分区下标。逻辑为先根据给定的分区下标找到分区，然后有处理以下情况：

1.没找到传递的key，就返回NULL也就是无法找到下一个；如果按照键的链表找到了key，有一个标志标记该key是否被处理过了，处理过则返回NULL；没处理过就查找键值中的值，如果被处理过，就接着找下一个值；如果该值没被处理过，修改处理位后返回该值，这样wordcount.c中的Reduce函数中get_next不会为NULL，会统计次数；当所有的值都被处理过后（循环结束后），将key的处理位设置为1，表示该分区该key被统计过了，然后返回NULL，Reduce函数中get_next为空，while循环结束，统计结束。

### 7.线程创建的执行函数

```c
void* MR_Mapper(void* arg) {
	struct mapper_arg* pass_arg = (struct mapper_arg*)arg; //arg：包含了传递给 Mapper 函数的参数，wordcount中主要是txt文本名
	mapper(pass_arg->arg);
	pthreads[pass_arg->id] = 0;
	free(pass_arg);
	return NULL;
}
void* MR_Reducer(void* arg) {
	struct reducer_arg* pass_arg = (struct reducer_arg*)arg;
	reducer(pass_arg->key, MR_getnext, pass_arg->partition_id);
	pthreads[pass_arg->id] = 0;
	free(pass_arg);
	return NULL;
}
```

这两个函数是在MR_Run函数中（即调用mapreduce框架时），map和reduce过程中各自创建线程的执行函数，map过程中传递包括文件名、线程标识符参数，通过函数指针mapper指向wordcount.c中的函数Map，即传递参数txt文件名到该函数并且执行，完成后销毁；reduce过程中传递key，线程标识符，分区标识符参数，通过函数指针reducer指向wordcount.c中的函数Reduce，即传递参数key、get_next函数和分区id到该函数并且执行，完成后销毁。

### 8.key排序函数

本实验有要求：对于每个分区，键（以及与这些键关联的值列表）应按键的升序排序；因此，当特定的Reduce线程（及其关联的分区）在工作时，应按顺序对该分区的每个键调用 Reduce() 函数。

所以需要一个keu排序函数。这里用了冒泡排序算法，函数sort_keys接受分区指针，然后根据分区的key值通过strcmp函数做比较，排出字典序。此函数会在map过程结束后reduce过程开始前调用。

```c
void swap_nodes(struct info_node_t* a, struct info_node_t* b) {
	char* temp_info = a->info;
	a->info = b->info;
	b->info = temp_info;

	struct data_node_t* temp_data = a->data;
	a->data = b->data;
	b->data = temp_data;
}

void sort_keys(struct partition_t* partition) {
	int swapped;
	struct info_node_t* ptr1;
	struct info_node_t* lptr = NULL;

	//检查空分区和单键分区
	if (partition->info_head == NULL || partition->info_head->next == NULL)
		return;

	do {
		swapped = 0;
		ptr1 = partition->info_head;

		while (ptr1->next != lptr) {
			if (strcmp(ptr1->info, ptr1->next->info) > 0) {
				swap_nodes(ptr1, ptr1->next);
				swapped = 1;
			}
			ptr1 = ptr1->next;
		}
		lptr = ptr1;
	} while (swapped);
}


```



### 9.MR_Run函数

```c
void MR_Run(int argc, char* argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition) {
	num_partitions = num_reducers;
	partition_locks = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t) * num_partitions);
	partitions = (struct partition_t*)malloc(sizeof(struct partition_t) * num_partitions);//allocate memory
	for (int i = 0; i < num_partitions; ++i) {
		partition_locks[i] = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
		partitions[i].info_head = NULL;
	}//initialization

	mapper = map;
	pthreads = (pthread_t*)malloc(sizeof(pthread_t) * num_mappers);
	for (int i = 0; i < num_mappers; ++i)
		pthreads[i] = 0;	//初始化为0表示空闲

	int current_work = 1;
	int total_work = argc - 1;

	// Create Mapper threads
	while (current_work <= total_work) {  //当前工作小于总的任务量
		for (int i = 0; i < num_mappers; ++i) {
			if (pthreads[i] == 0 && current_work <= total_work) {	//有空闲的线程
				struct mapper_arg* pass_arg = (struct mapper_arg*)malloc(sizeof(struct mapper_arg));
				pass_arg->arg = argv[current_work];	//分派线程txt文件名
                current_work++;
				pass_arg->id = i;
				pthread_create(&pthreads[i], NULL, MR_Mapper, pass_arg);	//创建线程执行map
			}
		}
		//sched_yield();  // Yield to other threads
	}

	// Wait for all Mapper threads to finish
	for (int i = 0; i < num_mappers; ++i)
		if (pthreads[i] != 0)
			pthread_join(pthreads[i], NULL);

	free(pthreads);  // Clean up Mapper threads



	// Create Reducer threads
	reducer = reduce;
	pthreads = (pthread_t*)malloc(sizeof(pthread_t) * num_reducers);
	for (int i = 0; i < num_reducers; ++i)
		pthreads[i] = 0;

	// Iterate over each partition and sort the keys
	for (int i = 0; i < num_partitions; ++i) {
		// Sort the keys in the current partition
		sort_keys(&partitions[i]);
        
        /*if (partitions[i].info_head != NULL) {
   			printf("partition: %d\n", i);
    		struct info_node_t* info = partitions[i].info_head;
   			 while (info != NULL) {
        		printf("key: %s\n", info->info);
        		struct data_node_t* data = info->data;
       			 while (data != NULL) {
            		printf("value: %s\n", data->value);
            		data = data->next;
        		 }
       		 info = info->next;
    		 }		
		}*///可以打印partition和info和data

		// Create Reducer threads for each key in sorted order
		struct info_node_t* info_ptr = partitions[i].info_head;
		while (info_ptr != NULL) {
			struct reducer_arg* pass_arg = (struct reducer_arg*)malloc(sizeof(struct reducer_arg));
			pass_arg->id = i;  // Thread ID is the partition ID
			pass_arg->key = info_ptr->info;
			pass_arg->partition_id = i;
			pthread_create(&pthreads[i], NULL, MR_Reducer, pass_arg);

			// Move to the next key
			info_ptr = info_ptr->next;
		}
	}

	// Wait for all Reducer threads to finish
	for (int i = 0; i < num_reducers; ++i)
		if (pthreads[i] != 0)
			pthread_join(pthreads[i], NULL);

	// Clean up Reducer threads
	free(pthreads);
	free(partition_locks);
	free(partitions);
}
```

上述代码为最关键的运行函数，主要分为初始化、map、reduce三个部分。

初始化部分包括分区数量、分区锁、分区属性、任务数量等参数；

map部分根据当前任务数量和总任务数量，创建对应的线程，并且调用执行函数，然后等待所有map线程完成，释放其线程占用的内存；reduce部分对于map出来的每个分区，先进行key排序，然后对分区中的key创建线程并调用执行函数，最后等待所有reduce线程完成，释放之前分配的线程、锁和分区内存。

## 三、相关问题

### 1. 编译成功后的可执行文件对多空格的txt文本进行词频统计

#### 1.1 对于一行的txt文本文件，exe文件进行词频统计后结果会出现以下情况：（举例）

```c
a 2
c 1
  1
```

即文本中只有a和c的字符，但是统计结果却出现了空格，事实上，这是由于Readme文件给出的wordcount.c文件中，Map函数使用了getline函数，而getline每行文本时会读取行末的换行符'\n'，因此strsep会多分词出一个空格，程序对正常的句子会统计空格，且频率固定为1。

#### 1.2 如果两个单词间出现多个空格，exe运行会出现死循环

当txt文本中将两个单词间多几个空格，txt作为程序的参数将导致运行进入死循环。这个应该是strsep 会将连续的空格当作多个分隔符来处理，导致产生空词，从而导致死循环，终端界面无法显示出结果。

#### 1.3 一些改进

基于以上测试过程中产生的问题，对wordcount.c进行小小改进，只要在Map函数中增加一行代码，只有分出来的词的长度大于0时，才会调用MR_Emit函数。

```c
//修改前：
 while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
            MR_Emit(token, "1");
        }
//修改后：
while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
            if (strlen(token) > 0) {
                MR_Emit(token, "1");
            }
        }
```

修改后发现词频统计结果效果明显提升，不仅不会像之前统计空格的频率，也不会因为多个连续空格或多空行进入死循环。

### 2. 编写过程中测试经常出现死循环，尤其是MR_Run那段

对于线程是否完成一开始是用标记量写的，测试的时候一直出现死循环（测试命令占用CPU几乎99%且终端不出先结果），甚至采用了显式调度线程sched_yield()，后来后来重构了一下，还是用线程的函数，才解决。

## 四、测试

在Linux系统中主要对以下文件进行编译：mapreduce.c；wordcount.c。假定编译出的可执行文件为function

```bash
gcc -o function mapreduce.c wordcount.c -Wall -Werror -O -pthread -std=gnu99 -g
```

其中 -g 选项用于内存泄露调试，-std=gnu99 定了编译器使用的 C 语言标准，这里指定为 GNU C99 标准，非该标准无法使用for循环。

由于没有脚本测试程序，个人创建了几个txt文本进行测试。

```c
//text1.txt
ab cd f aa b c ab cd c c d 
```

```c
//text2.txt
this is os labwork! !!

this
!!
```

```c
//text3.txt
OS labwork labwork os os!
os
```

在可执行文件目录下执行以下命令：

```bash
./function text1.txt text2.txt text3.txt
```

终端结果如下：

```bash
this 2
OS 1
f 1
cd 2
b 1
c 3
d 1
aa 1
os 3
ab 2
labwork 2
labwork! 1
os! 1
```

可知编译出来的可执行文件可以对多个txt文本参数同时进行词频统计，并且结果正确。

如果在Reduce阶段打印分区信息，可以得到以下结果：

```bash
partition: 0
key: labwork!
value: 1
partition: 1
key: !!
value: 1
value: 1
key: b
value: 1
partition: 2
key: c
value: 1
value: 1
value: 1
partition: 3
key: d
value: 1
partition: 4
key: os!
value: 1
partition: 5
key: f
value: 1
partition: 6
key: cd
value: 1
value: 1
partition: 7
key: aa
value: 1
key: os
value: 1
value: 1
value: 1
partition: 8
key: ab
value: 1
value: 1
partition: 9
key: OS
value: 1
key: is
value: 1
key: labwork
value: 1
value: 1
key: this
value: 1
value: 1

```

由此可以直观地看出Map过程存储到partition的数据的结构。此外，每个partition中的key经过函数排序后，确实是按照字典序进行排列的。

接着进行内存泄露检测，执行命令：

```bash
valgrind --leak-check=full ./function text1.txt text2.txt text3.txt
```

终端结果如下所示：

```bash
==32656== 
==32656== HEAP SUMMARY:
==32656==     in use at exit: 11,239 bytes in 96 blocks
==32656==   total heap usage: 124 allocs, 28 frees, 14,351 bytes allocated
==32656== 
==32656== 1,159 (320 direct, 839 indirect) bytes in 10 blocks are definitely lost in loss record 7 of 9
==32656==    at 0x4C29F73: malloc (vg_replace_malloc.c:309)
==32656==    by 0x401026: insert_info (necessity.c:7)
==32656==    by 0x400AF1: MR_Emit (mapreduce_tt.c:93)
==32656==    by 0x400F58: Map (wordcount.c:19)
==32656==    by 0x4009CA: MR_MapperAdapt (mapreduce_tt.c:55)
==32656==    by 0x4E3EDD4: start_thread (in /usr/lib64/libpthread-2.17.so)
==32656==    by 0x5150EAC: clone (in /usr/lib64/libc-2.17.so)
==32656== 
==32656== 1,680 bytes in 3 blocks are possibly lost in loss record 8 of 9
==32656==    at 0x4C2C089: calloc (vg_replace_malloc.c:762)
==32656==    by 0x40126C4: _dl_allocate_tls (in /usr/lib64/ld-2.17.so)
==32656==    by 0x4E3F7AB: pthread_create@@GLIBC_2.2.5 (in /usr/lib64/libpthread-2.17.so)
==32656==    by 0x400D54: MR_Run (mapreduce_tt.c:164)
==32656==    by 0x401008: main (wordcount.c:35)
==32656== 
==32656== 8,400 bytes in 15 blocks are possibly lost in loss record 9 of 9
==32656==    at 0x4C2C089: calloc (vg_replace_malloc.c:762)
==32656==    by 0x40126C4: _dl_allocate_tls (in /usr/lib64/ld-2.17.so)
==32656==    by 0x4E3F7AB: pthread_create@@GLIBC_2.2.5 (in /usr/lib64/libpthread-2.17.so)
==32656==    by 0x400E74: MR_Run (mapreduce_tt.c:197)
==32656==    by 0x401008: main (wordcount.c:35)
==32656== 
==32656== LEAK SUMMARY:
==32656==    definitely lost: 320 bytes in 10 blocks
==32656==    indirectly lost: 839 bytes in 68 blocks
==32656==      possibly lost: 10,080 bytes in 18 blocks
==32656==    still reachable: 0 bytes in 0 blocks
==32656==         suppressed: 0 bytes in 0 blocks
==32656== 
==32656== For lists of detected and suppressed errors, rerun with: -s
==32656== ERROR SUMMARY: 3 errors from 3 contexts (suppressed: 0 from 0)

```

## 五、完整代码

```c
//mapreduce.h
#ifndef __mapreduce_h__
#define __mapreduce_h__

// Different function pointer types used by MR
typedef char* (*Getter)(char* key, int partition_number);
typedef void (*Mapper)(char* file_name);
typedef void (*Reducer)(char* key, Getter get_func, int partition_number);
typedef unsigned long (*Partitioner)(char* key, int num_partitions);

// External functions: these are what you must define
void MR_Emit(char* key, char* value);

unsigned long MR_DefaultHashPartition(char* key, int num_partitions);

void MR_Run(int argc, char* argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition);

#endif  // __mapreduce_h__#pragma once
```

```c
//wordcount.c
#include "mapreduce.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void Map(char* file_name) {
    FILE* fp = fopen(file_name, "r");
    assert(fp != NULL);

    char* line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1) {
        char* token, * dummy = line;
        while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
            if(strlen(token)>0)  //此处为改进处
            MR_Emit(token, "1");
        }
    }
    free(line); 
    fclose(fp);
}

void Reduce(char* key, Getter get_next, int partition_number) {
    int   count = 0;
    char* value;
    while ((value = get_next(key, partition_number)) != NULL)
        count++;
    printf("%s %d\n", key, count);
}

int main(int argc, char* argv[]) {
    MR_Run(argc, argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
}

```

```c
//mapreduce.c
#include"mapreduce.h"
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <string.h>
#include <sys/types.h>

struct data_node_t {		//值的结构
    int proceed;			//表示是否被计算过，便于遍历
    char* value;
    struct data_node_t* next;
};

struct info_node_t {		//键的结构
    int proceed;			//表示是否被计算过，便于遍历
    char* info;
    struct data_node_t* data;//每个键对应的值，可能有很多mapper，导致一个键有多个值
    struct info_node_t* next;
};

struct partition_t {
    struct info_node_t* info_head;//指向键头
};

void insert_info(struct partition_t* part, char* key) {   //插入新键   管理每个分区中的信息节点链表
    struct info_node_t* new_info = (struct info_node_t*)malloc(sizeof(struct info_node_t));
	if (new_info == NULL) {
        // 处理内存分配失败的情况
        printf("Memory allocation failed ");
        return;
    }

    new_info->info = (char*)malloc(sizeof(char) * (strlen(key) + 1));
    new_info->proceed = 0;
    new_info->data = NULL;                  //分配内存空间+初始化
    new_info->next = NULL;
    
    strcpy(new_info->info, key);            //拷贝新键的数据
    new_info->next = part->info_head;       //新的信息节点插入到当前分区的信息节点链表的
    part->info_head = new_info;             //新节点变成头部
}

void insert_data(struct info_node_t* info, char* value) {   //根据键的结点上插入新值    管理每个信息节点中的数据节点链表
    struct data_node_t* new_node = (struct data_node_t*)malloc(sizeof(struct data_node_t));
    if (new_node == NULL) {
        // 处理内存分配失败的情况
        printf("Memory allocation failed ");
        return;
    }
    new_node->value = (char*)malloc(sizeof(char) * (strlen(value) + 1));
    new_node->proceed = 0;
    strcpy(new_node->value, value);
    new_node->next = info->data;
    info->data = new_node;           //将新的数据节点插入到信息节点的数据节点链表的头部。
}

pthread_mutex_t* partition_locks;
pthread_t* pthreads;   //0代表可以使用 available
struct partition_t* partitions;
int num_partitions;
Mapper mapper;		//函数指针，指向 Map 函数，调用会执行函数
Reducer reducer;	//函数指针，执行Reduce函数，调用会执行函数
//mapper传参 便于线程创建初始化参数
struct mapper_arg {
    int id;		//线程id
    char* arg;	//wordcount中主要是txt文本名
};
//reducer传参 便于线程创建初始化参数
struct reducer_arg {
    int id;		//线程id
    char* key;
    int partition_id;
};

unsigned long MR_DefaultHashPartition(char* key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

void MR_Emit(char* key, char* value) {
    unsigned long partition_index = MR_DefaultHashPartition(key, num_partitions);
    pthread_mutex_lock(&partition_locks[partition_index]);
    struct info_node_t* info_ptr = partitions[partition_index].info_head;
    while (info_ptr != NULL) {
        if (strcmp(info_ptr->info, key) == 0) {
            insert_data(info_ptr, value);
            pthread_mutex_unlock(&partition_locks[partition_index]);
            return;
        }
        info_ptr = info_ptr->next;
    }
    insert_info(&partitions[partition_index], key);
    insert_data(partitions[partition_index].info_head, value);
    pthread_mutex_unlock(&partition_locks[partition_index]);
}

char* MR_getnext(char* key, int partition_number) {
    struct info_node_t* info_ptr = partitions[partition_number].info_head;
    while (info_ptr != NULL) {
        if (strcmp(info_ptr->info, key) == 0) {					//找到分区是否有对应的key键，
            if (info_ptr->proceed == 1)							//该键是否被处理过
                return NULL;
            struct data_node_t* data_ptr = info_ptr->data;
            while (data_ptr != NULL) {
                if (data_ptr->proceed == 0) {
                    data_ptr->proceed = 1;
                    return data_ptr->value;
                }
                data_ptr = data_ptr->next;
            }
            info_ptr->proceed = 1;
            return NULL;
        }
        info_ptr = info_ptr->next;
    }
    return NULL;
}

void* MR_Mapper(void* arg) {
    struct mapper_arg* pass_arg = (struct mapper_arg*)arg; //arg：包含了传递给 Mapper 函数的参数，wordcount中主要是txt文本名
    mapper(pass_arg->arg);
    pthreads[pass_arg->id] = 0;
    free(pass_arg);
    return NULL;
}
void* MR_Reducer(void* arg) {
    struct reducer_arg* pass_arg = (struct reducer_arg*)arg;
    reducer(pass_arg->key, MR_getnext, pass_arg->partition_id);
    pthreads[pass_arg->id] = 0;
    free(pass_arg);
    return NULL;
}
//对分区中的键排序函数
void swap_nodes(struct info_node_t* a, struct info_node_t* b) {
    char* temp_info = a->info;
    a->info = b->info;
    b->info = temp_info;

    struct data_node_t* temp_data = a->data;
    a->data = b->data;
    b->data = temp_data;
}

void sort_keys(struct partition_t* partition) {
    int swapped;
    struct info_node_t* ptr1;
    struct info_node_t* lptr = NULL;

    //检查空分区和单键分区
    if (partition->info_head == NULL || partition->info_head->next == NULL)
        return;

    do {
        swapped = 0;
        ptr1 = partition->info_head;

        while (ptr1->next != lptr) {
            if (strcmp(ptr1->info, ptr1->next->info) > 0) {
                swap_nodes(ptr1, ptr1->next);
                swapped = 1;
            }
            ptr1 = ptr1->next;
        }
        lptr = ptr1;
    } while (swapped);
}


void MR_Run(int argc, char* argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition) {
    num_partitions = num_reducers;
    partition_locks = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t) * num_partitions);
    partitions = (struct partition_t*)malloc(sizeof(struct partition_t) * num_partitions);//allocate memory
    for (int i = 0; i < num_partitions; ++i) {
        partition_locks[i] = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
        partitions[i].info_head = NULL;
    }//initialization

    mapper = map;
    pthreads = (pthread_t*)malloc(sizeof(pthread_t) * num_mappers);
    for (int i = 0; i < num_mappers; ++i)
        pthreads[i] = 0;	//初始化为0表示空闲

    int current_work = 1;
    int total_work = argc - 1;

    // Create Mapper threads
    while (current_work <= total_work) {  //当前工作小于总的任务量
        for (int i = 0; i < num_mappers; ++i) {
            if (pthreads[i] == 0 && current_work <= total_work) {	//有空闲的线程
                struct mapper_arg* pass_arg = (struct mapper_arg*)malloc(sizeof(struct mapper_arg));
                pass_arg->arg = argv[current_work];	//分派线程txt文件名
                current_work++;
                pass_arg->id = i;
                pthread_create(&pthreads[i], NULL, MR_Mapper, pass_arg);	//创建线程执行map
            }
        }
        //sched_yield();  // Yield to other threads
    }

    // Wait for all Mapper threads to finish
    for (int i = 0; i < num_mappers; ++i)
        if (pthreads[i] != 0)
            pthread_join(pthreads[i], NULL);

    free(pthreads);  // Clean up Mapper threads



    // Create Reducer threads
    reducer = reduce;
    pthreads = (pthread_t*)malloc(sizeof(pthread_t) * num_reducers);
    for (int i = 0; i < num_reducers; ++i)
        pthreads[i] = 0;

    // Iterate over each partition and sort the keys
    for (int i = 0; i < num_partitions; ++i) {
        // Sort the keys in the current partition
        sort_keys(&partitions[i]);

        /*if (partitions[i].info_head != NULL) {
            printf("partition: %d\n", i);
            struct info_node_t* info = partitions[i].info_head;
             while (info != NULL) {
                printf("key: %s\n", info->info);
                struct data_node_t* data = info->data;
                 while (data != NULL) {
                    printf("value: %s\n", data->value);
                    data = data->next;
                 }
             info = info->next;
             }
        }*///可以打印partition和info和data

        // Create Reducer threads for each key in sorted order
        struct info_node_t* info_ptr = partitions[i].info_head;
        while (info_ptr != NULL) {
            struct reducer_arg* pass_arg = (struct reducer_arg*)malloc(sizeof(struct reducer_arg));
            pass_arg->id = i;  // Thread ID is the partition ID
            pass_arg->key = info_ptr->info;
            pass_arg->partition_id = i;
            pthread_create(&pthreads[i], NULL, MR_Reducer, pass_arg);

            // Move to the next key
            info_ptr = info_ptr->next;
        }
    }

    // Wait for all Reducer threads to finish
    for (int i = 0; i < num_reducers; ++i)
        if (pthreads[i] != 0)
            pthread_join(pthreads[i], NULL);

    // Clean up Reducer threads
    free(pthreads);
    free(partition_locks);
    free(partitions);
}
```

