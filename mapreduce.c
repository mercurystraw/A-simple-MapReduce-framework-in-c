#include"mapreduce.h"
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <string.h>
#include <sys/types.h>

struct data_node_t {
    int                 proceed;
    char* value;
    struct data_node_t* next;
};

struct info_node_t {
    int                 proceed;
    char* info;
    struct data_node_t* data;
    struct info_node_t* next;
};

struct partition_t {
    struct info_node_t* info_head;
};

void insert_info(struct partition_t* part, char* key) {   //�����¼�    ����ÿ�������е���Ϣ�ڵ�����
    struct info_node_t* new_info = (struct info_node_t*)malloc(sizeof(struct info_node_t));

    new_info->info = (char*)malloc(sizeof(char) * (strlen(key) + 1));
    new_info->data = NULL;                                  //�����ڴ�ռ�+��ʼ��
    new_info->proceed = 0;
    new_info->next = NULL;
    strcpy(new_info->info, key);                            //�����¼�

    new_info->next = part->info_head;                //�µ���Ϣ�ڵ���뵽��ǰ��������Ϣ�ڵ������
    part->info_head = new_info;                     //�½ڵ���ͷ��
}

void insert_data(struct info_node_t* info, char* value) {   //���ݼ��Ľ���ϲ�����ֵ    ����ÿ����Ϣ�ڵ��е����ݽڵ�����
    struct data_node_t* new_node = (struct data_node_t*)malloc(sizeof(struct data_node_t));
    new_node->value = (char*)malloc(sizeof(char) * (strlen(value) + 1));
    new_node->proceed = 0;
    strcpy(new_node->value, value);
    new_node->next = info->data;
    info->data = new_node;                           //���µ����ݽڵ���뵽��Ϣ�ڵ�����ݽڵ������ͷ����
}

pthread_mutex_t* partition_locks;
pthread_t* pthreads;   //0�������ʹ�� available
struct partition_t* partitions;
int num_partitions;
Mapper mapper;		//����ָ�룬ָ�� Map ���������û�ִ�к���
Reducer reducer;	//����ָ�룬ִ��Reduce���������û�ִ�к���
//mapper���� �����̴߳�����ʼ������
struct mapper_arg {
    int id;		//�߳�id
    char* arg;	//wordcount����Ҫ��txt�ı���
};
//reducer���� �����̴߳�����ʼ������
struct reducer_arg {
    int id;		//�߳�id
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

char* MR_GetNext(char* key, int partition_number) {
    struct info_node_t* info_ptr = partitions[partition_number].info_head;
    while (info_ptr != NULL) {
        if (strcmp(info_ptr->info, key) == 0) {					//�ҵ������Ƿ��ж�Ӧ��key����
            if (info_ptr->proceed == 1)							//�ü��Ƿ񱻴����
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
    struct mapper_arg* pass_arg = (struct mapper_arg*)arg; //arg�������˴��ݸ� Mapper �����Ĳ�����wordcount����Ҫ��txt�ı���
    mapper(pass_arg->arg);
    pthreads[pass_arg->id] = 0;
    free(pass_arg);
    return NULL;
}
void* MR_Reducer(void* arg) {
    struct reducer_arg* pass_arg = (struct reducer_arg*)arg;
    reducer(pass_arg->key, MR_GetNext, pass_arg->partition_id);
    pthreads[pass_arg->id] = 0;
    free(pass_arg);
    return NULL;
}

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

    //���շ����͵�������
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
        pthreads[i] = 0;	//��ʼ��Ϊ0��ʾ����

    int current_work = 1;
    int total_work = argc - 1;

    // Create Mapper threads
    while (current_work <= total_work) {  //��ǰ����С���ܵ�������
        for (int i = 0; i < num_mappers; ++i) {
            if (pthreads[i] == 0 && current_work <= total_work) {	//�п��е��߳�
                struct mapper_arg* pass_arg = (struct mapper_arg*)malloc(sizeof(struct mapper_arg));
                pass_arg->arg = argv[current_work];	//�����߳�txt�ļ���
                current_work++;
                pass_arg->id = i;
                pthread_create(&pthreads[i], NULL, MR_Mapper, pass_arg);	//�����߳�ִ��map
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
        }*///���Դ�ӡpartition��info��data

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