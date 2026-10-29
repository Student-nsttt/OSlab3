#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#define BUFFER_SIZE 130

char hex_buffer[BUFFER_SIZE];
int has_hex = 0;
int read_done = 0;

double dec_buffer;
int has_dec = 0;
int convert_done = 0;

pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond1 = PTHREAD_COND_INITIALIZER;

pthread_mutex_t mutex2 = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond2 = PTHREAD_COND_INITIALIZER;

void* reader_thread(void* arg) {
    char *filename = (char*)arg;
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Ошибка открытия файла");
        pthread_mutex_lock(&mutex1);
        read_done = 1;
        pthread_cond_signal(&cond1);
        pthread_mutex_unlock(&mutex1);
        return NULL;
    }

    char line[BUFFER_SIZE];
    while (fgets(line, sizeof(line), file)) {
        line[strcspn(line, "\n")] = '\0';
        if (strlen(line) == 0) continue;

        pthread_mutex_lock(&mutex1);
        while (has_hex) {
            pthread_cond_wait(&cond1, &mutex1);
        }
        
        strcpy(hex_buffer, line);
        has_hex = 1;
        
        pthread_cond_signal(&cond1);
        pthread_mutex_unlock(&mutex1);
    }

    fclose(file);

    pthread_mutex_lock(&mutex1);
    read_done = 1;
    pthread_cond_signal(&cond1);
    pthread_mutex_unlock(&mutex1);
    
    printf("Завершил чтение файла.\n");
    return NULL;
}

double hex_to_decimal(const char *hex) {
    return strtod(hex, NULL); 
}

void* converter_thread(void* arg) {
    while (1) {
        pthread_mutex_lock(&mutex1);
        while (!has_hex && !read_done) {
            pthread_cond_wait(&cond1, &mutex1);
        }

        if (!has_hex && read_done) {
            pthread_mutex_unlock(&mutex1);
            break;
        }

        char local_hex[BUFFER_SIZE];
        strcpy(local_hex, hex_buffer);
        has_hex = 0;
        pthread_cond_signal(&cond1);
        pthread_mutex_unlock(&mutex1);

        double local_dec = hex_to_decimal(local_hex);

        pthread_mutex_lock(&mutex2);
        while (has_dec) {
            pthread_cond_wait(&cond2, &mutex2);
        }
        dec_buffer = local_dec;
        has_dec = 1;
        pthread_cond_signal(&cond2);
        pthread_mutex_unlock(&mutex2);
    }

    pthread_mutex_lock(&mutex2);
    convert_done = 1;
    pthread_cond_signal(&cond2);
    pthread_mutex_unlock(&mutex2);
    return NULL;
}

void* averager_thread(void* arg) {
    double sum = 0.0;
    long long count = 0;

    while (1) {
        pthread_mutex_lock(&mutex2);
        while (!has_dec && !convert_done) {
            pthread_cond_wait(&cond2, &mutex2);
        }

        if (!has_dec && convert_done) {
            pthread_mutex_unlock(&mutex2);
            break;
        }

        double local_dec = dec_buffer;
        has_dec = 0;
        pthread_cond_signal(&cond2);
        pthread_mutex_unlock(&mutex2);

        sum += local_dec;
        count++;
    }

    if (count > 0) {
        printf("\n РЕЗУЛЬТАТ ПОТОКОВ \n");
        printf("Обработано чисел: %lld\n", count);
        printf("Среднее арифметическое: %f\n", sum / count);
    } else {
        printf("Нет данных для расчета.\n");
    }
    return NULL;
}

int main() {
    pthread_t thread1, thread2, thread3;
    char *filename = "test-file.txt";

    pthread_create(&thread1, NULL, reader_thread, filename);
    pthread_create(&thread2, NULL, converter_thread, NULL);
    pthread_create(&thread3, NULL, averager_thread, NULL);

    pthread_join(thread1, NULL);
    pthread_join(thread2, NULL);
    pthread_join(thread3, NULL);

    pthread_mutex_destroy(&mutex1);
    pthread_cond_destroy(&cond1);
    pthread_mutex_destroy(&mutex2);
    pthread_cond_destroy(&cond2);

    return 0;
}


