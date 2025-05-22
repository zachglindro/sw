#define _GNU_SOURCE             // For CPU_ZERO, CPU_SET, pthread_setaffinity_np, sched_getcpu
#define _XOPEN_SOURCE 600       // For barriers, other POSIX features
#define _DEFAULT_SOURCE         // For anet_ntoa
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <time.h>
#include <math.h>               // For floor, log2, sqrt
#include <errno.h>
#include <sched.h>              // For cpu_set_t, CPU_ZERO, CPU_SET, sched_getcpu

#define MAX_IP_LEN 16
#define CONFIG_FILE "config.txt"
#define MAX_CLIENTS 256
#define MAX_PRINT_DIM 5
#define Q_PARAM 3               // Order for Moving Average computation
#define MAX_RAND_INT 1000       // Upper bound (exclusive) for random integers in matrix, e.g. 1 to 999

// Structures
typedef struct {
    char ip[MAX_IP_LEN];
    int port;
    int id; // 1-indexed global ID
} ClientInfo;

typedef struct {
    int target_client_global_id;
    int* matrix_data_segment;
    int rows_to_send;
    int n_cols;

    double* result_storage_location;
    int expected_num_results;

    int* server_results_received_count;
    pthread_mutex_t* server_results_mutex;
    pthread_cond_t* server_results_cond;
} ServerRootCommThreadArgs;

typedef struct {
    int sender_client_global_id;
    int child_global_id;

    int* matrix_data_to_send;
    int rows_to_send_to_child;
    int n_cols;

    double* child_result_vector_storage_location;
    // expected_num_results_from_child is rows_to_send_to_child

    int* child_processed_flag;
} ClientChildHandlerThreadArgs;

typedef struct {
    int thread_idx_in_comp_pool; // For logging or other purposes
    int core_to_pin;

    const int* input_matrix_rows_ptr;
    int num_rows_for_this_thread;
    int n_cols;
    int q_param_val;

    double* output_results_for_these_rows;
} ClientComputeDoWorkThreadArgs;


// Globals
ClientInfo g_all_clients[MAX_CLIENTS];
int g_total_clients_t = 0;
long g_num_cores = 1;


// Utility Functions
long long timespec_diff_ms(struct timespec *start, struct timespec *end) {
    return (end->tv_sec - start->tv_sec) * 1000LL + (end->tv_nsec - start->tv_nsec) / 1000000LL;
}

void print_matrix_portion_int(const char* label, const int* data, int rows, int cols, int max_print) {
    printf("%s (%d x %d) (int):\n", label, rows, cols);
    for (int i = 0; i < rows && i < max_print; ++i) {
        for (int j = 0; j < cols && j < max_print; ++j) {
            printf("%3d ", data[i * (size_t)cols + j]);
        }
        if (cols > max_print) printf("...");
        printf("\n");
    }
    if (rows > max_print) printf("...\n");
}

void print_vector_double(const char* label, const double* data, int count, int max_print) {
    printf("%s (%d elements) (double):\n", label, count);
    for (int i = 0; i < count && i < max_print; ++i) {
        printf("%.4f ", data[i]);
    }
    if (count > max_print) printf("...");
    printf("\n");
}

int send_all(int sockfd, const void *buf, size_t len) {
    size_t total_sent = 0;
    const char *ptr = (const char *)buf;
    while (total_sent < len) {
        ssize_t sent = send(sockfd, ptr + total_sent, len - total_sent, 0);
        if (sent == -1) {
            if (errno == EINTR) continue;
            perror("send failed"); return -1;
        }
        if (sent == 0) { fprintf(stderr, "send_all: socket closed by peer\n"); return 0; }
        total_sent += sent;
    }
    return 1;
}

int recv_all(int sockfd, void *buf, size_t len) {
    size_t total_received = 0;
    char *ptr = (char *)buf;
    while (total_received < len) {
        ssize_t received = recv(sockfd, ptr + total_received, len - total_received, 0);
        if (received == -1) {
            if (errno == EINTR) continue;
            perror("recv failed"); return -1;
        }
        if (received == 0) { fprintf(stderr, "recv_all: socket closed by peer\n"); return 0; }
        total_received += received;
    }
    return 1;
}

int read_config(const char* filename) {
    FILE* fp = fopen(filename, "r");
    if (!fp) { perror("Failed to open config file"); return -1; }
    if (fscanf(fp, "%d\n", &g_total_clients_t) != 1) {
        fprintf(stderr, "Failed to read total number of clients from config.\n"); fclose(fp); return -1;
    }
    if (g_total_clients_t <= 0 || g_total_clients_t > MAX_CLIENTS) {
        fprintf(stderr, "Invalid number of clients: %d\n", g_total_clients_t); fclose(fp); return -1;
    }
    if (g_total_clients_t % 2 != 0 && g_total_clients_t > 1) {
        fprintf(stderr, "Warning: Number of clients t (%d) is odd. Binomial trees might be of different sizes.\n", g_total_clients_t);
    }
    for (int i = 0; i < g_total_clients_t; ++i) {
        g_all_clients[i].id = i + 1;
        if (fscanf(fp, "%15s %d\n", g_all_clients[i].ip, &g_all_clients[i].port) != 2) {
            fprintf(stderr, "Failed to read client %d info from config.\n", i + 1); fclose(fp); return -1;
        }
    }
    fclose(fp);
    printf("Config read successfully: %d clients.\n", g_total_clients_t);
    return 0;
}

// MODIFIED: generate_matrix to produce random positive integers
int* generate_matrix(int n_val) {
    if (n_val <= 0) return NULL;
    size_t num_elements = (size_t)n_val * (size_t)n_val;
    int* matrix = (int*)malloc(num_elements * sizeof(int));
    if (!matrix) { perror("Failed to allocate matrix"); return NULL; }

    // srand() should ideally be called once, e.g., in main or at the start of server_mode
    // For simplicity here, if server_mode is the only place generating, it's okay.
    // If generate_matrix could be called multiple times rapidly, move srand out.

    for (size_t i = 0; i < num_elements; ++i) {
        // rand() % MAX_RAND_INT gives 0 to MAX_RAND_INT-1. Add 1 for 1 to MAX_RAND_INT.
        matrix[i] = (rand() % MAX_RAND_INT) + 1;
    }
    printf("Generated %dx%d matrix with random positive integers (1 to %d).\n", n_val, n_val, MAX_RAND_INT);
    return matrix;
}

ClientInfo* find_client_info(int global_id) {
    if (global_id <= 0 || global_id > g_total_clients_t) return NULL;
    return &g_all_clients[global_id - 1];
}

void get_children_global_ids(int my_global_id, int total_t, int* children_ids_arr, int* num_children_ptr) {
    *num_children_ptr = 0;
    if (total_t <= 0) return;
    int num_clients_tree1 = total_t / 2;
    int num_clients_tree2 = total_t - num_clients_tree1;
    int tree_base_global_id_1idx, my_relative_id_0idx, nodes_in_my_current_tree;

    if (my_global_id <= num_clients_tree1) {
        if (num_clients_tree1 == 0) return;
        tree_base_global_id_1idx = 1;
        my_relative_id_0idx = my_global_id - 1;
        nodes_in_my_current_tree = num_clients_tree1;
    } else {
        if (num_clients_tree2 == 0) return;
        tree_base_global_id_1idx = num_clients_tree1 + 1;
        my_relative_id_0idx = my_global_id - tree_base_global_id_1idx;
        nodes_in_my_current_tree = num_clients_tree2;
    }
    if (nodes_in_my_current_tree <= 1) return;
    int max_k_val = floor(log2(nodes_in_my_current_tree - 1));
    for (int k = max_k_val; k >= 0; --k) {
        int child_candidate_relative_0idx = my_relative_id_0idx + (1 << k);
        if (child_candidate_relative_0idx < nodes_in_my_current_tree) {
            int lsb_val_of_child = child_candidate_relative_0idx & (-child_candidate_relative_0idx);
            int parent_of_candidate_relative_0idx = child_candidate_relative_0idx - lsb_val_of_child;
            if (parent_of_candidate_relative_0idx == my_relative_id_0idx) {
                children_ids_arr[*num_children_ptr] = tree_base_global_id_1idx + child_candidate_relative_0idx;
                (*num_children_ptr)++;
            }
        }
    }
}

// --- Computation Logic ---
double do_row_computation(const int* row_data, int n_cols, int q_param) {
    if (n_cols <= q_param || q_param <= 0) {
        return 0.0; // Not enough data for MA or invalid Q
    }

    double sum_sq_diff = 0.0;
    int num_terms = 0;

    for (int j = q_param; j < n_cols; ++j) { // Current element X_j (0-indexed)
        double current_ma_sum = 0.0;
        for (int k = 1; k <= q_param; ++k) { // Sum q elements before X_j
            current_ma_sum += row_data[j - k];
        }
        double current_ma = current_ma_sum / q_param;

        double diff = (double)row_data[j] - current_ma;
        sum_sq_diff += diff * diff;
        num_terms++;
    }

    if (num_terms == 0) return 0.0;
    return sqrt(sum_sq_diff / num_terms);
}

// --- Server ---
void* server_root_comm_thread(void* args) {
    ServerRootCommThreadArgs* thread_args = (ServerRootCommThreadArgs*)args;
    ClientInfo* target_client = find_client_info(thread_args->target_client_global_id);

    if (!target_client) {
        fprintf(stderr, "Server: Invalid target client ID %d\n", thread_args->target_client_global_id);
        free(thread_args); return NULL;
    }
    printf("Server: Thread trying to connect to client %d (%s:%d)\n", target_client->id, target_client->ip, target_client->port);

    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) { perror("Server: Socket creation failed"); free(thread_args); return NULL; }

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(target_client->port);
    if (inet_pton(AF_INET, target_client->ip, &serv_addr.sin_addr) <= 0) {
        perror("Server: Invalid address"); close(sock_fd); free(thread_args); return NULL;
    }

    if (connect(sock_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        char err_buf[256]; sprintf(err_buf, "Server: Connection Failed to client %d (%s:%d)", target_client->id, target_client->ip, target_client->port);
        perror(err_buf); close(sock_fd); free(thread_args); return NULL;
    }
    printf("Server: Connected to client %d. Sending %d rows, %d cols.\n", target_client->id, thread_args->rows_to_send, thread_args->n_cols);

    if (send_all(sock_fd, &thread_args->rows_to_send, sizeof(int)) <= 0) { close(sock_fd); free(thread_args); return NULL; }
    if (send_all(sock_fd, &thread_args->n_cols, sizeof(int)) <= 0) { close(sock_fd); free(thread_args); return NULL; }
    size_t data_size = (size_t)thread_args->rows_to_send * (size_t)thread_args->n_cols * sizeof(int);
    if (send_all(sock_fd, thread_args->matrix_data_segment, data_size) <= 0) { close(sock_fd); free(thread_args); return NULL; }

    printf("Server: Matrix data sent to client %d. Waiting for results vector.\n", target_client->id);

    // Receive results: first count, then data
    int num_results_received;
    if (recv_all(sock_fd, &num_results_received, sizeof(int)) <= 0) {
        fprintf(stderr, "Server: Failed to receive result count from client %d.\n", target_client->id);
        close(sock_fd); free(thread_args); return NULL;
    }

    if (num_results_received != thread_args->expected_num_results) {
        fprintf(stderr, "Server: Mismatch in expected results from client %d. Expected %d, Got %d.\n",
                target_client->id, thread_args->expected_num_results, num_results_received);
        // Handle error, maybe close and don't use data
    }

    if (num_results_received > 0) {
        size_t results_data_size = (size_t)num_results_received * sizeof(double);
        if (recv_all(sock_fd, thread_args->result_storage_location, results_data_size) <= 0) {
            fprintf(stderr, "Server: Failed to receive result data from client %d.\n", target_client->id);
            close(sock_fd); free(thread_args); return NULL;
        }
        printf("Server: Results vector received from client %d (%d doubles).\n", target_client->id, num_results_received);
       // print_vector_double("Server received from root", thread_args->result_storage_location, num_results_received, MAX_PRINT_DIM);
    }


    pthread_mutex_lock(thread_args->server_results_mutex);
    (*thread_args->server_results_received_count)++;
    if (*thread_args->server_results_received_count == 2) { // Assuming 2 root clients
            pthread_cond_signal(thread_args->server_results_cond);
    }
    pthread_mutex_unlock(thread_args->server_results_mutex);

    close(sock_fd);
    free(thread_args);
    return NULL;
}

void server_mode(int n_val) {
    // Seed the random number generator ONCE for this server instance
    srand(time(NULL)); // Uses current time as seed

    if (g_total_clients_t < 2 && g_total_clients_t != 0) {
         fprintf(stderr, "Server: Requires at least 2 clients for the specified two-tree distribution logic.\n");
         return;
    } else if (g_total_clients_t == 0){
        fprintf(stderr, "Server: No clients configured.\n"); return;
    }

    int* matrix = generate_matrix(n_val); // Now uses rand()
    if (!matrix) return;

    double* overall_result_vector = (double*)malloc((size_t)n_val * sizeof(double));
    if(!overall_result_vector) { perror("Server: Failed to allocate overall_result_vector"); free(matrix); return; }

    struct timespec time_before, time_after;

    pthread_t threads[2];
    int server_results_received_count = 0;
    pthread_mutex_t server_results_mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t server_results_cond = PTHREAD_COND_INITIALIZER;

    ServerRootCommThreadArgs* args1 = (ServerRootCommThreadArgs*)malloc(sizeof(ServerRootCommThreadArgs));
    if (!args1) {perror("Server: Malloc args1 failed"); free(matrix); free(overall_result_vector); return;}
    args1->target_client_global_id = 1;
    args1->matrix_data_segment = matrix;
    args1->rows_to_send = n_val / 2;
    args1->n_cols = n_val;
    args1->result_storage_location = overall_result_vector;
    args1->expected_num_results = args1->rows_to_send;
    args1->server_results_received_count = &server_results_received_count;
    args1->server_results_mutex = &server_results_mutex;
    args1->server_results_cond = &server_results_cond;
    
    // Capture time_before just before distributing matrix to slave processes
    clock_gettime(CLOCK_MONOTONIC, &time_before);
    
    if (pthread_create(&threads[0], NULL, server_root_comm_thread, args1) != 0) {
        perror("Server: Failed to create thread for client 1"); free(args1); free(matrix); free(overall_result_vector); return;
    }

    ServerRootCommThreadArgs* args2 = (ServerRootCommThreadArgs*)malloc(sizeof(ServerRootCommThreadArgs));
    if (!args2) {perror("Server: Malloc args2 failed"); pthread_cancel(threads[0]); pthread_join(threads[0], NULL); free(matrix); free(overall_result_vector); return;}
    args2->target_client_global_id = (g_total_clients_t / 2) + 1;
    args2->matrix_data_segment = matrix + (size_t)(n_val / 2) * (size_t)n_val;
    args2->rows_to_send = n_val - (n_val / 2);
    args2->n_cols = n_val;
    args2->result_storage_location = overall_result_vector + (n_val / 2);
    args2->expected_num_results = args2->rows_to_send;
    args2->server_results_received_count = &server_results_received_count;
    args2->server_results_mutex = &server_results_mutex;
    args2->server_results_cond = &server_results_cond;
    if (pthread_create(&threads[1], NULL, server_root_comm_thread, args2) != 0) {
        perror("Server: Failed to create thread for client (t/2)+1"); free(args2); pthread_cancel(threads[0]); pthread_join(threads[0], NULL); free(matrix); free(overall_result_vector); return;
    }

    pthread_mutex_lock(&server_results_mutex);
    while (server_results_received_count < 2) {
        pthread_cond_wait(&server_results_cond, &server_results_mutex);
    }
    pthread_mutex_unlock(&server_results_mutex);
    printf("Server: Results received from both root clients.\n");

    pthread_join(threads[0], NULL);
    pthread_join(threads[1], NULL);

    clock_gettime(CLOCK_MONOTONIC, &time_after);
    long long elapsed_ms = timespec_diff_ms(&time_before, &time_after);
    printf("Server: Total time elapsed: %lld ms\n", elapsed_ms);

    print_vector_double("Server Overall Result Vector", overall_result_vector, n_val, MAX_PRINT_DIM * 2);

    free(matrix);
    free(overall_result_vector);
    pthread_mutex_destroy(&server_results_mutex);
    pthread_cond_destroy(&server_results_cond);
}


// --- Client ---
void* client_compute_do_work_task(void* args) {
    ClientComputeDoWorkThreadArgs* task_args = (ClientComputeDoWorkThreadArgs*)args;

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(task_args->core_to_pin, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
        fprintf(stderr, "Client: Warning - pthread_setaffinity_np failed for computation thread %d on core %d: %s. Continuing without affinity.\n",
                task_args->thread_idx_in_comp_pool, task_args->core_to_pin, strerror(errno));
    }

    for (int i = 0; i < task_args->num_rows_for_this_thread; ++i) {
        const int* current_row_ptr = task_args->input_matrix_rows_ptr + (i * (size_t)task_args->n_cols);
        task_args->output_results_for_these_rows[i] =
            do_row_computation(current_row_ptr, task_args->n_cols, task_args->q_param_val);
    }
    free(task_args);
    return NULL;
}

void* client_child_handler_thread(void* args) {
    ClientChildHandlerThreadArgs* thread_args = (ClientChildHandlerThreadArgs*)args;
    ClientInfo* target_child_info = find_client_info(thread_args->child_global_id);

    *(thread_args->child_processed_flag) = 0;

    if (!target_child_info) {
        fprintf(stderr, "Client %d: Invalid target child ID %d\n", thread_args->sender_client_global_id, thread_args->child_global_id);
        *(thread_args->child_processed_flag) = -1; free(thread_args); return NULL;
    }
    printf("Client %d: Thread for child %d (%s:%d) starting.\n",
        thread_args->sender_client_global_id, target_child_info->id, target_child_info->ip, target_child_info->port);

    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) { perror("Client: Child socket creation failed"); *(thread_args->child_processed_flag) = -1; free(thread_args); return NULL; }

    struct sockaddr_in child_serv_addr;
    child_serv_addr.sin_family = AF_INET;
    child_serv_addr.sin_port = htons(target_child_info->port);
    if (inet_pton(AF_INET, target_child_info->ip, &child_serv_addr.sin_addr) <= 0) {
        perror("Client: Invalid child address"); close(sock_fd); *(thread_args->child_processed_flag) = -1; free(thread_args); return NULL;
    }

    if (connect(sock_fd, (struct sockaddr*)&child_serv_addr, sizeof(child_serv_addr)) < 0) {
        char err_buf[256]; sprintf(err_buf, "Client %d: Connection Failed to child %d (%s:%d)", thread_args->sender_client_global_id, target_child_info->id, target_child_info->ip, target_child_info->port);
        perror(err_buf); close(sock_fd); *(thread_args->child_processed_flag) = -1; free(thread_args); return NULL;
    }

    printf("Client %d: Connected to child %d. Sending %d rows, %d cols.\n", thread_args->sender_client_global_id, target_child_info->id, thread_args->rows_to_send_to_child, thread_args->n_cols);
    if (send_all(sock_fd, &thread_args->rows_to_send_to_child, sizeof(int)) <= 0) { close(sock_fd); *(thread_args->child_processed_flag) = -1; free(thread_args); return NULL; }
    if (send_all(sock_fd, &thread_args->n_cols, sizeof(int)) <= 0) { close(sock_fd); *(thread_args->child_processed_flag) = -1; free(thread_args); return NULL; }
    size_t data_size = (size_t)thread_args->rows_to_send_to_child * (size_t)thread_args->n_cols * sizeof(int);
    if (send_all(sock_fd, thread_args->matrix_data_to_send, data_size) <= 0) { close(sock_fd); *(thread_args->child_processed_flag) = -1; free(thread_args); return NULL; }

    printf("Client %d: Matrix sent to child %d. Waiting for results vector.\n", thread_args->sender_client_global_id, target_child_info->id);

    int num_results_from_child;
    if (recv_all(sock_fd, &num_results_from_child, sizeof(int)) <= 0) {
        fprintf(stderr, "Client %d: Failed to receive result count from child %d.\n", thread_args->sender_client_global_id, target_child_info->id);
        *(thread_args->child_processed_flag) = -1; close(sock_fd); free(thread_args); return NULL;
    }

    if (num_results_from_child != thread_args->rows_to_send_to_child) {
         fprintf(stderr, "Client %d: Mismatch in expected results from child %d. Expected %d, Got %d.\n",
                thread_args->sender_client_global_id, target_child_info->id, thread_args->rows_to_send_to_child, num_results_from_child);
        *(thread_args->child_processed_flag) = -1;
    }

    if (num_results_from_child > 0 && *(thread_args->child_processed_flag) != -1) {
        size_t results_data_size_child = (size_t)num_results_from_child * sizeof(double);
        if (recv_all(sock_fd, thread_args->child_result_vector_storage_location, results_data_size_child) <= 0) {
            fprintf(stderr, "Client %d: Failed to receive result data from child %d.\n", thread_args->sender_client_global_id, target_child_info->id);
            *(thread_args->child_processed_flag) = -1;
        } else {
            printf("Client %d: Results received from child %d.\n", thread_args->sender_client_global_id, target_child_info->id);
            *(thread_args->child_processed_flag) = 1;
        }
    } else if (num_results_from_child == 0 && *(thread_args->child_processed_flag) != -1) {
        *(thread_args->child_processed_flag) = 1;
    }

    close(sock_fd);
    free(thread_args);
    return NULL;
}

void client_mode(int my_listen_port, int my_global_id) {
    struct timespec time_before_client_overall, time_after_client_overall;
    clock_gettime(CLOCK_MONOTONIC, &time_before_client_overall);

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("Client: Listener socket creation failed"); return; }
    int optval = 1; setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
    struct sockaddr_in my_addr;
    my_addr.sin_family = AF_INET; my_addr.sin_addr.s_addr = INADDR_ANY; my_addr.sin_port = htons(my_listen_port);
    if (bind(listen_fd, (struct sockaddr*)&my_addr, sizeof(my_addr)) < 0) {
        char err_buf[256]; sprintf(err_buf, "Client %d: Bind failed on port %d", my_global_id, my_listen_port);
        perror(err_buf); close(listen_fd); return;
    }
    if (listen(listen_fd, 1) < 0) { perror("Client: Listen failed"); close(listen_fd); return; }
    printf("Client %d: Listening on port %d\n", my_global_id, my_listen_port);

    struct sockaddr_in parent_addr; socklen_t parent_addr_len = sizeof(parent_addr);
    int parent_conn_fd = accept(listen_fd, (struct sockaddr*)&parent_addr, &parent_addr_len);
    if (parent_conn_fd < 0) { perror("Client: Accept failed"); close(listen_fd); return; }
    printf("Client %d: Accepted connection from parent/server %s:%d\n", my_global_id, inet_ntoa(parent_addr.sin_addr), ntohs(parent_addr.sin_port));
    close(listen_fd);

    int R_incoming, N_original_cols;
    if (recv_all(parent_conn_fd, &R_incoming, sizeof(int)) <= 0) { close(parent_conn_fd); return; }
    if (recv_all(parent_conn_fd, &N_original_cols, sizeof(int)) <= 0) { close(parent_conn_fd); return; }
    printf("Client %d: Receiving matrix of %d x %d\n", my_global_id, R_incoming, N_original_cols);
    size_t received_data_size = (size_t)R_incoming * (size_t)N_original_cols * sizeof(int);
    int* received_matrix_data = (int*)malloc(received_data_size);
    if (!received_matrix_data) { perror("Client: Malloc failed for received matrix"); close(parent_conn_fd); return; }
    if (recv_all(parent_conn_fd, received_matrix_data, received_data_size) <= 0) {
        free(received_matrix_data); close(parent_conn_fd); return;
    }

    double* final_results_for_parent = (double*)malloc((size_t)R_incoming * sizeof(double));
    if(!final_results_for_parent) { perror("Client: Malloc for final_results_for_parent failed"); free(received_matrix_data); close(parent_conn_fd); return; }

    int children_global_ids[MAX_CLIENTS]; int num_children;
    get_children_global_ids(my_global_id, g_total_clients_t, children_global_ids, &num_children);

    pthread_t* child_handler_threads = NULL;
    int* child_processed_flags = NULL;
    int R_local = R_incoming;
    int* current_matrix_segment_ptr_for_dist = received_matrix_data;
    size_t current_child_result_offset = 0;

    int temp_R_local_calc = R_incoming;
    for(int i=0; i < num_children; ++i) temp_R_local_calc /= 2;
    R_local = temp_R_local_calc;
    current_child_result_offset = (size_t)R_local;

    if (num_children > 0) {
        printf("Client %d: Has %d children. Distributing matrix and preparing for results...\n", my_global_id, num_children);
        child_handler_threads = (pthread_t*)malloc(num_children * sizeof(pthread_t));
        if (!child_handler_threads) {
            perror("Client: Malloc for child_handler_threads failed");
            free(received_matrix_data); free(final_results_for_parent); close(parent_conn_fd);
            return;
        }
        child_processed_flags = (int*)calloc(num_children, sizeof(int));
        if (!child_processed_flags) {
            perror("Client: Calloc for child_processed_flags failed");
            free(received_matrix_data); free(final_results_for_parent); close(parent_conn_fd);
            free(child_handler_threads);
            return;
        }

        int rows_for_distribution_iter = R_incoming;
        for (int i = 0; i < num_children; ++i) {
            int rows_for_this_child = rows_for_distribution_iter / 2;
            int* data_ptr_for_this_child = current_matrix_segment_ptr_for_dist + (size_t)(rows_for_distribution_iter / 2) * (size_t)N_original_cols;

            ClientChildHandlerThreadArgs* child_args = (ClientChildHandlerThreadArgs*)malloc(sizeof(ClientChildHandlerThreadArgs));
            if(!child_args) { perror("Client: Malloc child_args failed"); child_processed_flags[i] = -1; continue; }

            child_args->sender_client_global_id = my_global_id;
            child_args->child_global_id = children_global_ids[i];
            child_args->matrix_data_to_send = data_ptr_for_this_child;
            child_args->rows_to_send_to_child = rows_for_this_child;
            child_args->n_cols = N_original_cols;
            child_args->child_result_vector_storage_location = final_results_for_parent + current_child_result_offset;
            child_args->child_processed_flag = &child_processed_flags[i];

            if (pthread_create(&child_handler_threads[i], NULL, client_child_handler_thread, child_args) != 0) {
                perror("Client: Failed to create child_handler_thread"); free(child_args); child_processed_flags[i] = -1;
            }
            current_child_result_offset += rows_for_this_child;
            rows_for_distribution_iter /= 2;
        }
    }
    printf("Client %d: Will process %d rows locally (matrix portion starts at received_matrix_data).\n", my_global_id, R_local);

    struct timespec time_before_computation, time_after_computation;
    clock_gettime(CLOCK_MONOTONIC, &time_before_computation);

    if (R_local > 0) {
        int num_comp_threads = (R_local < g_num_cores) ? R_local : (int)g_num_cores;
        if (num_comp_threads <= 0 && R_local > 0) num_comp_threads = 1;

        pthread_t* compute_threads = NULL;
        if (num_comp_threads > 0) {
            compute_threads = (pthread_t*)malloc(num_comp_threads * sizeof(pthread_t));
            if (!compute_threads) { perror("Client: Malloc compute_threads failed"); /* Critical error, consider exiting */ }
        }

        int current_row_offset_for_comp = 0;
        for (int i = 0; i < num_comp_threads; ++i) {
            if (!compute_threads) break; // Malloc failed, can't create threads
            ClientComputeDoWorkThreadArgs* comp_args = (ClientComputeDoWorkThreadArgs*)malloc(sizeof(ClientComputeDoWorkThreadArgs));
            if (!comp_args) { perror("Client: Malloc comp_args failed"); continue; }

            comp_args->thread_idx_in_comp_pool = i;
            comp_args->core_to_pin = i % (int)g_num_cores;

            int rows_for_this_comp_thread = R_local / num_comp_threads;
            if (i < R_local % num_comp_threads) rows_for_this_comp_thread++;

            comp_args->input_matrix_rows_ptr = received_matrix_data + (size_t)current_row_offset_for_comp * (size_t)N_original_cols;
            comp_args->num_rows_for_this_thread = rows_for_this_comp_thread;
            comp_args->n_cols = N_original_cols;
            comp_args->q_param_val = Q_PARAM;
            comp_args->output_results_for_these_rows = final_results_for_parent + current_row_offset_for_comp;

            if (pthread_create(&compute_threads[i], NULL, client_compute_do_work_task, comp_args) != 0) {
                perror("Client: Failed to create compute thread"); free(comp_args); compute_threads[i] = 0;
            }
            current_row_offset_for_comp += rows_for_this_comp_thread;
        }
        for (int i = 0; i < num_comp_threads; ++i) {
            if (compute_threads && compute_threads[i]) pthread_join(compute_threads[i], NULL);
        }
        if (compute_threads) free(compute_threads);
    }
    clock_gettime(CLOCK_MONOTONIC, &time_after_computation);
    long long elapsed_ms_computation = timespec_diff_ms(&time_before_computation, &time_after_computation);
    printf("Client %d: Computation time: %lld ms for %d local rows.\n", my_global_id, elapsed_ms_computation, R_local);

    if (num_children > 0 && child_handler_threads) {
        int all_children_ok = 1;
        for (int i = 0; i < num_children; ++i) {
            if (child_handler_threads[i]) pthread_join(child_handler_threads[i], NULL);
            if (child_processed_flags && child_processed_flags[i] != 1) {
                fprintf(stderr, "Client %d: Child %d (global ID %d) did not process successfully (flag=%d).\n", my_global_id, i, children_global_ids[i], child_processed_flags[i]);
                all_children_ok = 0;
            }
        }
        if(all_children_ok) printf("Client %d: All children processed and results received.\n", my_global_id);
        else printf("Client %d: Errors encountered with some children.\n", my_global_id);
    }

    printf("Client %d: Sending %d results to parent/server.\n", my_global_id, R_incoming);
    if (send_all(parent_conn_fd, &R_incoming, sizeof(int)) <= 0) { fprintf(stderr, "Client %d: Error sending result count\n", my_global_id); }
    if (R_incoming > 0) {
      if (send_all(parent_conn_fd, final_results_for_parent, (size_t)R_incoming * sizeof(double)) <= 0) { fprintf(stderr, "Client %d: Error sending result data\n", my_global_id); }
    }

    close(parent_conn_fd);
    free(received_matrix_data);
    free(final_results_for_parent);
    if (child_handler_threads) free(child_handler_threads);
    if (child_processed_flags) free(child_processed_flags);

    clock_gettime(CLOCK_MONOTONIC, &time_after_client_overall);
    long long elapsed_ms_client_overall = timespec_diff_ms(&time_before_client_overall, &time_after_client_overall);
    printf("Client %d: Overall client time: %lld ms.\n", my_global_id, elapsed_ms_client_overall);
}


// --- Main ---
int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage:\n  Server: %s <n_matrix_size> 0\n  Client: %s <listen_port> <client_id (>0)\n", argv[0], argv[0]);
        return 1;
    }
    g_num_cores = sysconf(_SC_NPROCESSORS_ONLN);
    if (g_num_cores < 1) g_num_cores = 1;
    printf("System has %ld CPU cores available.\n", g_num_cores);

    int arg1 = atoi(argv[1]); int arg2 = atoi(argv[2]);
    if (read_config(CONFIG_FILE) != 0) {
        fprintf(stderr, "Fatal: Failed to read configuration file %s. Exiting.\n", CONFIG_FILE); return 1;
    }

    if (arg2 == 0) {
        if (arg1 <= 0) { fprintf(stderr, "Server mode: Matrix size n must be positive.\n"); return 1; }
        printf("Starting in SERVER mode. Matrix size: %d\n", arg1);
        server_mode(arg1);
    } else if (arg2 > 0) {
        if (arg1 <= 0 || arg1 > 65535) { fprintf(stderr, "Client mode: Port must be between 1 and 65535.\n"); return 1; }
        if (arg2 <= 0 || arg2 > g_total_clients_t) {
             fprintf(stderr, "Client mode: Client ID %d is invalid for %d total clients.\n", arg2, g_total_clients_t); return 1;
        }
        printf("Starting in CLIENT mode. Listening Port: %d, Client ID: %d\n", arg1, arg2);
        client_mode(arg1, arg2);
    } else {
        fprintf(stderr, "Invalid mode indicator (second argument).\n"); return 1;
    }
    return 0;
}