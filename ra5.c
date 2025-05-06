#define _XOPEN_SOURCE 600 // For barriers, other POSIX features
#define _DEFAULT_SOURCE // For anet_ntoa
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <time.h>
#include <math.h> // For floor, log2
#include <errno.h>

#define MAX_IP_LEN 16
#define CONFIG_FILE "config.txt"
#define ACK_MSG "ACK"
#define ACK_LEN 4 // Including null terminator
#define MAX_CLIENTS 256 // Max clients supported by config parsing
#define MAX_PRINT_DIM 5 // For printing small matrix portions for debug

// Structures
typedef struct {
    char ip[MAX_IP_LEN];
    int port;
    int id; // 1-indexed global ID
} ClientInfo;

typedef struct {
    int target_client_global_id; // 1-indexed global ID
    int* matrix_data_segment;    // Pointer to the start of data for this client
    int rows_to_send;
    int n_cols;
    // ClientInfo* all_clients_list; // Not used in thread, kept for potential future
    // int total_t_clients;       // Not used in thread
    // For server to track ACKs from root clients
    int* server_acks_received_count;
    pthread_mutex_t* server_ack_mutex;
    pthread_cond_t* server_ack_cond;
} ServerSendThreadArgs;

typedef struct {
    int sender_client_global_id; // ID of the client sending the data (parent) <--- FIX
    int child_global_id;
    int* matrix_data_to_send; // Pointer to the start of data for this child
    int rows_to_send;
    int n_cols;
    // ClientInfo* all_clients_list; // Not used in thread
    // int total_t_clients;       // Not used in thread
    // For client to track ACKs from its children
    int* child_ack_flag; // Points to an element in an array of flags
} ClientSendToChildThreadArgs;

// Global (for simplicity in this example, can be passed around)
ClientInfo g_all_clients[MAX_CLIENTS];
int g_total_clients_t = 0;

// Utility Functions
long long timespec_diff_ms(struct timespec *start, struct timespec *end) {
    return (end->tv_sec - start->tv_sec) * 1000LL + (end->tv_nsec - start->tv_nsec) / 1000000LL;
}

void print_matrix_portion(const char* label, int* data, int rows, int cols, int max_print) {
    printf("%s (%d x %d):\n", label, rows, cols);
    for (int i = 0; i < rows && i < max_print; ++i) {
        for (int j = 0; j < cols && j < max_print; ++j) {
            printf("%3d ", data[i * (size_t)cols + j]); // Use size_t for cols in index
        }
        if (cols > max_print) printf("...");
        printf("\n");
    }
    if (rows > max_print) printf("...\n");
}

// Reliable send
int send_all(int sockfd, const void *buf, size_t len) {
    size_t total_sent = 0;
    const char *ptr = (const char *)buf;
    while (total_sent < len) {
        ssize_t sent = send(sockfd, ptr + total_sent, len - total_sent, 0);
        if (sent == -1) {
            if (errno == EINTR) continue;
            perror("send failed");
            return -1;
        }
        if (sent == 0) { 
            fprintf(stderr, "send_all: socket closed by peer\n");
            return 0; 
        }
        total_sent += sent;
    }
    return 1;
}

// Reliable receive
int recv_all(int sockfd, void *buf, size_t len) {
    size_t total_received = 0;
    char *ptr = (char *)buf;
    while (total_received < len) {
        ssize_t received = recv(sockfd, ptr + total_received, len - total_received, 0);
        if (received == -1) {
            if (errno == EINTR) continue;
            perror("recv failed");
            return -1;
        }
        if (received == 0) { 
            fprintf(stderr, "recv_all: socket closed by peer\n");
            return 0;
        }
        total_received += received;
    }
    return 1;
}

// Configuration Reading
int read_config(const char* filename) {
    FILE* fp = fopen(filename, "r");
    if (!fp) {
        perror("Failed to open config file");
        return -1;
    }

    if (fscanf(fp, "%d\n", &g_total_clients_t) != 1) {
        fprintf(stderr, "Failed to read total number of clients from config.\n");
        fclose(fp);
        return -1;
    }

    if (g_total_clients_t <= 0 || g_total_clients_t > MAX_CLIENTS) {
        fprintf(stderr, "Invalid number of clients: %d\n", g_total_clients_t);
        fclose(fp);
        return -1;
    }
     if (g_total_clients_t % 2 != 0 && g_total_clients_t > 1) { 
        fprintf(stderr, "Warning: Number of clients t (%d) is odd. Binomial trees might be of different sizes.\n", g_total_clients_t);
    }


    for (int i = 0; i < g_total_clients_t; ++i) {
        g_all_clients[i].id = i + 1; // 1-indexed ID
        if (fscanf(fp, "%15s %d\n", g_all_clients[i].ip, &g_all_clients[i].port) != 2) {
            fprintf(stderr, "Failed to read client %d info from config.\n", i + 1);
            fclose(fp);
            return -1;
        }
    }
    fclose(fp);
    printf("Config read successfully: %d clients.\n", g_total_clients_t);
    return 0;
}

// Matrix Generation (Server)
int* generate_matrix(int n_val) {
    if (n_val <= 0) return NULL;
    // Use size_t for multiplication to prevent overflow for large n_val
    size_t num_elements = (size_t)n_val * (size_t)n_val;
    int* matrix = (int*)malloc(num_elements * sizeof(int));
    if (!matrix) {
        perror("Failed to allocate matrix");
        return NULL;
    }
    for (int i = 0; i < n_val; ++i) {
        for (int j = 0; j < n_val; ++j) {
            matrix[(size_t)i * (size_t)n_val + j] = i * n_val + j; // Simple predictable values
        }
    }
    printf("Generated %dx%d matrix.\n", n_val, n_val);
    return matrix;
}

ClientInfo* find_client_info(int global_id) {
    if (global_id <= 0 || global_id > g_total_clients_t) return NULL;
    return &g_all_clients[global_id - 1]; // IDs are 1-indexed
}

// Binomial Tree Children Calculation - REVISED
void get_children_global_ids(int my_global_id, int total_t, int* children_ids_arr, int* num_children_ptr) {
    *num_children_ptr = 0;
    if (total_t <= 0) return;

    int num_clients_tree1 = total_t / 2;
    int num_clients_tree2 = total_t - num_clients_tree1;

    int tree_base_global_id_1idx;
    int my_relative_id_0idx;
    int nodes_in_my_current_tree;

    if (my_global_id <= num_clients_tree1) { // In first tree
        if (num_clients_tree1 == 0) return; // This tree is empty, client ID must be for tree2 or invalid
        tree_base_global_id_1idx = 1;
        my_relative_id_0idx = my_global_id - 1;
        nodes_in_my_current_tree = num_clients_tree1;
    } else { // In second tree
        if (num_clients_tree2 == 0) return; // This tree is empty, client ID invalid
        tree_base_global_id_1idx = num_clients_tree1 + 1;
        my_relative_id_0idx = my_global_id - tree_base_global_id_1idx;
        nodes_in_my_current_tree = num_clients_tree2;
    }

    if (nodes_in_my_current_tree <= 1) { // Single node in this tree (or empty) means no children
        return;
    }

    int max_k_val = floor(log2(nodes_in_my_current_tree - 1));

    for (int k = max_k_val; k >= 0; --k) { // Iterate largest k first for "furthest child"
        int child_candidate_relative_0idx = my_relative_id_0idx + (1 << k);

        if (child_candidate_relative_0idx < nodes_in_my_current_tree) {
            int lsb_val_of_child = child_candidate_relative_0idx & (-child_candidate_relative_0idx);
            // child_candidate_relative_0idx is always > 0 since my_relative_id_0idx >=0 and (1<<k) >=1.
            // So lsb_val_of_child will be > 0.
            int parent_of_candidate_relative_0idx = child_candidate_relative_0idx - lsb_val_of_child;

            if (parent_of_candidate_relative_0idx == my_relative_id_0idx) {
                children_ids_arr[*num_children_ptr] = tree_base_global_id_1idx + child_candidate_relative_0idx;
                (*num_children_ptr)++;
            }
        }
    }
}


// --- Server ---
void* server_send_to_root_client_thread(void* args) {
    ServerSendThreadArgs* thread_args = (ServerSendThreadArgs*)args;
    ClientInfo* target_client = find_client_info(thread_args->target_client_global_id);

    if (!target_client) {
        fprintf(stderr, "Server: Invalid target client ID %d\n", thread_args->target_client_global_id);
        // Signal error to main server thread if necessary, for now just exit thread
        // Ensure ack count logic handles this (e.g., don't wait indefinitely)
        // For simplicity, we assume config is correct and clients exist.
        free(thread_args);
        return NULL;
    }

    printf("Server: Thread trying to connect to client %d (%s:%d)\n", target_client->id, target_client->ip, target_client->port);

    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("Server: Socket creation failed");
        free(thread_args);
        return NULL;
    }

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(target_client->port);
    if (inet_pton(AF_INET, target_client->ip, &serv_addr.sin_addr) <= 0) {
        perror("Server: Invalid address / Address not supported");
        close(sock_fd);
        free(thread_args);
        return NULL;
    }

    if (connect(sock_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        char err_buf[256];
        sprintf(err_buf, "Server: Connection Failed to client %d (%s:%d)", target_client->id, target_client->ip, target_client->port);
        perror(err_buf);
        close(sock_fd);
        free(thread_args);
        return NULL;
    }
    printf("Server: Connected to client %d. Sending %d rows, %d cols.\n", target_client->id, thread_args->rows_to_send, thread_args->n_cols);
    // print_matrix_portion("Server sending to root", thread_args->matrix_data_segment, thread_args->rows_to_send, thread_args->n_cols, MAX_PRINT_DIM);


    // Send dimensions
    if (send_all(sock_fd, &thread_args->rows_to_send, sizeof(int)) <= 0) { close(sock_fd); free(thread_args); return NULL; }
    if (send_all(sock_fd, &thread_args->n_cols, sizeof(int)) <= 0) { close(sock_fd); free(thread_args); return NULL; }

    // Send matrix data
    size_t data_size = (size_t)thread_args->rows_to_send * (size_t)thread_args->n_cols * sizeof(int);
    if (send_all(sock_fd, thread_args->matrix_data_segment, data_size) <= 0) { close(sock_fd); free(thread_args); return NULL; }
    
    printf("Server: Matrix data sent to client %d. Waiting for ACK.\n", target_client->id);

    char ack_buf[ACK_LEN];
    if (recv_all(sock_fd, ack_buf, ACK_LEN) <= 0) {
        fprintf(stderr, "Server: Failed to receive ACK from client %d or connection closed.\n", target_client->id);
    } else if (strcmp(ack_buf, ACK_MSG) == 0) {
        printf("Server: ACK received from client %d.\n", target_client->id);
        pthread_mutex_lock(thread_args->server_ack_mutex);
        (*thread_args->server_acks_received_count)++;
        // Signal only if this is the last expected ACK
        if (*thread_args->server_acks_received_count == 2) { // Assuming 2 root clients to ACK
             pthread_cond_signal(thread_args->server_ack_cond);
        }
        pthread_mutex_unlock(thread_args->server_ack_mutex);
    } else {
        fprintf(stderr, "Server: Invalid ACK from client %d: %s\n", target_client->id, ack_buf);
    }

    close(sock_fd);
    free(thread_args);
    return NULL;
}

void server_mode(int n_val) {
    // Config should be read by main before calling server_mode
    if (g_total_clients_t < 1) { // Need at least one client for spec, 2 for full logic
         fprintf(stderr, "Server: Config problem or no clients defined.\n");
         return;
    }
    if (g_total_clients_t == 1) {
        fprintf(stderr, "Server: Running with 1 client. Sending full matrix to client 1.\n");
        // Simplified logic for 1 client if needed, or stick to spec requiring t/2 logic
    } else if (g_total_clients_t < 2) { // Effectively catches t=1 again if not handled above
        fprintf(stderr, "Server: Requires at least 2 clients for the specified two-tree distribution logic.\n");
        return;
    }


    int* matrix = generate_matrix(n_val);
    if (!matrix) return;

    struct timespec time_before, time_after;
    clock_gettime(CLOCK_MONOTONIC, &time_before);

    pthread_t threads[2]; // For the two root clients
    int server_acks_received_count = 0;
    pthread_mutex_t server_ack_mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t server_ack_cond = PTHREAD_COND_INITIALIZER;

    // Client 1 (Root of first tree)
    ServerSendThreadArgs* args1 = (ServerSendThreadArgs*)malloc(sizeof(ServerSendThreadArgs));
    if (!args1) { perror("Server: Malloc args1 failed"); free(matrix); return; }
    args1->target_client_global_id = 1;
    args1->matrix_data_segment = matrix;
    args1->rows_to_send = n_val / 2;
    args1->n_cols = n_val;
    args1->server_acks_received_count = &server_acks_received_count;
    args1->server_ack_mutex = &server_ack_mutex;
    args1->server_ack_cond = &server_ack_cond;

    if (pthread_create(&threads[0], NULL, server_send_to_root_client_thread, args1) != 0) {
        perror("Server: Failed to create thread for client 1");
        free(args1); 
        // Potentially join/cancel other thread if it was started
        free(matrix); // Or handle partial failure more gracefully
        return;
    }

    int root_client_2_id = (g_total_clients_t / 2) + 1;
    if (root_client_2_id > g_total_clients_t || root_client_2_id <= 0){
        fprintf(stderr, "Server: Invalid ID for second root client (%d) with t=%d.\n", root_client_2_id, g_total_clients_t);
        // This case implies t=1, which should be handled earlier or has specific logic.
        // If t=1, this thread creation part for client (t/2)+1 should be skipped.
        // For now, assuming t>=2 as per earlier checks.
        // If t=1, this will be (1/2)+1 = 1. Server sends to client 1 twice? Spec implies distinct clients.
        // The check `g_total_clients_t < 2` should prevent this.
    }


    // Client (t/2) + 1 (Root of second tree)
    ServerSendThreadArgs* args2 = (ServerSendThreadArgs*)malloc(sizeof(ServerSendThreadArgs));
    if (!args2) { perror("Server: Malloc args2 failed"); free(matrix); /* join thread 0? */ return; }
    args2->target_client_global_id = root_client_2_id;
    args2->matrix_data_segment = matrix + (size_t)(n_val / 2) * (size_t)n_val; // Offset for second half
    args2->rows_to_send = n_val - (n_val / 2); // Remaining rows
    args2->n_cols = n_val;
    args2->server_acks_received_count = &server_acks_received_count;
    args2->server_ack_mutex = &server_ack_mutex;
    args2->server_ack_cond = &server_ack_cond;
    
    if (pthread_create(&threads[1], NULL, server_send_to_root_client_thread, args2) != 0) {
        perror("Server: Failed to create thread for client (t/2)+1");
        free(args2);
        // Potentially join/cancel thread 0
        free(matrix);
        return;
    }
    
    // Wait for both ACKs using condition variable
    pthread_mutex_lock(&server_ack_mutex);
    // If clients fail to connect/ACK, this could wait forever. Add timeout?
    while (server_acks_received_count < 2) { // Expect 2 ACKs
        pthread_cond_wait(&server_ack_cond, &server_ack_mutex);
    }
    pthread_mutex_unlock(&server_ack_mutex);

    printf("Server: Both root clients processed (or connection attempts finished).\n");

    pthread_join(threads[0], NULL); // Wait for thread 1 to finish its work
    pthread_join(threads[1], NULL); // Wait for thread 2 to finish its work

    clock_gettime(CLOCK_MONOTONIC, &time_after);
    long long elapsed_ms = timespec_diff_ms(&time_before, &time_after);
    printf("Server: Total time elapsed: %lld ms\n", elapsed_ms);

    free(matrix);
    pthread_mutex_destroy(&server_ack_mutex);
    pthread_cond_destroy(&server_ack_cond);
}


// --- Client ---
void* client_send_to_child_thread(void* args) {
    ClientSendToChildThreadArgs* thread_args = (ClientSendToChildThreadArgs*)args;
    ClientInfo* target_child_info = find_client_info(thread_args->child_global_id);

    *(thread_args->child_ack_flag) = 0; // Initialize flag

    if (!target_child_info) {
        fprintf(stderr, "Client %d: Invalid target child ID %d (not found in config)\n",
                thread_args->sender_client_global_id, thread_args->child_global_id);
        free(thread_args->matrix_data_to_send); // Data was malloced if this is a copy, or it's a pointer into parent's data
                                                // The problem implies it's a pointer into parent's data, so NO free here.
        free(thread_args);
        return NULL;
    }
    // Corrected logging using sender_client_global_id
    printf("Client %d: Thread trying to connect to child %d (%s:%d)\n",
        thread_args->sender_client_global_id,
        target_child_info->id, target_child_info->ip, target_child_info->port);


    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) { perror("Client: Child socket creation failed"); free(thread_args); return NULL; }

    struct sockaddr_in child_serv_addr;
    child_serv_addr.sin_family = AF_INET;
    child_serv_addr.sin_port = htons(target_child_info->port);
    if (inet_pton(AF_INET, target_child_info->ip, &child_serv_addr.sin_addr) <= 0) {
        perror("Client: Invalid child address"); close(sock_fd); free(thread_args); return NULL;
    }

    if (connect(sock_fd, (struct sockaddr*)&child_serv_addr, sizeof(child_serv_addr)) < 0) {
        char err_buf[256];
        sprintf(err_buf, "Client %d: Connection Failed to child %d (%s:%d)", 
                thread_args->sender_client_global_id, target_child_info->id, target_child_info->ip, target_child_info->port);
        perror(err_buf);
        close(sock_fd); free(thread_args); return NULL;
    }

    printf("Client %d: Connected to child %d. Sending %d rows, %d cols.\n",
           thread_args->sender_client_global_id, target_child_info->id, thread_args->rows_to_send, thread_args->n_cols);
    // print_matrix_portion("Client sending to child", thread_args->matrix_data_to_send, thread_args->rows_to_send, thread_args->n_cols, MAX_PRINT_DIM);

    if (send_all(sock_fd, &thread_args->rows_to_send, sizeof(int)) <= 0) { close(sock_fd); free(thread_args); return NULL; }
    if (send_all(sock_fd, &thread_args->n_cols, sizeof(int)) <= 0) { close(sock_fd); free(thread_args); return NULL; }
    
    size_t data_size = (size_t)thread_args->rows_to_send * (size_t)thread_args->n_cols * sizeof(int);
    if (send_all(sock_fd, thread_args->matrix_data_to_send, data_size) <= 0) { close(sock_fd); free(thread_args); return NULL; }

    printf("Client %d: Matrix data sent to child %d. Waiting for ACK.\n",
           thread_args->sender_client_global_id, target_child_info->id);

    char ack_buf[ACK_LEN];
    if (recv_all(sock_fd, ack_buf, ACK_LEN) <= 0) {
        fprintf(stderr, "Client %d: Failed to receive ACK from child %d or connection closed.\n",
                thread_args->sender_client_global_id, target_child_info->id);
    } else if (strcmp(ack_buf, ACK_MSG) == 0) {
        printf("Client %d: ACK received from child %d.\n",
               thread_args->sender_client_global_id, target_child_info->id);
        *(thread_args->child_ack_flag) = 1;
    } else {
        fprintf(stderr, "Client %d: Invalid ACK from child %d: %s\n",
                thread_args->sender_client_global_id, target_child_info->id, ack_buf);
    }

    close(sock_fd);
    // The matrix_data_to_send is a pointer into the parent client's matrix, so DO NOT free it here.
    free(thread_args); // Free the arguments structure itself.
    return NULL;
}


void client_mode(int my_listen_port, int my_global_id) {
    // Config should be read by main before calling client_mode
    ClientInfo* myself = find_client_info(my_global_id); // Redundant if main validates well
    if(!myself){
        fprintf(stderr, "Client %d: Could not find own info in config (should not happen if main validates).\n", my_global_id);
        return;
    }
    
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("Client: Listener socket creation failed"); return; }

    int optval = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

    struct sockaddr_in my_addr;
    my_addr.sin_family = AF_INET;
    my_addr.sin_addr.s_addr = INADDR_ANY; 
    my_addr.sin_port = htons(my_listen_port);

    if (bind(listen_fd, (struct sockaddr*)&my_addr, sizeof(my_addr)) < 0) {
        char err_buf[256];
        sprintf(err_buf, "Client %d: Bind failed on port %d", my_global_id, my_listen_port);
        perror(err_buf); 
        close(listen_fd); return;
    }
    if (listen(listen_fd, 1) < 0) { 
        perror("Client: Listen failed"); close(listen_fd); return;
    }
    printf("Client %d: Listening on port %d\n", my_global_id, my_listen_port);

    struct sockaddr_in parent_addr;
    socklen_t parent_addr_len = sizeof(parent_addr);
    int parent_conn_fd = accept(listen_fd, (struct sockaddr*)&parent_addr, &parent_addr_len);
    if (parent_conn_fd < 0) {
        perror("Client: Accept failed"); close(listen_fd); return;
    }
    printf("Client %d: Accepted connection from parent/server %s:%d\n", my_global_id, 
           inet_ntoa(parent_addr.sin_addr), ntohs(parent_addr.sin_port));
    close(listen_fd); // Stop listening for more connections on this descriptor

    struct timespec time_before_client, time_after_client;
    clock_gettime(CLOCK_MONOTONIC, &time_before_client);

    int received_rows, received_cols;
    if (recv_all(parent_conn_fd, &received_rows, sizeof(int)) <= 0) { close(parent_conn_fd); return; }
    if (recv_all(parent_conn_fd, &received_cols, sizeof(int)) <= 0) { close(parent_conn_fd); return; }

    printf("Client %d: Receiving matrix of %d x %d\n", my_global_id, received_rows, received_cols);
    size_t received_data_size = (size_t)received_rows * (size_t)received_cols * sizeof(int);
    int* my_matrix_data = (int*)malloc(received_data_size);
    if (!my_matrix_data) { perror("Client: Malloc failed for received matrix"); close(parent_conn_fd); return; }

    if (recv_all(parent_conn_fd, my_matrix_data, received_data_size) <= 0) {
        free(my_matrix_data); close(parent_conn_fd); return;
    }
    // print_matrix_portion("Client received", my_matrix_data, received_rows, received_cols, MAX_PRINT_DIM);


    int children_global_ids[MAX_CLIENTS]; 
    int num_children;
    get_children_global_ids(my_global_id, g_total_clients_t, children_global_ids, &num_children);

    pthread_t* child_threads = NULL;
    int* child_ack_flags = NULL;
    size_t n_cols_size_t = (size_t)received_cols; // For safer indexing

    if (num_children > 0) {
        printf("Client %d: Has %d children. Distributing matrix...\n", my_global_id, num_children);
        child_threads = (pthread_t*)malloc(num_children * sizeof(pthread_t));
        child_ack_flags = (int*)calloc(num_children, sizeof(int)); 
        if (!child_threads || !child_ack_flags) {
            perror("Client: Malloc for child thread structures failed");
            free(my_matrix_data); 
            if(child_threads) free(child_threads); 
            if(child_ack_flags) free(child_ack_flags); 
            close(parent_conn_fd); return;
        }

        int* current_matrix_segment_ptr = my_matrix_data;
        int current_rows_for_distribution = received_rows;

        for (int i = 0; i < num_children; ++i) {
            int rows_for_this_child = current_rows_for_distribution / 2;
            // Data for child is the lower half of the current segment
            int* data_ptr_for_this_child = current_matrix_segment_ptr + (size_t)rows_for_this_child * n_cols_size_t;

            ClientSendToChildThreadArgs* child_args = (ClientSendToChildThreadArgs*)malloc(sizeof(ClientSendToChildThreadArgs));
            if (!child_args) {
                perror("Client: Malloc for child_args failed");
                // Mark this child as failed to process, potentially. For now, log and continue.
                // This would be a critical error, may need to abort or cleanup more carefully.
                child_ack_flags[i] = -1; // Indicate failure to launch/prepare
                continue; 
            }
            child_args->sender_client_global_id = my_global_id; // <--- FIX
            child_args->child_global_id = children_global_ids[i];
            child_args->matrix_data_to_send = data_ptr_for_this_child;
            child_args->rows_to_send = rows_for_this_child;
            child_args->n_cols = received_cols; 
            child_args->child_ack_flag = &child_ack_flags[i];
            
            if (pthread_create(&child_threads[i], NULL, client_send_to_child_thread, child_args) != 0) {
                perror("Client: Failed to create child thread");
                free(child_args); // Free args if thread creation failed
                child_ack_flags[i] = -1; // Indicate failure
            }
            // The current client retains the upper half
            current_rows_for_distribution /= 2; 
            // current_matrix_segment_ptr does not change, as we are always taking upper half of current.
        }
        
        int all_child_acks_ok = 1;
        for (int i = 0; i < num_children; ++i) {
            if (child_ack_flags[i] != -1) { // If thread was launched (or args malloced)
                pthread_join(child_threads[i], NULL); 
                if (child_ack_flags[i] == 0) { // Check if ACK was actually received
                     fprintf(stderr, "Client %d: Did not receive successful ACK from child %d.\n", my_global_id, children_global_ids[i]);
                     all_child_acks_ok = 0; // Or handle error more strictly
                }
            } else {
                fprintf(stderr, "Client %d: Thread for child %d was not successfully launched or prepared.\n", my_global_id, children_global_ids[i]);
                all_child_acks_ok = 0;
            }
        }
        if(all_child_acks_ok) printf("Client %d: All children processed and acknowledged.\n", my_global_id);
        else printf("Client %d: Some children did not process successfully.\n", my_global_id);
        
        // The remaining matrix for self is pointed to by my_matrix_data, with current_rows_for_distribution rows
        // print_matrix_portion("Client self portion after distribution", my_matrix_data, current_rows_for_distribution, received_cols, MAX_PRINT_DIM);
    } else {
        printf("Client %d: Is a leaf node. Retains all %d rows.\n", my_global_id, received_rows);
        // print_matrix_portion("Client self portion (leaf)", my_matrix_data, received_rows, received_cols, MAX_PRINT_DIM);
    }

    printf("Client %d: Sending ACK to parent/server.\n", my_global_id);
    if (send_all(parent_conn_fd, ACK_MSG, ACK_LEN) <= 0) {
        fprintf(stderr, "Client %d: Failed to send ACK to parent/server.\n", my_global_id);
    }

    clock_gettime(CLOCK_MONOTONIC, &time_after_client);
    long long elapsed_ms_client = timespec_diff_ms(&time_before_client, &time_after_client);
    printf("Client %d: Total time elapsed: %lld ms\n", my_global_id, elapsed_ms_client);

    close(parent_conn_fd);
    free(my_matrix_data); // Free the matrix data received by this client
    free(child_threads);  // free(NULL) is safe
    free(child_ack_flags); // free(NULL) is safe
}


// --- Main ---
int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage:\n");
        fprintf(stderr, "  Server: %s <n_matrix_size> 0\n", argv[0]);
        fprintf(stderr, "  Client: %s <listen_port> <client_id (>0)\n", argv[0]);
        return 1;
    }

    int arg1 = atoi(argv[1]);
    int arg2 = atoi(argv[2]);

    if (read_config(CONFIG_FILE) != 0) { 
        fprintf(stderr, "Fatal: Failed to read configuration file %s. Exiting.\n", CONFIG_FILE);
        return 1;
    }

    if (arg2 == 0) { // Server mode
        if (arg1 <= 0) {
            fprintf(stderr, "Server mode: Matrix size n must be positive.\n");
            return 1;
        }
        printf("Starting in SERVER mode. Matrix size: %d\n", arg1);
        server_mode(arg1);
    } else if (arg2 > 0) { // Client mode
        if (arg1 <= 0 || arg1 > 65535) {
            fprintf(stderr, "Client mode: Port must be between 1 and 65535.\n");
            return 1;
        }
        if (arg2 <= 0 || arg2 > g_total_clients_t) {
             fprintf(stderr, "Client mode: Client ID %d is invalid for %d total clients defined in config.\n", arg2, g_total_clients_t);
             return 1;
        }
        // Optional: Check if arg1 (listen_port) matches config port for client arg2
        // ClientInfo* my_config = find_client_info(arg2);
        // if (my_config && my_config->port != arg1) {
        //     printf("Warning: Client %d listening on port %d from arg, but config specifies port %d.\n", arg2, arg1, my_config->port);
        // }

        printf("Starting in CLIENT mode. Listening Port: %d, Client ID: %d\n", arg1, arg2);
        client_mode(arg1, arg2);
    } else {
        fprintf(stderr, "Invalid mode indicator (second argument). Must be 0 for server or >0 for client ID.\n");
        return 1;
    }

    return 0;
}
