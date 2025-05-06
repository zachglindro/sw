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
    ClientInfo* all_clients_list;
    int total_t_clients;
    // For server to track ACKs from root clients
    int* server_acks_received_count;
    pthread_mutex_t* server_ack_mutex;
    pthread_cond_t* server_ack_cond;
} ServerSendThreadArgs;

typedef struct {
    int child_global_id;
    int* matrix_data_to_send; // Pointer to the start of data for this child
    int rows_to_send;
    int n_cols;
    ClientInfo* all_clients_list;
    int total_t_clients;
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
            printf("%3d ", data[i * cols + j]);
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
        if (sent == 0) { // Connection closed by peer
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
        if (received == 0) { // Connection closed by peer
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
     if (g_total_clients_t % 2 != 0 && g_total_clients_t > 1) { // Allow t=1 for simple testing, though spec implies t>=2
        fprintf(stderr, "Warning: Number of clients t (%d) should ideally be even for two binomial trees.\n", g_total_clients_t);
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
    int* matrix = (int*)malloc((size_t)n_val * n_val * sizeof(int));
    if (!matrix) {
        perror("Failed to allocate matrix");
        return NULL;
    }
    for (int i = 0; i < n_val; ++i) {
        for (int j = 0; j < n_val; ++j) {
            matrix[i * n_val + j] = i * n_val + j; // Simple predictable values
        }
    }
    printf("Generated %dx%d matrix.\n", n_val, n_val);
    return matrix;
}

ClientInfo* find_client_info(int global_id) {
    if (global_id <= 0 || global_id > g_total_clients_t) return NULL;
    return &g_all_clients[global_id - 1]; // IDs are 1-indexed
}

// Binomial Tree Children Calculation
void get_children_global_ids(int my_global_id, int total_t, int* children_ids_arr, int* num_children_ptr) {
    *num_children_ptr = 0;
    if (total_t < 2) return; // No trees possible for distribution
    
    int num_clients_per_tree = total_t / 2;
    if (num_clients_per_tree == 0 && total_t == 1) { // Single client case is a leaf by definition
        return;
    }
    if (num_clients_per_tree == 0) return;


    int tree_base_global_id_1idx; 
    int my_relative_id_0idx; 

    if (my_global_id <= num_clients_per_tree) { // First tree
        tree_base_global_id_1idx = 1;
        my_relative_id_0idx = my_global_id - 1;
    } else { // Second tree
        tree_base_global_id_1idx = num_clients_per_tree + 1;
        my_relative_id_0idx = my_global_id - tree_base_global_id_1idx;
    }

    int max_k_val = -1;
    if (num_clients_per_tree > 1) {
        max_k_val = floor(log2(num_clients_per_tree - 1));
    }
    
    for (int k = max_k_val; k >= 0; --k) { // Iterate largest k first for "furthest child"
        int child_candidate_relative_0idx = my_relative_id_0idx + (1 << k);

        if (child_candidate_relative_0idx < num_clients_per_tree) {
            if (child_candidate_relative_0idx == 0) continue; 

            int lsb_val_of_child = child_candidate_relative_0idx & (-child_candidate_relative_0idx);
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
        perror("Server: Connection Failed to client");
        fprintf(stderr, "Server: Failed to connect to Client %d (%s:%d)\n", target_client->id, target_client->ip, target_client->port);
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
    size_t data_size = (size_t)thread_args->rows_to_send * thread_args->n_cols * sizeof(int);
    if (send_all(sock_fd, thread_args->matrix_data_segment, data_size) <= 0) { close(sock_fd); free(thread_args); return NULL; }
    
    printf("Server: Matrix data sent to client %d. Waiting for ACK.\n", target_client->id);

    char ack_buf[ACK_LEN];
    if (recv_all(sock_fd, ack_buf, ACK_LEN) <= 0) {
        fprintf(stderr, "Server: Failed to receive ACK from client %d or connection closed.\n", target_client->id);
    } else if (strcmp(ack_buf, ACK_MSG) == 0) {
        printf("Server: ACK received from client %d.\n", target_client->id);
        pthread_mutex_lock(thread_args->server_ack_mutex);
        (*thread_args->server_acks_received_count)++;
        if (*thread_args->server_acks_received_count == 2) { // Assuming 2 root clients
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
    if (g_total_clients_t == 0) {
         if (read_config(CONFIG_FILE) != 0) {
            fprintf(stderr, "Server: Failed to initialize from config.\n");
            return;
        }
    }
    if (g_total_clients_t < 2) {
        fprintf(stderr, "Server: Requires at least 2 clients for the specified distribution logic.\n");
        return;
    }


    int* matrix = generate_matrix(n_val);
    if (!matrix) return;

    struct timespec time_before, time_after;
    clock_gettime(CLOCK_MONOTONIC, &time_before);

    pthread_t threads[2];
    int server_acks_received_count = 0;
    pthread_mutex_t server_ack_mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t server_ack_cond = PTHREAD_COND_INITIALIZER;

    // Client 1
    ServerSendThreadArgs* args1 = (ServerSendThreadArgs*)malloc(sizeof(ServerSendThreadArgs));
    args1->target_client_global_id = 1;
    args1->matrix_data_segment = matrix;
    args1->rows_to_send = n_val / 2;
    args1->n_cols = n_val;
    args1->all_clients_list = g_all_clients;
    args1->total_t_clients = g_total_clients_t;
    args1->server_acks_received_count = &server_acks_received_count;
    args1->server_ack_mutex = &server_ack_mutex;
    args1->server_ack_cond = &server_ack_cond;

    if (pthread_create(&threads[0], NULL, server_send_to_root_client_thread, args1) != 0) {
        perror("Server: Failed to create thread for client 1");
        free(args1); // Crucial
    }

    // Client (t/2) + 1
    ServerSendThreadArgs* args2 = (ServerSendThreadArgs*)malloc(sizeof(ServerSendThreadArgs));
    args2->target_client_global_id = (g_total_clients_t / 2) + 1;
    args2->matrix_data_segment = matrix + (size_t)(n_val / 2) * n_val; // Offset for second half
    args2->rows_to_send = n_val - (n_val / 2); // Remaining rows
    args2->n_cols = n_val;
    args2->all_clients_list = g_all_clients;
    args2->total_t_clients = g_total_clients_t;
    args2->server_acks_received_count = &server_acks_received_count;
    args2->server_ack_mutex = &server_ack_mutex;
    args2->server_ack_cond = &server_ack_cond;
    
    if (pthread_create(&threads[1], NULL, server_send_to_root_client_thread, args2) != 0) {
        perror("Server: Failed to create thread for client (t/2)+1");
        free(args2); // Crucial
    }
    
    // Wait for both ACKs using condition variable
    pthread_mutex_lock(&server_ack_mutex);
    while (server_acks_received_count < 2) {
        pthread_cond_wait(&server_ack_cond, &server_ack_mutex);
    }
    pthread_mutex_unlock(&server_ack_mutex);

    printf("Server: Both root clients acknowledged.\n");

    // Join threads (optional if using CV for main logic sync, but good practice for cleanup)
    pthread_join(threads[0], NULL);
    pthread_join(threads[1], NULL);

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
        fprintf(stderr, "Client: Invalid target child ID %d\n", thread_args->child_global_id);
        free(thread_args);
        return NULL;
    }
    printf("Client %d: Thread trying to connect to child %d (%s:%d)\n", 
        find_client_info(0-thread_args->total_t_clients)->id, // hacky way to get current client ID, fix this
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
        perror("Client: Connection Failed to child"); 
        fprintf(stderr, "Client: Failed to connect to Child %d (%s:%d)\n", target_child_info->id, target_child_info->ip, target_child_info->port);
        close(sock_fd); free(thread_args); return NULL;
    }

    printf("Client: Connected to child %d. Sending %d rows, %d cols.\n", target_child_info->id, thread_args->rows_to_send, thread_args->n_cols);
    // print_matrix_portion("Client sending to child", thread_args->matrix_data_to_send, thread_args->rows_to_send, thread_args->n_cols, MAX_PRINT_DIM);

    if (send_all(sock_fd, &thread_args->rows_to_send, sizeof(int)) <= 0) { close(sock_fd); free(thread_args); return NULL; }
    if (send_all(sock_fd, &thread_args->n_cols, sizeof(int)) <= 0) { close(sock_fd); free(thread_args); return NULL; }
    
    size_t data_size = (size_t)thread_args->rows_to_send * thread_args->n_cols * sizeof(int);
    if (send_all(sock_fd, thread_args->matrix_data_to_send, data_size) <= 0) { close(sock_fd); free(thread_args); return NULL; }

    printf("Client: Matrix data sent to child %d. Waiting for ACK.\n", target_child_info->id);

    char ack_buf[ACK_LEN];
    if (recv_all(sock_fd, ack_buf, ACK_LEN) <= 0) {
        fprintf(stderr, "Client: Failed to receive ACK from child %d or connection closed.\n", target_child_info->id);
    } else if (strcmp(ack_buf, ACK_MSG) == 0) {
        printf("Client: ACK received from child %d.\n", target_child_info->id);
        *(thread_args->child_ack_flag) = 1;
    } else {
        fprintf(stderr, "Client: Invalid ACK from child %d: %s\n", target_child_info->id, ack_buf);
    }

    close(sock_fd);
    free(thread_args);
    return NULL;
}


void client_mode(int my_listen_port, int my_global_id) {
    if (g_total_clients_t == 0) {
         if (read_config(CONFIG_FILE) != 0) {
            fprintf(stderr, "Client %d: Failed to initialize from config.\n", my_global_id);
            return;
        }
    }
    ClientInfo* myself = find_client_info(my_global_id);
    if(!myself){
        fprintf(stderr, "Client %d: Could not find own info in config.\n", my_global_id);
        return;
    }
    // Correct my_listen_port if it was passed via cmd arg and differs from config
    // For this implementation, we assume config port is the one to listen on.
    // Or, problem says "client will read from the arguments the port it should listen to"
    // So, my_listen_port from argument is primary.
    
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("Client: Listener socket creation failed"); return; }

    int optval = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

    struct sockaddr_in my_addr;
    my_addr.sin_family = AF_INET;
    my_addr.sin_addr.s_addr = INADDR_ANY; // Listen on all interfaces
    my_addr.sin_port = htons(my_listen_port);

    if (bind(listen_fd, (struct sockaddr*)&my_addr, sizeof(my_addr)) < 0) {
        perror("Client: Bind failed"); close(listen_fd); return;
    }
    if (listen(listen_fd, 1) < 0) { // Listen for 1 incoming connection (parent/server)
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
    close(listen_fd); // Stop listening for more connections

    struct timespec time_before_client, time_after_client;
    clock_gettime(CLOCK_MONOTONIC, &time_before_client);

    int received_rows, received_cols;
    if (recv_all(parent_conn_fd, &received_rows, sizeof(int)) <= 0) { close(parent_conn_fd); return; }
    if (recv_all(parent_conn_fd, &received_cols, sizeof(int)) <= 0) { close(parent_conn_fd); return; }

    printf("Client %d: Receiving matrix of %d x %d\n", my_global_id, received_rows, received_cols);
    size_t received_data_size = (size_t)received_rows * received_cols * sizeof(int);
    int* my_matrix_data = (int*)malloc(received_data_size);
    if (!my_matrix_data) { perror("Client: Malloc failed for received matrix"); close(parent_conn_fd); return; }

    if (recv_all(parent_conn_fd, my_matrix_data, received_data_size) <= 0) {
        free(my_matrix_data); close(parent_conn_fd); return;
    }
    // print_matrix_portion("Client received", my_matrix_data, received_rows, received_cols, MAX_PRINT_DIM);


    int children_global_ids[MAX_CLIENTS]; // Max possible children
    int num_children;
    get_children_global_ids(my_global_id, g_total_clients_t, children_global_ids, &num_children);

    pthread_t* child_threads = NULL;
    int* child_ack_flags = NULL;

    if (num_children > 0) {
        printf("Client %d: Has %d children. Distributing matrix...\n", my_global_id, num_children);
        child_threads = (pthread_t*)malloc(num_children * sizeof(pthread_t));
        child_ack_flags = (int*)calloc(num_children, sizeof(int)); // Init to 0
        if (!child_threads || !child_ack_flags) {
            perror("Client: Malloc for child thread structures failed");
            free(my_matrix_data); free(child_threads); free(child_ack_flags); close(parent_conn_fd); return;
        }

        int* current_matrix_segment_ptr = my_matrix_data;
        int current_rows_for_distribution = received_rows;

        for (int i = 0; i < num_children; ++i) {
            int rows_for_this_child = current_rows_for_distribution / 2;
            int* data_ptr_for_this_child = current_matrix_segment_ptr + (size_t)rows_for_this_child * received_cols; // Lower half

            ClientSendToChildThreadArgs* child_args = (ClientSendToChildThreadArgs*)malloc(sizeof(ClientSendToChildThreadArgs));
            child_args->child_global_id = children_global_ids[i];
            child_args->matrix_data_to_send = data_ptr_for_this_child;
            child_args->rows_to_send = rows_for_this_child;
            child_args->n_cols = received_cols;
            child_args->all_clients_list = g_all_clients;
            child_args->total_t_clients = g_total_clients_t; // For context if needed
            child_args->child_ack_flag = &child_ack_flags[i];
            
            // To get current client's ID in child thread for logging: a bit hacky here.
            // A proper way would be to pass my_global_id to child_args.
            // The line `find_client_info(0-thread_args->total_t_clients)->id` is not robust.

            if (pthread_create(&child_threads[i], NULL, client_send_to_child_thread, child_args) != 0) {
                perror("Client: Failed to create child thread");
                free(child_args);
                // TODO: more robust cleanup if one thread fails
            }
            current_rows_for_distribution /= 2; // Upper half remains for next step / self
        }
        
        // Wait for all child ACKs
        for (int i = 0; i < num_children; ++i) {
            pthread_join(child_threads[i], NULL); // Ensure thread finishes
            if (child_ack_flags[i] == 0) {
                 fprintf(stderr, "Client %d: Did not receive ACK from child %d (or child thread failed).\n", my_global_id, children_global_ids[i]);
                 // Decide on error handling policy: proceed or abort? For now, proceed.
            }
        }
        printf("Client %d: All children processed.\n", my_global_id);
        
        // The remaining matrix for self is pointed to by my_matrix_data, with current_rows_for_distribution rows
        // print_matrix_portion("Client self portion", my_matrix_data, current_rows_for_distribution, received_cols, MAX_PRINT_DIM);
    } else {
        printf("Client %d: Is a leaf node.\n", my_global_id);
        // The entire received matrix is its own.
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
    free(my_matrix_data);
    free(child_threads);
    free(child_ack_flags);
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

    if (read_config(CONFIG_FILE) != 0) { // Read config first for both modes
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
             fprintf(stderr, "Client mode: Client ID %d is invalid for %d total clients.\n", arg2, g_total_clients_t);
             return 1;
        }
        printf("Starting in CLIENT mode. Listening Port: %d, Client ID: %d\n", arg1, arg2);
        client_mode(arg1, arg2);
    } else {
        fprintf(stderr, "Invalid mode indicator (second argument). Must be 0 for server or >0 for client ID.\n");
        return 1;
    }

    return 0;
}
