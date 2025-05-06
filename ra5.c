#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <stdint.h>
#include <math.h>
#include <errno.h>
#include <stdbool.h>
#include <limits.h> // For INT_MAX check

// --- Configuration ---
#define CONFIG_FILE "config.txt"
#define ACK_MSG "ACK" // Not used for final result, maybe initial handshake?
#define ACK_LEN 3
#define MAX_CLIENTS 128
#define DEFAULT_Q 10 // Default Moving Average Order if not specified

// --- Structures ---
typedef struct {
    char ip[INET_ADDRSTRLEN];
    int port;
    int id; // Client ID (1-based)
} ClientInfo;

typedef struct {
    int n; // Matrix dimension (nxn)
    int q; // Moving average order
    int total_clients;
    ClientInfo *clients; // Array of all clients
} Config;

// Structure to hold matrix data contiguously for easier column handling
typedef struct {
    double *data; // Store elements contiguously (row-major assumed here)
    uint32_t rows;
    uint32_t cols;
} Matrix;

typedef struct {
    int client_sock;
    Matrix matrix_part; // Matrix columns to send
    int target_client_id;
    Config *config;
    double *result_vector_part; // Pointer to where the server should store the received result
    pthread_mutex_t *result_mutex; // To protect writing to the result vector part
} ServerCommThreadArgs;

typedef struct {
    int parent_sock; // Socket connected to the parent/server
    int client_id;
    int port_to_listen;
    Config *config;
    double computation_time; // To store time spent ONLY on MA calculation
} ClientArgs;

typedef struct {
    int child_id;
    Matrix matrix_part; // Columns to send to child
    Config *config;
    // FIX 1: Change type to double**
    double **received_child_vector; // Pointer to store result vector pointer received FROM child
    uint32_t *received_child_vector_size; // Pointer to store the size
    pthread_mutex_t *child_result_mutex; // Protect access to received vector storage
} ClientCommThreadArgs;

// --- Global Variables ---
// None preferred

// --- Error Handling & Time Helper ---
void error_exit(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

double time_diff(struct timeval *start, struct timeval *end) {
    return (end->tv_sec - start->tv_sec) + (end->tv_usec - start->tv_usec) / 1000000.0;
}

// --- Matrix Helpers ---

// Allocate matrix structure and data buffer
Matrix create_matrix(uint32_t rows, uint32_t cols) {
    Matrix m;
    m.rows = rows;
    m.cols = cols;
    // Handle 0-size allocation request gracefully
    if (rows == 0 || cols == 0) {
        m.data = NULL;
        return m;
    }
    m.data = malloc((size_t)rows * cols * sizeof(double)); // Use double for MA results
    if (!m.data) {
        perror("Failed to allocate matrix data");
        m.rows = m.cols = 0; // Indicate failure
    }
    return m;
}

void free_matrix(Matrix *m) {
    if (m && m->data) {
        free(m->data);
        m->data = NULL;
        m->rows = m->cols = 0;
    }
}

// Get element (assuming row-major storage)
double get_matrix_element(const Matrix *m, uint32_t r, uint32_t c) {
    // Add bounds checking for safety
    if (!m || !m->data || r >= m->rows || c >= m->cols) {
         fprintf(stderr, "WARN: get_matrix_element out of bounds (%u, %u) for size (%u, %u)\n", r, c, m?m->rows:0, m?m->cols:0);
         return NAN; // Error or invalid access
    }
    return m->data[(size_t)r * m->cols + c];
}

// Set element (assuming row-major storage)
void set_matrix_element(Matrix *m, uint32_t r, uint32_t c, double value) {
     // Add bounds checking for safety
    if (!m || !m->data || r >= m->rows || c >= m->cols) {
         fprintf(stderr, "WARN: set_matrix_element out of bounds (%u, %u) for size (%u, %u)\n", r, c, m?m->rows:0, m?m->cols:0);
         return;
    }
    m->data[(size_t)r * m->cols + c] = value;
}

// Extract a sub-matrix containing specific columns from a source matrix
Matrix extract_columns(const Matrix *source, uint32_t start_col, uint32_t num_cols) {
    if (!source || !source->data || start_col > source->cols || start_col + num_cols > source->cols || num_cols == 0) {
         fprintf(stderr, "WARN: Invalid parameters for extract_columns start=%u, num=%u, source_cols=%u\n", start_col, num_cols, source?source->cols:0);
        return create_matrix(0, 0); // Return empty matrix on error
    }
    Matrix dest = create_matrix(source->rows, num_cols);
    if (!dest.data && !(source->rows == 0 || num_cols == 0)) return dest; // Allocation failed check (unless 0xN or Nx0)

    // Optimization: If extracting all columns, just copy the whole data
    if (start_col == 0 && num_cols == source->cols) {
        memcpy(dest.data, source->data, (size_t)source->rows * source->cols * sizeof(double));
    } else {
        // Copy column by column (less efficient but general)
        // This could be optimized using memcpy for contiguous blocks if memory layout allows
         for (uint32_t r = 0; r < source->rows; ++r) {
            for (uint32_t c = 0; c < num_cols; ++c) {
                // Read from source column (start_col + c)
                double val = get_matrix_element(source, r, start_col + c);
                // Write to destination column c
                set_matrix_element(&dest, r, c, val);
            }
        }
    }
    return dest;
}


// --- Network Helpers (Adapted for Matrix/Vector) ---
int send_all(int sockfd, const void *buf, size_t len); // Declaration (implementation unchanged)
int recv_all(int sockfd, void *buf, size_t len);     // Declaration (implementation unchanged)

// Send matrix dimensions and data
int send_matrix_data(int sockfd, const Matrix *m) {
    uint32_t net_rows = htonl(m->rows);
    uint32_t net_cols = htonl(m->cols);

    if (send_all(sockfd, &net_rows, sizeof(net_rows)) == -1) return -1;
    if (send_all(sockfd, &net_cols, sizeof(net_cols)) == -1) return -1;

    // Only send data if dimensions are non-zero
    if (m->rows > 0 && m->cols > 0) {
        size_t matrix_size_bytes = (size_t)m->rows * m->cols * sizeof(double);
        // Assuming doubles don't need byte swapping or using a portable format (like text or XDR) is better.
        // For simplicity, sending raw bytes. Requires sender/receiver to have same double representation.
        if (send_all(sockfd, m->data, matrix_size_bytes) == -1) return -1;
         printf("Sent matrix dimensions (%u x %u) and data (%zu bytes) to socket %d\n", m->rows, m->cols, matrix_size_bytes, sockfd);
    } else {
         printf("Sent matrix dimensions (%u x %u) (zero size, no data) to socket %d\n", m->rows, m->cols, sockfd);
    }


    return 0;
}

// Receive matrix dimensions and data
Matrix recv_matrix_data(int sockfd) {
    uint32_t net_rows, net_cols;
    uint32_t rows, cols;

    if (recv_all(sockfd, &net_rows, sizeof(net_rows)) == -1) return create_matrix(0,0);
    if (recv_all(sockfd, &net_cols, sizeof(net_cols)) == -1) return create_matrix(0,0);

    rows = ntohl(net_rows);
    cols = ntohl(net_cols);

    // Handle receiving 0 dimensions correctly
    if (rows == 0 || cols == 0) {
        printf("Received matrix dimensions (%u x %u) (zero size) from socket %d\n", rows, cols, sockfd);
        return create_matrix(rows, cols); // Return valid but empty matrix
    }

    Matrix m = create_matrix(rows, cols);
    if (!m.data) {
        return m; // Allocation failed
    }

    size_t matrix_size_bytes = (size_t)rows * cols * sizeof(double);
    if (recv_all(sockfd, m.data, matrix_size_bytes) == -1) {
        free_matrix(&m);
        return create_matrix(0,0);
    }

    printf("Received matrix dimensions (%u x %u) and data (%zu bytes) from socket %d\n", rows, cols, matrix_size_bytes, sockfd);
    return m;
}

// Send vector size and data
int send_vector(int sockfd, const double *vec, uint32_t size) {
    uint32_t net_size = htonl(size);
    if (send_all(sockfd, &net_size, sizeof(net_size)) == -1) return -1;

    if (size > 0) {
        size_t vector_size_bytes = (size_t)size * sizeof(double);
        if (send_all(sockfd, vec, vector_size_bytes) == -1) return -1;
        printf("Sent vector size (%u) and data (%zu bytes) to socket %d\n", size, vector_size_bytes, sockfd);
    } else {
        printf("Sent vector size (0) (no data) to socket %d\n", sockfd);
    }
    return 0;
}

// Receive vector size and data. Allocates memory for the vector. Caller must free.
double* recv_vector(int sockfd, uint32_t *size) {
    uint32_t net_size;
    if (recv_all(sockfd, &net_size, sizeof(net_size)) == -1) {
        *size = 0;
        return NULL;
    }
    *size = ntohl(net_size);

    if (*size == 0) {
         printf("Received vector size (0) (no data) from socket %d\n", sockfd);
        return NULL; // Return NULL for 0 size, consistent with malloc(0) behavior uncertainty
    }

    size_t vector_size_bytes = (size_t)(*size) * sizeof(double);
    double *vec = malloc(vector_size_bytes);
    if (!vec) {
        perror("malloc for received vector failed");
        *size = 0; // Reflect failure
        // Should try to consume data from socket? Difficult to know size. Best effort: close socket?
        // For now, return NULL. Might cause issues upstream.
        return NULL;
    }

    if (recv_all(sockfd, vec, vector_size_bytes) == -1) {
        free(vec);
        *size = 0;
        return NULL;
    }
    printf("Received vector size (%u) and data (%zu bytes) from socket %d\n", *size, vector_size_bytes, sockfd);
    return vec;
}


// --- Binomial Tree Helpers ---
int find_parent(int client_id, int total_clients);      // Declaration (implementation unchanged)
int* find_children(int client_id, int total_clients); // Declaration (implementation unchanged)

// --- Config Reader ---
Config* read_config(const char *filename, int n_arg, int q_arg); // Added n, q args
void free_config(Config *config);                      // Declaration (implementation unchanged)

// --- Moving Average Computation ---
// Computes the Order Q trailing moving average for each column in the matrix.
// Returns a vector containing the *last* MA value computed for each column.
// Also calculates and returns the computation time.
double* compute_moving_average(const Matrix *m, int Q, double *computation_time) {
    if (!m || m->rows == 0 || m->cols == 0) {
        // Handle empty matrix case: return empty result, 0 time
        printf("Computed MAs for 0 columns (empty input matrix). Time: 0.000000 s\n");
        *computation_time = 0.0;
        return NULL; // Return NULL for 0 columns, consistent with recv_vector
    }
     if (!m->data || Q <= 0 || Q > m->rows) {
        fprintf(stderr, "Invalid input for moving average computation (Q=%d, rows=%u).\n", Q, m->rows);
        *computation_time = 0.0;
        return NULL;
    }


    struct timeval start_time, end_time;
    gettimeofday(&start_time, NULL);

    double *result_vector = malloc(m->cols * sizeof(double));
    if (!result_vector) {
        perror("Failed to allocate result vector for moving average");
        *computation_time = 0.0;
        return NULL;
    }

    for (uint32_t j = 0; j < m->cols; ++j) { // Iterate through columns
        double current_sum = 0.0;
        // Handle Q=1 case separately for efficiency (no loop needed)
        if (Q == 1) {
             current_sum = get_matrix_element(m, m->rows - 1, j);
        } else {
            // Calculate initial sum for the first MA value (at row Q-1)
            for (uint32_t i = 0; i < Q; ++i) {
                current_sum += get_matrix_element(m, i, j);
            }
            // Slide the window if necessary (more than Q rows)
            for (uint32_t i = Q; i < m->rows; ++i) {
                current_sum -= get_matrix_element(m, i - Q, j); // Subtract element leaving window
                current_sum += get_matrix_element(m, i, j);     // Add element entering window
            }
        }
        // Store the *last* computed MA value for this column
        result_vector[j] = current_sum / Q;
    }

    gettimeofday(&end_time, NULL);
    *computation_time = time_diff(&start_time, &end_time);

    printf("Computed MAs for %u columns (Q=%d). Time: %.6f s\n", m->cols, Q, *computation_time);
    return result_vector;
}


// --- Server Logic ---

// Thread function: Sends column slice, receives result vector part
void* server_comm_thread(void *arg) {
    ServerCommThreadArgs *args = (ServerCommThreadArgs *)arg;
    int client_sock = -1;
    struct sockaddr_in cli_addr;
    ClientInfo *target_client = NULL;
    double *received_p_part = NULL;
    uint32_t received_p_size = 0;
    void* thread_return_status = (void*)-1; // Default to failure

    // Find target client info
    for (int i = 0; i < args->config->total_clients; ++i) {
        if (args->config->clients[i].id == args->target_client_id) {
            target_client = &args->config->clients[i];
            break;
        }
    }
    if (!target_client) {
        fprintf(stderr, "Server: Could not find client info for ID %d\n", args->target_client_id);
        goto cleanup; // Use goto for cleanup
    }

    // --- Connect to Client ---
    client_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (client_sock < 0) { perror("Server: socket"); goto cleanup; }
    memset(&cli_addr, 0, sizeof(cli_addr));
    cli_addr.sin_family = AF_INET;
    cli_addr.sin_port = htons(target_client->port);
    if (inet_pton(AF_INET, target_client->ip, &cli_addr.sin_addr) <= 0) {
        fprintf(stderr, "Server: Invalid address for client %d\n", args->target_client_id); goto cleanup;
    }
    printf("Server: Connecting to Client %d (%s:%d)...\n", args->target_client_id, target_client->ip, target_client->port);
    if (connect(client_sock, (struct sockaddr *)&cli_addr, sizeof(cli_addr)) < 0) {
        perror("Server: connect failed"); goto cleanup;
    }
    printf("Server: Connected successfully to Client %d.\n", args->target_client_id);

    // --- Send Matrix Columns ---
    if (send_matrix_data(client_sock, &args->matrix_part) != 0) {
        fprintf(stderr, "Server: Failed to send matrix columns to Client %d\n", args->target_client_id);
        goto cleanup;
    }
    printf("Server: Matrix columns sent to Client %d.\n", args->target_client_id);


    // --- Wait for Result Vector ---
    printf("Server: Waiting for result vector from Client %d...\n", args->target_client_id);
    received_p_part = recv_vector(client_sock, &received_p_size);

    if (!received_p_part && received_p_size > 0) { // Check if recv failed but expected data
        fprintf(stderr, "Server: Failed to receive result vector from Client %d\n", args->target_client_id);
        goto cleanup;
    } else if (received_p_part == NULL && received_p_size == 0) {
         printf("Server: Received empty result vector (size 0) from Client %d.\n", args->target_client_id);
         // This might be valid if the client received 0 columns, handle based on expected size below
    } else {
        printf("Server: Received result vector (size %u) from Client %d.\n", received_p_size, args->target_client_id);
    }


    // --- Store Received Vector ---
    // Calculate expected size based on columns sent
    uint32_t expected_p_size = args->matrix_part.cols;

    if (received_p_size != expected_p_size) {
         fprintf(stderr, "Server: Received vector size %u from client %d, expected %u (based on cols sent)\n",
                 received_p_size, args->target_client_id, expected_p_size);
         goto cleanup;
    }

    // Copy received data (only if size > 0)
    if (expected_p_size > 0) {
        pthread_mutex_lock(args->result_mutex);
        memcpy(args->result_vector_part, received_p_part, received_p_size * sizeof(double));
        pthread_mutex_unlock(args->result_mutex);
    }

    thread_return_status = (void*)0; // Success

cleanup:
    // Free resources allocated in this thread
    free(received_p_part); // Free the buffer allocated by recv_vector (safe if NULL)
    free_matrix(&args->matrix_part); // Free the matrix columns sent (server side copy)
    if (client_sock >= 0) {
        close(client_sock);
    }
    pthread_exit(thread_return_status);
}

void run_server(int n, int q) {
    printf("--- Running as SERVER ---\n");
    printf("Matrix size n = %d, Moving Average Order Q = %d\n", n, q);

    // Read config
    Config *config = read_config(CONFIG_FILE, n, q);
    if (!config) exit(EXIT_FAILURE);
    // Basic validations (T power of 2, n divisible by T, Q <= n etc.)
    if (config->total_clients <= 0 || ((config->total_clients & (config->total_clients - 1)) != 0 && config->total_clients > 1)) {
         fprintf(stderr, "Server Error: T (%d) must be > 0 and a power of 2.\n", config->total_clients); free_config(config); exit(EXIT_FAILURE);
    }
     if (n % config->total_clients != 0) {
         fprintf(stderr, "Server Error: n (%d) must be divisible by T (%d).\n", n, config->total_clients); free_config(config); exit(EXIT_FAILURE);
     }
     if (q <= 0 || q > n) {
          fprintf(stderr, "Server Error: Q (%d) must be between 1 and n (%d).\n", q, n); free_config(config); exit(EXIT_FAILURE);
     }

    // Allocate and initialize original matrix X
    printf("Server: Allocating %d x %d matrix...\n", n, n);
    Matrix matrix_x = create_matrix(n, n);
     if (!matrix_x.data && n>0) { free_config(config); exit(EXIT_FAILURE); } // Check allocation unless n=0

    printf("Server: Initializing matrix X...\n");
    for (size_t i = 0; i < n; ++i) {
        for (size_t j = 0; j < n; ++j) {
            // Initialize with some values (e.g., doubles)
            set_matrix_element(&matrix_x, i, j, (double)(i * n + j)); // Example init
        }
    }
    printf("Server: Matrix X created successfully.\n");

    // Allocate space for the final result vector 'p'
    double *final_p = NULL;
    if (n > 0) { // Only allocate if matrix has columns
        final_p = malloc(n * sizeof(double));
        if (!final_p) { error_exit("Server: Failed to allocate final result vector p"); }
    }
    pthread_mutex_t result_mutex = PTHREAD_MUTEX_INITIALIZER;


    // Prepare arguments for sender/receiver threads
    pthread_t tid1, tid2 = 0; // Initialize tid2
    ServerCommThreadArgs args1 = {0}, args2 = {0}; // Initialize args
    int root_client_1_id = 1;
    int root_client_2_id = (config->total_clients == 1) ? -1 : (config->total_clients / 2) + 1;
    uint32_t cols_per_root = (config->total_clients == 1) ? n : n / 2;

    // Extract columns for client 1
    printf("Server: Extracting columns 0 to %u for Client %d\n", cols_per_root - 1, root_client_1_id);
    args1.matrix_part = extract_columns(&matrix_x, 0, cols_per_root);
    // Check extraction success (handles cols_per_root=0 case)
    if (!args1.matrix_part.data && cols_per_root > 0) error_exit("Server: Failed to extract columns for client 1");
    args1.target_client_id = root_client_1_id;
    args1.config = config;
    args1.result_vector_part = final_p; // Client 1 results go at the start
    args1.result_mutex = &result_mutex;

    if (root_client_2_id > 0) {
        uint32_t start_col_2 = cols_per_root;
        uint32_t num_cols_2 = n - cols_per_root;
        printf("Server: Extracting columns %u to %u for Client %d\n", start_col_2, n - 1, root_client_2_id);
        args2.matrix_part = extract_columns(&matrix_x, start_col_2, num_cols_2);
        if (!args2.matrix_part.data && num_cols_2 > 0) error_exit("Server: Failed to extract columns for client 2");
        args2.target_client_id = root_client_2_id;
        args2.config = config;
        args2.result_vector_part = final_p + cols_per_root; // Client 2 results go in the second half
        args2.result_mutex = &result_mutex;
    }

    // Original matrix X no longer needed if columns were copied
    free_matrix(&matrix_x); // Free the large initial matrix

    // Record time before starting communication
    struct timeval time_before, time_after;
    gettimeofday(&time_before, NULL);
    printf("Server: Starting communication at %ld.%06ld\n", time_before.tv_sec, time_before.tv_usec);

    // Launch threads
    printf("Server: Launching thread for Client %d\n", args1.target_client_id);
    if (pthread_create(&tid1, NULL, server_comm_thread, &args1) != 0) {
        // If thread 1 fails, free its matrix part explicitly
        free_matrix(&args1.matrix_part);
        error_exit("Server: Failed to create thread for client 1");
    }
    if (root_client_2_id > 0) {
         printf("Server: Launching thread for Client %d\n", args2.target_client_id);
        if (pthread_create(&tid2, NULL, server_comm_thread, &args2) != 0) {
            // Clean up thread 1? Robust cleanup is complex. Exit for simplicity.
            free_matrix(&args2.matrix_part); // Free matrix for thread 2
            perror("Server: Failed to create thread for client 2");
            // Attempt to cancel/join thread 1? Or just exit?
            exit(EXIT_FAILURE);
        }
    }

    // Wait for threads to complete (receive results)
    void *ret1, *ret2 = (void*)0;
    pthread_join(tid1, &ret1);
    if (root_client_2_id > 0) {
        pthread_join(tid2, &ret2);
    }

    // Server work (rebuilding vector p) is implicitly done when threads write results.

    // Record time after receiving results and threads joined
    gettimeofday(&time_after, NULL);
    printf("Server: Communication and rebuild finished at %ld.%06ld\n", time_after.tv_sec, time_after.tv_usec);

    // Check thread results
    if (ret1 != (void*)0 || (root_client_2_id > 0 && ret2 != (void*)0)) {
        fprintf(stderr, "Server: One or more communication threads failed. Exiting.\n");
        // Note: Matrix parts inside args were freed by the threads themselves
        free(final_p);
        pthread_mutex_destroy(&result_mutex);
        free_config(config);
        exit(EXIT_FAILURE);
    }
    pthread_mutex_destroy(&result_mutex);

    printf("Server: All result vectors received successfully.\n");
    // Optional: Print first few elements of final_p for verification
    if (n > 0) {
        printf("Server: Final vector p (first 10 elements): [ ");
        for(int i=0; i<n && i<10; ++i) printf("%.2f ", final_p[i]);
        printf("... ]\n");
    } else {
        printf("Server: Final vector p is empty (n=0).\n");
    }


    // Calculate and print elapsed time
    double elapsed = time_diff(&time_before, &time_after);
    printf("Server: Total time elapsed: %.6f seconds\n", elapsed);
    // Output for Table 1 (append to a file or just print)
    printf("TABLE1_DATA: n=%d t=%d q=%d time=%.6f\n", config->n, config->total_clients, config->q, elapsed);


    // Cleanup
    free(final_p);
    free_config(config);
    printf("--- Server finished ---\n");
}


// --- Client Logic ---

// Thread function: Sends column slice to child, receives result vector back
void* client_comm_child_thread(void *arg) {
    ClientCommThreadArgs *args = (ClientCommThreadArgs *)arg;
    int child_sock = -1;
    struct sockaddr_in child_addr;
    ClientInfo *child_info = NULL;
    double *child_p = NULL;
    uint32_t child_p_size = 0;
    void* thread_return_status = (void*)-1; // Default to failure


     // Find child client info
    for (int i = 0; i < args->config->total_clients; ++i) {
        if (args->config->clients[i].id == args->child_id) {
            child_info = &args->config->clients[i];
            break;
        }
    }
    if (!child_info) {
        fprintf(stderr, "Client Child Comm: Could not find info for child ID %d\n", args->child_id);
        goto cleanup_client_comm;
    }

    // --- Connect to child ---
    child_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (child_sock < 0) { perror("Client Child Comm: socket"); goto cleanup_client_comm; }
    memset(&child_addr, 0, sizeof(child_addr));
    child_addr.sin_family = AF_INET;
    child_addr.sin_port = htons(child_info->port);
    if (inet_pton(AF_INET, child_info->ip, &child_addr.sin_addr) <= 0) {
        fprintf(stderr, "Client Child Comm: Invalid address for child %d\n", args->child_id); goto cleanup_client_comm;
    }
    printf("Client Child Comm: Connecting to Child %d (%s:%d)...\n", args->child_id, child_info->ip, child_info->port);
    if (connect(child_sock, (struct sockaddr *)&child_addr, sizeof(child_addr)) < 0) {
       perror("Client Child Comm: connect failed"); goto cleanup_client_comm;
    }
    printf("Client Child Comm: Connected successfully to Child %d.\n", args->child_id);

    // --- Send Matrix Columns to Child ---
    if (send_matrix_data(child_sock, &args->matrix_part) != 0) {
        fprintf(stderr, "Client Child Comm: Failed to send matrix to Child %d\n", args->child_id);
        goto cleanup_client_comm;
    }
    printf("Client Child Comm: Matrix columns sent to Child %d.\n", args->child_id);


    // --- Wait for Result Vector from Child ---
    printf("Client Child Comm: Waiting for result vector from Child %d...\n", args->child_id);
    child_p = recv_vector(child_sock, &child_p_size); // child_p is allocated by recv_vector

    if (!child_p && child_p_size > 0) { // Check if recv failed but expected data
        fprintf(stderr, "Client Child Comm: Failed to receive result vector from Child %d\n", args->child_id);
        goto cleanup_client_comm;
    }
    // Check if size matches columns sent
    if (child_p_size != args->matrix_part.cols) {
         fprintf(stderr, "Client Child Comm: Received vector size %u from child %d, expected %u\n",
                 child_p_size, args->child_id, args->matrix_part.cols);
         goto cleanup_client_comm;
    }
     printf("Client Child Comm: Received result vector (size %u) from Child %d.\n", child_p_size, args->child_id);


    // --- Store Received Vector Safely ---
    pthread_mutex_lock(args->child_result_mutex);
    *(args->received_child_vector_size) = child_p_size;
    // FIX 1 Correction: Assign pointer directly
    *(args->received_child_vector) = child_p; // Transfer ownership of malloc'd buffer
    child_p = NULL; // Avoid double free in cleanup phase
    pthread_mutex_unlock(args->child_result_mutex);

    thread_return_status = (void*)0; // Success

cleanup_client_comm:
    // Free resources allocated in this thread
    free_matrix(&args->matrix_part); // Free the matrix slice passed to this thread
    free(child_p); // Free if recv failed after allocation or size mismatch
    if (child_sock >= 0) {
        close(child_sock);
    }
    pthread_exit(thread_return_status);
}

void run_client(int port_to_listen, int client_id, int q) {
    printf("--- Running as CLIENT ID %d ---\n", client_id);
    printf("Listening on Port: %d, MA Order Q = %d\n", port_to_listen, q);

    Config *config = NULL;
    int listen_fd = -1, conn_fd = -1;
    Matrix my_matrix_cols = {0};
    double *p_local = NULL;
    int *children_ids = NULL;
    pthread_t *child_threads = NULL;
    ClientCommThreadArgs *child_args = NULL;
    double **child_results_vec = NULL;
    uint32_t *child_results_size = NULL;
    double *p_combined = NULL;

    // Read config
    config = read_config(CONFIG_FILE, 0, q); // n not known yet, q is
    if (!config) exit(EXIT_FAILURE);
    if (client_id <= 0 || client_id > config->total_clients) {
        fprintf(stderr, "Client %d: Invalid client ID for T=%d\n", client_id, config->total_clients);
        free_config(config); exit(EXIT_FAILURE);
    }
    config->q = q; // Store Q in config


    // Setup listening socket
    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) error_exit("Client: Socket creation failed");
    int optval = 1; setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)); // Ignore error
    struct sockaddr_in serv_addr={0}, cli_addr={0};
    socklen_t cli_len = sizeof(cli_addr);
    serv_addr.sin_family = AF_INET; serv_addr.sin_addr.s_addr = htonl(INADDR_ANY); serv_addr.sin_port = htons(port_to_listen);
    if (bind(listen_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
         fprintf(stderr, "Client %d: ", client_id); error_exit("bind failed");
    }
    if (listen(listen_fd, 5) < 0) error_exit("Client: listen failed");
    printf("Client %d: Listening on port %d...\n", client_id, port_to_listen);


    // Accept connection from parent/server
    conn_fd = accept(listen_fd, (struct sockaddr*)&cli_addr, &cli_len);
    if (conn_fd < 0) error_exit("Client: accept failed");
    close(listen_fd); listen_fd = -1; // Stop listening

    char cl_ip[INET_ADDRSTRLEN]; inet_ntop(AF_INET, &cli_addr.sin_addr, cl_ip, sizeof(cl_ip));
    printf("Client %d: Accepted connection from %s:%d\n", client_id, cl_ip, ntohs(cli_addr.sin_port));

    // --- Receive Matrix Columns ---
    my_matrix_cols = recv_matrix_data(conn_fd);
    // Check if receive failed or returned 0x0 matrix when expecting data
    if (!my_matrix_cols.data && (my_matrix_cols.rows != 0 || my_matrix_cols.cols != 0)) {
        fprintf(stderr, "Client %d: Failed to receive matrix columns properly.\n", client_id);
        close(conn_fd); free_config(config); exit(EXIT_FAILURE);
    }
     // Update config->n based on received matrix (needed for TABLE2 output)
     config->n = my_matrix_cols.rows;
     printf("Client %d: Received initial matrix columns (%u rows x %u cols)\n", client_id, my_matrix_cols.rows, my_matrix_cols.cols);


    // --- Compute Moving Average (Timed) ---
    double computation_time = 0.0;
    p_local = compute_moving_average(&my_matrix_cols, config->q, &computation_time);
    // Check MA computation result (p_local is NULL if it fails or if cols=0)
     if (!p_local && my_matrix_cols.cols > 0) {
        fprintf(stderr, "Client %d: Failed to compute moving average.\n", client_id);
        free_matrix(&my_matrix_cols); close(conn_fd); free_config(config); exit(EXIT_FAILURE);
    }
    uint32_t p_local_size = my_matrix_cols.cols; // MA vector size = number of cols received
    printf("Client %d: MA computation time: %.6f seconds\n", client_id, computation_time);
    // Output for Table 2 (append to file or print)
    printf("TABLE2_DATA: n=%d t=%d client=%d q=%d time=%.6f\n", config->n, config->total_clients, client_id, config->q, computation_time);


    // --- Handle Children (Distribution & M1PR) ---
    children_ids = find_children(client_id, config->total_clients);
    if (!children_ids) {
         fprintf(stderr, "Client %d: Failed to determine children\n", client_id);
          free_matrix(&my_matrix_cols); free(p_local); close(conn_fd); free_config(config); exit(EXIT_FAILURE);
    }
    int num_children = 0;
    while(children_ids[num_children] != 0) num_children++;

    uint32_t p_combined_size = 0;

    if (num_children == 0) {
        printf("Client %d: Is a leaf node.\n", client_id);
        // Leaf node: the result to send back is just p_local
        p_combined = p_local; // Transfer ownership
        p_combined_size = p_local_size;
        p_local = NULL; // Avoid double free
    } else {
        printf("Client %d: Is an internal node with %d children.\n", client_id, num_children);

        child_threads = malloc(sizeof(pthread_t) * num_children);
        child_args = malloc(sizeof(ClientCommThreadArgs) * num_children);
        child_results_vec = calloc(num_children, sizeof(double*)); // Array of pointers to vectors
        child_results_size = calloc(num_children, sizeof(uint32_t));
        pthread_mutex_t child_result_mutex = PTHREAD_MUTEX_INITIALIZER;

        if (!child_threads || !child_args || !child_results_vec || !child_results_size) {
             error_exit("Client: Failed malloc for child thread management");
        }

        uint32_t current_cols_in_node = my_matrix_cols.cols;
        uint32_t cols_processed_by_children = 0;

        // Send to children (highest ID first) and launch receiver threads
        for (int i = num_children - 1; i >= 0; --i) {
            int child_id = children_ids[i];
            // Columns are halved at each step down the tree for non-leaf children
            // Ensure integer division works as expected (T is power of 2, n div by T)
            uint32_t cols_to_send = current_cols_in_node / 2;
            uint32_t cols_to_keep = current_cols_in_node - cols_to_send;

            child_args[i].matrix_part = create_matrix(0, 0); // Initialize empty

            if (cols_to_send > 0) {
                uint32_t start_col_in_mymatrix = cols_to_keep; // Start after the columns we keep

                printf("Client %d: Preparing to send %u cols (starting at local col %u) to child %d.\n",
                       client_id, cols_to_send, start_col_in_mymatrix, child_id); // Sending the 'lower half' columns

                // Extract the columns to send from the *original* matrix received by this node
                child_args[i].matrix_part = extract_columns(&my_matrix_cols, start_col_in_mymatrix, cols_to_send);
                 if (!child_args[i].matrix_part.data && cols_to_send > 0) {
                     fprintf(stderr, "Client %d: Failed to extract columns for child %d\n", client_id, child_id);
                     // Major error, attempt cleanup - very complex, exit for now
                     exit(EXIT_FAILURE);
                 }

                child_args[i].child_id = child_id;
                child_args[i].config = config;
                // FIX 2 Correction: Assign correct type double** to double**
                child_args[i].received_child_vector = &child_results_vec[i];
                child_args[i].received_child_vector_size = &child_results_size[i];
                child_args[i].child_result_mutex = &child_result_mutex;

                if (pthread_create(&child_threads[i], NULL, client_comm_child_thread, &child_args[i]) != 0) {
                    perror("Client: Failed to create thread for child");
                    free_matrix(&child_args[i].matrix_part); // Free extracted matrix if thread failed
                    exit(EXIT_FAILURE); // Simplified cleanup
                }
                cols_processed_by_children += cols_to_send;
            } else {
                 // This case might happen if current_cols_in_node is 1
                 printf("Client %d: No columns to send to child %d (cols_to_send=0).\n", client_id, child_id);
                 child_results_vec[i] = NULL; // Ensure it's marked as empty
                 child_results_size[i] = 0;
                 // Initialize args even if no thread created
                 child_args[i].child_id = child_id;
                 child_args[i].config = config;
                 child_args[i].received_child_vector = &child_results_vec[i];
                 child_args[i].received_child_vector_size = &child_results_size[i];
                 child_args[i].child_result_mutex = &child_result_mutex;
            }
            // Update remaining columns for the next child (or self)
            current_cols_in_node = cols_to_keep;
        }
         // The final 'cols_to_keep' are the ones processed by this node itself (covered by p_local)
        uint32_t cols_for_self = current_cols_in_node;
         printf("Client %d: Columns processed locally: %u\n", client_id, cols_for_self);
         // Sanity check:
         if (cols_for_self + cols_processed_by_children != my_matrix_cols.cols) {
              fprintf(stderr, "Client %d: ERROR - Column count mismatch! Self=%u, Children=%u, Total=%u\n",
                     client_id, cols_for_self, cols_processed_by_children, my_matrix_cols.cols);
              exit(EXIT_FAILURE);
         }
         // Ensure p_local_size matches cols_for_self
         if (p_local_size != cols_for_self) {
               fprintf(stderr, "Client %d: ERROR - p_local size (%u) mismatch! Expected cols_for_self=%u\n",
                     client_id, p_local_size, cols_for_self);
              exit(EXIT_FAILURE);
         }

        // Wait for all child threads to complete (receive results)
        printf("Client %d: Waiting for results from %d children...\n", client_id, num_children);
        bool all_child_results_ok = true;
        for (int i = 0; i < num_children; ++i) {
             // Only join if a thread was potentially created (cols_to_send was > 0)
             // Check matrix_part.cols which was set even if thread creation failed
             if (child_args[i].matrix_part.cols > 0) {
                void *t_ret;
                pthread_join(child_threads[i], &t_ret);
                if (t_ret != (void*)0) {
                    fprintf(stderr, "Client %d: Thread for child %d failed.\n", client_id, children_ids[i]);
                    all_child_results_ok = false;
                     // Ensure result vec is NULL if thread failed after possible partial receive?
                     // The thread function handles freeing its child_p on failure before storage.
                }
             }
              // Check if result was actually received and size matches (lock needed?)
             // Lock might be needed if checking while other threads are still writing, but join ensures they finished writing.
             // pthread_mutex_lock(&child_result_mutex); // Maybe not needed after join
             if(child_results_vec[i] == NULL && child_results_size[i] != 0) { // Should not happen if thread succeeded
                  fprintf(stderr, "Client %d: Result size/ptr mismatch for child %d (size=%u, ptr=NULL)\n", client_id, children_ids[i], child_results_size[i]);
                 all_child_results_ok = false;
             } else if (child_results_vec[i] != NULL && child_results_size[i] != child_args[i].matrix_part.cols) {
                 // Check received size matches expected sent size (matrix_part.cols in child_args[i])
                  fprintf(stderr, "Client %d: Result size mismatch for child %d (received=%u, expected=%u)\n",
                          client_id, children_ids[i], child_results_size[i], child_args[i].matrix_part.cols);
                 all_child_results_ok = false;
             } else if (child_results_vec[i] == NULL && child_args[i].matrix_part.cols > 0 && all_child_results_ok) {
                  // If thread succeeded (ret=0) but result is still NULL, something is wrong.
                  // This check might be redundant if the size checks above cover it.
                  fprintf(stderr, "Client %d: Result vector NULL for child %d despite success?\n", client_id, children_ids[i]);
                  all_child_results_ok = false;
             }
             // pthread_mutex_unlock(&child_result_mutex);
        }
        pthread_mutex_destroy(&child_result_mutex);


        if (!all_child_results_ok) {
            fprintf(stderr, "Client %d: Failed to get results from all children. Cannot proceed.\n", client_id);
             // Free received child results if any
            for(int i=0; i<num_children; ++i) free(child_results_vec[i]); // Safe if NULL
             free(child_results_vec); free(child_results_size); free(child_args); free(child_threads); free(children_ids); free(p_local); free_matrix(&my_matrix_cols); close(conn_fd); free_config(config);
            exit(EXIT_FAILURE);
        }
        printf("Client %d: All child results received.\n", client_id);

        // Combine results: p_local + child_results[0] + child_results[1] + ...
        // Order matters! It should match the original column order.
        // p_local corresponds to the *first* `cols_for_self` columns this node received.
        // Child results correspond to subsequent blocks.
        p_combined_size = p_local_size; // Start with own size
        for(int i=0; i<num_children; ++i) p_combined_size += child_results_size[i];

        printf("Client %d: Combining local vector (size %u) with child vectors. Total size: %u\n", client_id, p_local_size, p_combined_size);
        if (p_combined_size > 0) {
            p_combined = malloc(p_combined_size * sizeof(double));
            if (!p_combined) error_exit("Client: Failed malloc for combined vector");

            // 1. Copy p_local (first part) if it exists
            if (p_local && p_local_size > 0) {
                 memcpy(p_combined, p_local, p_local_size * sizeof(double));
            }
            size_t current_offset = p_local_size;


            // 2. Copy child results in *increasing* order of child ID (which corresponds to column order)
            for (int i = 0; i < num_children; ++i) { // Iterate 0 to num_children-1
                 if (child_results_vec[i] && child_results_size[i] > 0) {
                     memcpy(p_combined + current_offset, child_results_vec[i], child_results_size[i] * sizeof(double));
                     current_offset += child_results_size[i];
                 }
                 // Free child vector after copying (or if size was 0)
                 free(child_results_vec[i]);
                 child_results_vec[i] = NULL;
            }
             // Sanity check offset
             if (current_offset != p_combined_size) {
                  fprintf(stderr, "Client %d: ERROR - Mismatch in combined vector size! Offset=%zu, Total=%u\n",
                         client_id, current_offset, p_combined_size);
                  exit(EXIT_FAILURE);
             }
        } else {
             p_combined = NULL; // Combined result is empty if local and all children were empty
        }


        // Free management arrays
        free(child_results_vec); child_results_vec = NULL;
        free(child_results_size); child_results_size = NULL;
        free(child_args); child_args = NULL;
        free(child_threads); child_threads = NULL;

    } // End internal node processing

    // Free p_local if it wasn't transferred to p_combined (i.e., internal node case)
    free(p_local); p_local = NULL;


    // --- Send Combined Result Vector back to Parent ---
    printf("Client %d: Sending combined result vector (size %u) back to parent/server (socket %d).\n", client_id, p_combined_size, conn_fd);
    if (send_vector(conn_fd, p_combined, p_combined_size) != 0) {
        fprintf(stderr, "Client %d: Failed to send result vector back.\n", client_id);
        // Error, but continue to cleanup
    } else {
        printf("Client %d: Result vector sent successfully.\n", client_id);
    }

    // Cleanup
    free(p_combined); // Free the final vector sent (safe if NULL)
    free(children_ids);
    free_matrix(&my_matrix_cols); // Free the matrix received initially
    close(conn_fd); conn_fd = -1;
    free_config(config); config = NULL;
    printf("--- Client %d finished ---\n", client_id);
}

// --- Main ---
int main(int argc, char *argv[]) {
     // Usage: Server: ./program <n> <Q> 0
     // Usage: Client: ./program <port> <client_id> <Q>
    if (argc != 4) {
        fprintf(stderr, "Usage:\n");
        fprintf(stderr, "  Server: %s <n> <Q> 0\n", argv[0]);
        fprintf(stderr, "  Client: %s <port> <client_id> <Q>\n", argv[0]);
        return EXIT_FAILURE;
    }

    // Input validation
    errno = 0; // To distinguish success/failure after strtol
    long arg1_l = strtol(argv[1], NULL, 10);
    long arg2_l = strtol(argv[2], NULL, 10);
    long arg3_l = strtol(argv[3], NULL, 10); // Mode (0=server) or Q (client)

     // Check for conversion errors (non-numeric input)
    if (errno != 0) {
        perror("Error converting arguments to numbers");
        return EXIT_FAILURE;
    }
     // Check ranges (basic)
     if (arg1_l <= 0 || arg2_l <= 0) {
          fprintf(stderr, "Error: First two arguments must be positive.\n");
          return EXIT_FAILURE;
     }


    if (arg3_l == 0) {
        // Server mode
        if (arg1_l > INT_MAX || arg2_l > INT_MAX) {
             fprintf(stderr, "Error: n or Q too large for server.\n"); return EXIT_FAILURE;
        }
        int n = (int)arg1_l;
        int q = (int)arg2_l;
        run_server(n, q);
    } else {
        // Client mode
        if (arg1_l > 65535 || arg2_l > INT_MAX || arg3_l <= 0 || arg3_l > INT_MAX) {
             fprintf(stderr, "Error: Invalid port, client ID, or Q for client.\n"); return EXIT_FAILURE;
        }
        int port = (int)arg1_l;
        int client_id = (int)arg2_l;
        int q = (int)arg3_l; // Q is the third arg for client

        run_client(port, client_id, q);
    }

    return EXIT_SUCCESS;
}


// --- Helper Implementations ---

// Sends exactly 'len' bytes from 'buf' to 'sockfd'. Handles partial sends.
int send_all(int sockfd, const void *buf, size_t len) {
    size_t total = 0;
    ssize_t n;
    const char *ptr = (const char *)buf;
    while (total < len) {
        n = send(sockfd, ptr + total, len - total, MSG_NOSIGNAL); // Add MSG_NOSIGNAL for robustness
        if (n == -1) { if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue; perror("send"); return -1; }
        if (n == 0) { fprintf(stderr, "send: connection closed\n"); return -1; }
        total += n;
    }
    return 0; // Success
}

// Receives exactly 'len' bytes into 'buf' from 'sockfd'. Handles partial receives.
int recv_all(int sockfd, void *buf, size_t len) {
    size_t total = 0;
    ssize_t n;
    char *ptr = (char *)buf;
    while (total < len) {
        n = recv(sockfd, ptr + total, len - total, 0);
        if (n == -1) { if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue; perror("recv"); return -1; }
        if (n == 0) {
            // Connection closed, check if we expected more data
            if (total < len) fprintf(stderr, "recv: connection closed prematurely (got %zu, expected %zu)\n", total, len);
            else fprintf(stderr, "recv: connection closed\n");
            return -1;
        }
        total += n;
    }
    return 0; // Success
}

// Find the 0-based position of the least significant bit set to 1
int find_lsb_pos(int n) {
    if (n <= 0) return -1; // Handle 0 or negative
    int pos = 0;
    // Check n > 0 outside loop to avoid infinite loop if n was 0 initially
    while ((n & 1) == 0) {
        n >>= 1;
        pos++;
        // Add a safeguard against potential unexpected large values or hangs
        if (pos >= sizeof(int) * 8) return -1;
    }
    return pos;
}


// Find the parent client ID
int find_parent(int client_id, int total_clients) {
     if (total_clients <= 0 || (total_clients & (total_clients - 1)) != 0) return -1; // Invalid T (allow T=1)
     if (total_clients == 1 && client_id == 1) return 0; // Special case T=1
     if (total_clients == 1 && client_id != 1) return -1; // Invalid client for T=1

    int half_clients = total_clients / 2;
    int base, relative_id;

    if (client_id >= 1 && client_id <= half_clients) { base = 1; }
    else if (client_id > half_clients && client_id <= total_clients) { base = half_clients + 1; }
    else { return -1; } // Invalid client_id

    relative_id = client_id - base;
    if (relative_id == 0) return 0; // Root node, parent is server

    int lsb_pos_rel = find_lsb_pos(relative_id); // LSB of 0-based relative ID
    if (lsb_pos_rel < 0) return -1; // Error finding LSB (shouldn't happen if relative_id > 0)

    return client_id - (1 << lsb_pos_rel);
}

// Find children client IDs. Returns a dynamically allocated array terminated by 0.
int* find_children(int client_id, int total_clients) {
    // Allow T=1, where there are no children
     if (total_clients <= 0 || (total_clients & (total_clients - 1)) != 0) return NULL;

    int half_clients = (total_clients == 1) ? 0 : total_clients / 2; // Handle T=1
    int tree_max_id;

    // Determine tree and range
    if (client_id >= 1 && client_id <= half_clients) { // Tree 1
        tree_max_id = half_clients;
    } else if (client_id > half_clients && client_id <= total_clients) { // Tree 2
        tree_max_id = total_clients;
    } else if (total_clients == 1 && client_id == 1) { // Special case T=1
         tree_max_id = 1; // No children possible anyway
    }
    else { return NULL; } // Invalid client_id for given T

    // Allocate space - maximum possible children is log2(T/2), plus terminator
    int max_possible_children = 0;
    if (total_clients > 1) max_possible_children = (int)(log2(total_clients / 2)) + 1;
    int *children = malloc(sizeof(int) * (max_possible_children + 1)); // +1 for terminator
    if (!children) { perror("malloc children"); return NULL; }
    int child_count = 0;

    // Iterate through potential children: client_id + 2^i
    for (int i = 0; ; ++i) {
        long long power_of_2_ll = 1LL << i; // Use long long for intermediate power
        // Check for potential overflow before adding
        if (client_id > INT_MAX - power_of_2_ll) {
            break; // Adding would overflow int
        }
        int potential_child_id = client_id + (int)power_of_2_ll;

        // Check if child is within the same tree's boundary
        if (potential_child_id > tree_max_id) {
            break;
        }

        // Verify that this potential child's parent is the current client_id
        int parent_check = find_parent(potential_child_id, total_clients);
        if (parent_check == client_id) {
            // Check for overflow before assigning to array (redundant here but good practice)
            if (child_count < max_possible_children) {
                 children[child_count++] = potential_child_id;
            } else {
                 // Should not happen if max_possible_children is calculated correctly
                 fprintf(stderr, "Error: Exceeded maximum child capacity in find_children\n");
                 break;
            }

        } else if (parent_check == -1) { free(children); return NULL; } // Error finding parent

        // Safety break for the loop if i gets too large
        if (i >= sizeof(int) * 8 - 1) break;
    }
    children[child_count] = 0; // Null-terminate the list
    return children;
}


// Config Reader (Added n, q args, primarily for client output)
Config* read_config(const char *filename, int n_arg, int q_arg) {
    FILE *fp = fopen(filename, "r");
    if (!fp) { perror("fopen config.txt"); return NULL; }

    Config *config = malloc(sizeof(Config));
    if (!config) { perror("malloc config"); fclose(fp); return NULL; }
    config->clients = NULL;
    config->n = n_arg; // Store n from command line (server) or 0 (client init)
    config->q = q_arg; // Store q from command line

    if (fscanf(fp, "%d\n", &config->total_clients) != 1 || config->total_clients <= 0 || config->total_clients > MAX_CLIENTS) {
        fprintf(stderr, "Invalid T in config file.\n"); fclose(fp); free(config); return NULL;
    }
     // Power of 2 check done in server/client main logic now

    config->clients = malloc(sizeof(ClientInfo) * config->total_clients);
    if (!config->clients) { perror("malloc clients array"); fclose(fp); free(config); return NULL; }

    for (int i = 0; i < config->total_clients; ++i) {
        config->clients[i].id = i + 1;
        if (fscanf(fp, "%s %d\n", config->clients[i].ip, &config->clients[i].port) != 2) {
            fprintf(stderr, "Error reading client %d info.\n", i + 1);
            fclose(fp); free(config->clients); free(config); return NULL;
        }
        // Basic IP/Port validation (reuse from previous version)
        struct sockaddr_in sa;
        if (config->clients[i].port <= 0 || config->clients[i].port > 65535 || inet_pton(AF_INET, config->clients[i].ip, &(sa.sin_addr)) != 1) {
             fprintf(stderr, "Invalid IP/Port for client %d\n", i+1); fclose(fp); free(config->clients); free(config); return NULL;
        }
    }
    fclose(fp);
    // Client needs N for Table 2 output. It gets it from received matrix rows.
    // We update config->n inside run_client after receiving matrix.
    printf("Successfully read config for %d clients.\n", config->total_clients);
    return config;
}

void free_config(Config *config) {
    if (config) {
        free(config->clients);
        free(config);
    }
}