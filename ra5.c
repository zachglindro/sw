#define _GNU_SOURCE // For ffs() if needed, though can implement manually
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
#include <stdint.h> // For uint32_t
#include <math.h> // For pow(), log2()
#include <errno.h>
#include <stdbool.h>

// --- Configuration ---
#define CONFIG_FILE "config.txt"
#define ACK_MSG "ACK"
#define ACK_LEN 3
#define MAX_CLIENTS 128 // Sensible maximum for config parsing

// --- Structures ---
typedef struct {
char ip[INET_ADDRSTRLEN];
int port;
int id; // Client ID (1-based)
} ClientInfo;

typedef struct {
int n; // Matrix dimension
int total_clients;
ClientInfo *clients; // Array of all clients
} Config;

typedef struct {
int client_sock;
int *matrix_part;
uint32_t rows;
uint32_t cols;
int target_client_id; // For server -> root client sends
Config *config; // For server -> root client sends
} ServerSendThreadArgs;

typedef struct {
int parent_sock; // Socket connected to the parent/server
int client_id;
int port_to_listen;
Config *config;
struct timeval time_before;
struct timeval time_after;
} ClientArgs;

typedef struct {
int child_id;
int *matrix_part;
uint32_t rows;
uint32_t cols;
Config *config;
int *ack_received_flag; // Pointer to a flag indicating if ACK was received
pthread_mutex_t *ack_mutex; // Mutex to protect the flag
} ClientSendThreadArgs;

// --- Global Variables ---
// Use sparingly, prefer passing args. Can be useful for shared data like config.
// Config config_g; // Decided against global config, pass via args

// --- Error Handling ---
void error_exit(const char *msg) {
perror(msg);
exit(EXIT_FAILURE);
}

// --- Time Helper ---
double time_diff(struct timeval *start, struct timeval *end) {
return (end->tv_sec - start->tv_sec) + (end->tv_usec - start->tv_usec) / 1000000.0;
}

// --- Network Helpers ---

// Sends exactly 'len' bytes from 'buf' to 'sockfd'. Handles partial sends.
int send_all(int sockfd, const void *buf, size_t len) {
size_t total = 0;
ssize_t n;
const char *ptr = (const char *)buf;

while (total < len) {
    n = send(sockfd, ptr + total, len - total, 0);
    if (n == -1) {
        if (errno == EINTR) continue; // Interrupted, try again
        perror("send");
        return -1; // Error
    }
    if (n == 0) {
        fprintf(stderr, "send: connection closed prematurely\n");
        return -1; // Connection closed
    }
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
    if (n == -1) {
         if (errno == EINTR) continue; // Interrupted, try again
        perror("recv");
        return -1; // Error
    }
    if (n == 0) {
        fprintf(stderr, "recv: connection closed prematurely\n");
        return -1; // Connection closed
    }
    total += n;
}
return 0; // Success
}

// Send matrix dimensions and data
int send_matrix(int sockfd, int *matrix, uint32_t rows, uint32_t cols) {
uint32_t net_rows = htonl(rows);
uint32_t net_cols = htonl(cols);

if (send_all(sockfd, &net_rows, sizeof(net_rows)) == -1) return -1;
if (send_all(sockfd, &net_cols, sizeof(net_cols)) == -1) return -1;

size_t matrix_size_bytes = (size_t)rows * cols * sizeof(int);
// Convert matrix elements to network byte order if needed (not strictly necessary if all machines are same endianness, but good practice)
// For simplicity here, assuming same endianness or that ints don't need swapping for this specific problem context.
// If needed, loop through matrix and htonl each element before sending.
if (send_all(sockfd, matrix, matrix_size_bytes) == -1) return -1;

printf("Sent matrix dimensions (%u x %u) and data (%zu bytes) to socket %d\n", rows, cols, matrix_size_bytes, sockfd);
return 0;
}

// Receive matrix dimensions and data
int* recv_matrix(int sockfd, uint32_t *rows, uint32_t *cols) {
uint32_t net_rows, net_cols;

if (recv_all(sockfd, &net_rows, sizeof(net_rows)) == -1) return NULL;
if (recv_all(sockfd, &net_cols, sizeof(net_cols)) == -1) return NULL;

*rows = ntohl(net_rows);
*cols = ntohl(net_cols);

if (*rows == 0 || *cols == 0) {
    fprintf(stderr, "Received invalid matrix dimensions (%u x %u)\n", *rows, *cols);
    return NULL;
}

size_t matrix_size_bytes = (size_t)(*rows) * (*cols) * sizeof(int);
int *matrix = malloc(matrix_size_bytes);
if (!matrix) {
    perror("malloc for received matrix failed");
    return NULL;
}

if (recv_all(sockfd, matrix, matrix_size_bytes) == -1) {
    free(matrix);
    return NULL;
}
// ntohl elements if they were converted by sender

printf("Received matrix dimensions (%u x %u) and data (%zu bytes) from socket %d\n", *rows, *cols, matrix_size_bytes, sockfd);
return matrix;
}

// --- Binomial Tree Helpers ---

// Find the 0-based position of the least significant bit set to 1
int find_lsb_pos(int n) {
if (n == 0) return -1; // Or handle as error
// ffs returns 1-based index, or 0 if no bit set
// int pos = ffs(n);
// return (pos > 0) ? pos - 1 : -1;
// Manual implementation:
int pos = 0;
while ((n & 1) == 0) {
n >>= 1;
pos++;
if (pos > 30) return -1; // Avoid infinite loop for n=0
}
return pos;
}

// Find the parent client ID
// Returns 0 if it's a root node (connected to server), -1 on error
int find_parent(int client_id, int total_clients) {
if (total_clients <= 1 || (total_clients & (total_clients - 1)) != 0) {
fprintf(stderr, "Error: Total clients (%d) must be a power of 2 > 1 for binomial tree.\n", total_clients);
return -1;
}
int half_clients = total_clients / 2;
int base;
int relative_id;

if (client_id >= 1 && client_id <= half_clients) { // Tree 1
    base = 1;
    relative_id = client_id - base; // 0-based relative ID
    if (relative_id == 0) return 0; // Root of tree 1, parent is server
} else if (client_id > half_clients && client_id <= total_clients) { // Tree 2
    base = half_clients + 1;
    relative_id = client_id - base; // 0-based relative ID
     if (relative_id == 0) return 0; // Root of tree 2, parent is server
} else {
    fprintf(stderr, "Error: Invalid client_id %d for T=%d\n", client_id, total_clients);
    return -1;
}

int lsb_pos = find_lsb_pos(relative_id + 1); // LSB of 1-based relative ID
if (lsb_pos < 0) {
     fprintf(stderr, "Error: Cannot find LSB for relative_id %d (client %d)\n", relative_id, client_id);
     return -1; // Should not happen for valid relative_id > 0
}

// Parent's relative ID is relative_id - (1 << lsb_pos) ?? No, parent finding seems different
// Let's retry based on examples: Parent of 4(rel 3) is 3(rel 2). Parent of 7(rel 6) is 5(rel 4). Parent of 8(rel 7) is 7(rel 6).
// The LSB rule was: parent_relative_id = relative_id - (1 << lsb_pos(relative_id)) -> NO, was lsb_pos(j-base+1)
// Let's use the structure definition: child = parent + 2^i.
// So parent = child - 2^i for some i. Which i? The largest i such that parent + 2^i = child.
// This i corresponds to the lsb_pos of the *relative index* of the child *within the list of children of the parent*.
// Let's use the LSB of the *client's relative ID*:
int lsb_pos_rel = find_lsb_pos(relative_id); // LSB of 0-based relative ID
if (lsb_pos_rel < 0 && relative_id != 0) { // lsb_pos is -1 only for 0
     fprintf(stderr, "Error: Cannot find LSB for relative_id %d (client %d)\n", relative_id, client_id);
     return -1;
}

// Trying the rule: parent ID = client_id - 2^(LSB position of relative_id)
// Test T=16, tree 1 (base=1)
// Client 4: rel_id=3 (011). lsb_pos=0. Parent = 4 - 2^0 = 3. Correct.
// Client 7: rel_id=6 (110). lsb_pos=1. Parent = 7 - 2^1 = 5. Correct.
// Client 8: rel_id=7 (111). lsb_pos=0. Parent = 8 - 2^0 = 7. Correct.
// This seems simpler and matches the structure derived earlier.

int parent_id = client_id - (1 << lsb_pos_rel);
return parent_id;
}

// Find children client IDs. Returns a dynamically allocated array of IDs terminated by 0.
// Caller must free the returned array. Returns NULL on error.
int* find_children(int client_id, int total_clients) {
if (total_clients <= 1 || (total_clients & (total_clients - 1)) != 0) {
fprintf(stderr, "Error: Total clients (%d) must be a power of 2 > 1 for binomial tree.\n", total_clients);
return NULL;
}
int half_clients = total_clients / 2;
int base;
int tree_max_id;

if (client_id >= 1 && client_id <= half_clients) { // Tree 1
    base = 1;
    tree_max_id = half_clients;
} else if (client_id > half_clients && client_id <= total_clients) { // Tree 2
    base = half_clients + 1;
    tree_max_id = total_clients;
} else {
    fprintf(stderr, "Error: Invalid client_id %d for T=%d\n", client_id, total_clients);
    return NULL;
}

int *children = malloc(sizeof(int) * (total_clients + 1)); // Max possible children + terminator
if (!children) {
    perror("malloc for children array failed");
    return NULL;
}
int child_count = 0;

// Iterate through potential children: client_id + 2^i
for (int i = 0; ; ++i) {
    int power_of_2 = 1 << i;
    int potential_child_id = client_id + power_of_2;

    if (potential_child_id > tree_max_id) {
        break; // Exceeded tree boundary
    }

    // Verify that this potential child's parent is the current client_id
    int parent_check = find_parent(potential_child_id, total_clients);
    if (parent_check == client_id) {
        children[child_count++] = potential_child_id;
    } else if (parent_check == -1) {
        // Error finding parent, propagate
        free(children);
        return NULL;
    }
    // Check for potential overflow with the next power_of_2
    if (i >= 30) break; // Avoid large shifts
}

children[child_count] = 0; // Null-terminate the list
return children;
}

// --- Config Reader ---
Config* read_config(const char *filename) {
FILE *fp = fopen(filename, "r");
if (!fp) {
perror("fopen config.txt failed");
return NULL;
}

Config *config = malloc(sizeof(Config));
if (!config) {
    perror("malloc for config failed");
    fclose(fp);
    return NULL;
}
config->clients = NULL; // Initialize

if (fscanf(fp, "%d\n", &config->total_clients) != 1 || config->total_clients <= 0 || config->total_clients > MAX_CLIENTS) {
    fprintf(stderr, "Invalid number of clients T in config file.\n");
    fclose(fp);
    free(config);
    return NULL;
}

 if ((config->total_clients & (config->total_clients - 1)) != 0 && config->total_clients != 1) {
     fprintf(stderr, "Error: Total clients T (%d) must be a power of 2.\n", config->total_clients);
     fclose(fp);
     free(config);
     return NULL;
 }


config->clients = malloc(sizeof(ClientInfo) * config->total_clients);
if (!config->clients) {
    perror("malloc for client info array failed");
    fclose(fp);
    free(config);
    return NULL;
}

for (int i = 0; i < config->total_clients; ++i) {
    config->clients[i].id = i + 1; // Assign 1-based ID
    if (fscanf(fp, "%s %d\n", config->clients[i].ip, &config->clients[i].port) != 2) {
        fprintf(stderr, "Error reading client %d info from config file.\n", i + 1);
        fclose(fp);
        free(config->clients);
        free(config);
        return NULL;
    }
     // Basic validation
    if (config->clients[i].port <= 0 || config->clients[i].port > 65535) {
         fprintf(stderr, "Invalid port %d for client %d\n", config->clients[i].port, i + 1);
        fclose(fp);
        free(config->clients);
        free(config);
        return NULL;
    }
    struct sockaddr_in sa;
    if (inet_pton(AF_INET, config->clients[i].ip, &(sa.sin_addr)) != 1) {
         fprintf(stderr, "Invalid IP address format '%s' for client %d\n", config->clients[i].ip, i + 1);
        fclose(fp);
        free(config->clients);
        free(config);
        return NULL;
    }
}

fclose(fp);
printf("Successfully read config for %d clients.\n", config->total_clients);
return config;
}

void free_config(Config *config) {
if (config) {
free(config->clients);
free(config);
}
}

// --- Server Logic ---

// Thread function for server sending data to one root client and waiting for ACK
void* server_send_thread(void *arg) {
ServerSendThreadArgs *args = (ServerSendThreadArgs *)arg;
int client_sock = -1; // Initialize to invalid state
struct sockaddr_in cli_addr;

// Find target client info
ClientInfo *target_client = NULL;
for (int i = 0; i < args->config->total_clients; ++i) {
    if (args->config->clients[i].id == args->target_client_id) {
        target_client = &args->config->clients[i];
        break;
    }
}

if (!target_client) {
    fprintf(stderr, "Server: Could not find client info for ID %d\n", args->target_client_id);
    pthread_exit((void*)-1);
}

// Create socket
client_sock = socket(AF_INET, SOCK_STREAM, 0);
if (client_sock < 0) {
    perror("Server: socket creation failed");
    pthread_exit((void*)-1);
}

// Prepare client address
memset(&cli_addr, 0, sizeof(cli_addr));
cli_addr.sin_family = AF_INET;
cli_addr.sin_port = htons(target_client->port);
if (inet_pton(AF_INET, target_client->ip, &cli_addr.sin_addr) <= 0) {
    fprintf(stderr, "Server: Invalid address/ Address not supported for client %d\n", args->target_client_id);
    close(client_sock);
    pthread_exit((void*)-1);
}

// Connect to client
printf("Server: Connecting to Client %d (%s:%d)...\n", args->target_client_id, target_client->ip, target_client->port);
if (connect(client_sock, (struct sockaddr *)&cli_addr, sizeof(cli_addr)) < 0) {
    perror("Server: connect failed");
    fprintf(stderr, "Server: Failed to connect to Client %d (%s:%d)\n", args->target_client_id, target_client->ip, target_client->port);
    close(client_sock);
    pthread_exit((void*)-1);
}
printf("Server: Connected successfully to Client %d.\n", args->target_client_id);


// Send matrix part
if (send_matrix(client_sock, args->matrix_part, args->rows, args->cols) != 0) {
    fprintf(stderr, "Server: Failed to send matrix to Client %d\n", args->target_client_id);
    close(client_sock);
    pthread_exit((void*)-1);
}
 printf("Server: Matrix part sent to Client %d.\n", args->target_client_id);

// Wait for ACK
char ack_buf[ACK_LEN + 1];
printf("Server: Waiting for ACK from Client %d...\n", args->target_client_id);
if (recv_all(client_sock, ack_buf, ACK_LEN) != 0) {
    fprintf(stderr, "Server: Failed to receive ACK from Client %d\n", args->target_client_id);
    close(client_sock);
    pthread_exit((void*)-1);
}
ack_buf[ACK_LEN] = '\0';

if (strcmp(ack_buf, ACK_MSG) == 0) {
    printf("Server: Received ACK from Client %d.\n", args->target_client_id);
} else {
    fprintf(stderr, "Server: Received invalid ACK ('%s') from Client %d\n", ack_buf, args->target_client_id);
    close(client_sock);
    pthread_exit((void*)-1);
}

close(client_sock);
pthread_exit((void*)0); // Success
}

void run_server(int n) {
printf("--- Running as SERVER ---\n");
printf("Matrix size n = %d\n", n);

// Read config
Config *config = read_config(CONFIG_FILE);
if (!config) {
    exit(EXIT_FAILURE);
}
if (config->total_clients == 0) {
     fprintf(stderr, "Server: No clients specified in config.txt\n");
     free_config(config);
     exit(EXIT_FAILURE);
}
 if (n % config->total_clients != 0) {
    fprintf(stderr, "Server Error: Matrix size n (%d) must be divisible by total clients T (%d).\n", n, config->total_clients);
    free_config(config);
    exit(EXIT_FAILURE);
}
 if ((config->total_clients & (config->total_clients - 1)) != 0 && config->total_clients > 1) {
     fprintf(stderr, "Server Error: Total clients T (%d) must be a power of 2.\n", config->total_clients);
     free_config(config);
     exit(EXIT_FAILURE);
 }


// Allocate and initialize matrix (can be very large!)
printf("Server: Allocating %d x %d matrix...\n", n, n);
size_t matrix_elements = (size_t)n * n;
size_t matrix_size_bytes = matrix_elements * sizeof(int);
int *matrix = malloc(matrix_size_bytes);
if (!matrix) {
    fprintf(stderr, "Server: Failed to allocate matrix (%zu bytes)\n", matrix_size_bytes);
    free_config(config);
    exit(EXIT_FAILURE);
}
printf("Server: Initializing matrix...\n");
for (size_t i = 0; i < n; ++i) {
    for (size_t j = 0; j < n; ++j) {
        matrix[i * n + j] = (int)(i * n + j); // Sequential values
    }
}
printf("Server: Matrix created successfully.\n");

// Prepare arguments for sender threads
pthread_t tid1, tid2;
ServerSendThreadArgs args1, args2;
int half_rows = n / 2;
int root_client_1_id = 1;
int root_client_2_id = (config->total_clients / 2) + 1;

if (config->total_clients == 1) { // Special case: only one client
    root_client_2_id = -1; // Indicate no second client
    half_rows = n; // Send the whole matrix to client 1
} else if (config->total_clients < 1) {
     fprintf(stderr, "Server Error: Invalid number of clients: %d\n", config->total_clients);
     free(matrix);
     free_config(config);
     exit(EXIT_FAILURE);
}


args1.matrix_part = matrix; // First half starts at the beginning
args1.rows = half_rows;
args1.cols = n;
args1.target_client_id = root_client_1_id;
args1.config = config;

if (root_client_2_id > 0) {
    args2.matrix_part = matrix + (size_t)half_rows * n; // Second half offset
    args2.rows = n - half_rows; // Remaining rows (handles odd n if needed, though we assume divisibility)
    args2.cols = n;
    args2.target_client_id = root_client_2_id;
    args2.config = config;
}

// Record time before sending
struct timeval time_before, time_after;
gettimeofday(&time_before, NULL);
printf("Server: Starting data transfer at %ld.%06ld\n", time_before.tv_sec, time_before.tv_usec);

// Start sender threads
printf("Server: Launching thread to send first half (%d rows) to Client %d\n", args1.rows, args1.target_client_id);
if (pthread_create(&tid1, NULL, server_send_thread, &args1) != 0) {
    perror("Server: Failed to create thread for client 1");
    free(matrix);
    free_config(config);
    exit(EXIT_FAILURE);
}

if (root_client_2_id > 0) {
    printf("Server: Launching thread to send second half (%d rows) to Client %d\n", args2.rows, args2.target_client_id);
    if (pthread_create(&tid2, NULL, server_send_thread, &args2) != 0) {
        perror("Server: Failed to create thread for client 2");
        // Consider how to handle tid1 if it's running
        pthread_cancel(tid1); // Attempt to cancel
        pthread_join(tid1, NULL); // Wait for it
        free(matrix);
        free_config(config);
        exit(EXIT_FAILURE);
    }
}

// Wait for threads to complete (i.e., ACKs received)
void *ret1, *ret2 = (void*)0; // Initialize ret2 to success for the single client case
pthread_join(tid1, &ret1);
if (root_client_2_id > 0) {
    pthread_join(tid2, &ret2);
}

// Record time after receiving ACKs
gettimeofday(&time_after, NULL);
printf("Server: Received final ACK at %ld.%06ld\n", time_after.tv_sec, time_after.tv_usec);


// Check thread results
if (ret1 != (void*)0 || (root_client_2_id > 0 && ret2 != (void*)0)) {
    fprintf(stderr, "Server: One or more send/ack threads failed. Exiting.\n");
    free(matrix);
    free_config(config);
    exit(EXIT_FAILURE);
}

printf("Server: All ACKs received successfully.\n");

// Calculate and print elapsed time
double elapsed = time_diff(&time_before, &time_after);
printf("Server: Total time elapsed: %.6f seconds\n", elapsed);

// Cleanup
free(matrix);
free_config(config);
printf("--- Server finished ---\n");
}

// --- Client Logic ---

// Thread function for client sending data to one child and waiting for ACK
void* client_send_child_thread(void *arg) {
ClientSendThreadArgs *args = (ClientSendThreadArgs *)arg;
int child_sock = -1;
struct sockaddr_in child_addr;
ClientInfo *child_info = NULL;
bool ack_local_flag = false; // Local status

// Find child client info
for (int i = 0; i < args->config->total_clients; ++i) {
    if (args->config->clients[i].id == args->child_id) {
        child_info = &args->config->clients[i];
        break;
    }
}
if (!child_info) {
    fprintf(stderr, "Client Child Sender: Could not find client info for child ID %d\n", args->child_id);
    pthread_exit((void*)-1);
}

// Create socket
child_sock = socket(AF_INET, SOCK_STREAM, 0);
if (child_sock < 0) {
    perror("Client Child Sender: socket creation failed");
     pthread_exit((void*)-1);
}

// Prepare child address
memset(&child_addr, 0, sizeof(child_addr));
child_addr.sin_family = AF_INET;
child_addr.sin_port = htons(child_info->port);
if (inet_pton(AF_INET, child_info->ip, &child_addr.sin_addr) <= 0) {
    fprintf(stderr, "Client Child Sender: Invalid address for child %d\n", args->child_id);
    close(child_sock);
    pthread_exit((void*)-1);
}

// Connect to child
printf("Client Child Sender: Connecting to Child %d (%s:%d)...\n", args->child_id, child_info->ip, child_info->port);
 if (connect(child_sock, (struct sockaddr *)&child_addr, sizeof(child_addr)) < 0) {
    perror("Client Child Sender: connect failed");
    fprintf(stderr, "Client Child Sender: Failed connection to child %d\n", args->child_id);
    close(child_sock);
    pthread_exit((void*)-1);
}
 printf("Client Child Sender: Connected successfully to Child %d.\n", args->child_id);


// Send matrix part
if (send_matrix(child_sock, args->matrix_part, args->rows, args->cols) != 0) {
    fprintf(stderr, "Client Child Sender: Failed to send matrix to Child %d\n", args->child_id);
    close(child_sock);
    pthread_exit((void*)-1);
}
printf("Client Child Sender: Matrix part sent to Child %d.\n", args->child_id);

// Wait for ACK
char ack_buf[ACK_LEN + 1];
printf("Client Child Sender: Waiting for ACK from Child %d...\n", args->child_id);
if (recv_all(child_sock, ack_buf, ACK_LEN) != 0) {
    fprintf(stderr, "Client Child Sender: Failed to receive ACK from Child %d\n", args->child_id);
    close(child_sock);
    pthread_exit((void*)-1);
}
ack_buf[ACK_LEN] = '\0';

 if (strcmp(ack_buf, ACK_MSG) == 0) {
    printf("Client Child Sender: Received ACK from Child %d.\n", args->child_id);
    ack_local_flag = true;
} else {
    fprintf(stderr, "Client Child Sender: Received invalid ACK ('%s') from Child %d\n", ack_buf, args->child_id);
    close(child_sock);
    pthread_exit((void*)-1);
}

close(child_sock);

// Update shared ACK flag safely
pthread_mutex_lock(args->ack_mutex);
*(args->ack_received_flag) = 1; // Mark ACK as received for this child
pthread_mutex_unlock(args->ack_mutex);

pthread_exit((void*)0); // Success
}

void run_client(int port_to_listen, int client_id) {
printf("--- Running as CLIENT ---\n");
printf("Client ID: %d, Listening on Port: %d\n", client_id, port_to_listen);

// Read config to get T and other client details
Config *config = read_config(CONFIG_FILE);
if (!config) {
    exit(EXIT_FAILURE);
}
 if (client_id <= 0 || client_id > config->total_clients) {
    fprintf(stderr, "Client Error: Invalid client ID %d for T=%d\n", client_id, config->total_clients);
    free_config(config);
    exit(EXIT_FAILURE);
}

// Validate own port (optional, but good check)
bool port_matches = false;
for(int i=0; i<config->total_clients; ++i) {
    if (config->clients[i].id == client_id && config->clients[i].port == port_to_listen) {
        port_matches = true;
        break;
    }
}
 if (!port_matches) {
     fprintf(stderr, "Client Warning: Listening port %d does not match config file for client %d.\n", port_to_listen, client_id);
     // Continue anyway, assuming command line is correct.
 }


// Setup listening socket
int listen_fd, conn_fd;
struct sockaddr_in serv_addr, cli_addr;
socklen_t cli_len = sizeof(cli_addr);

listen_fd = socket(AF_INET, SOCK_STREAM, 0);
if (listen_fd < 0) {
    error_exit("Client: Socket creation failed");
}

int optval = 1;
if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0) {
     perror("Client: setsockopt(SO_REUSEADDR) failed");
     // Non-fatal, continue
}


memset(&serv_addr, 0, sizeof(serv_addr));
serv_addr.sin_family = AF_INET;
serv_addr.sin_addr.s_addr = htonl(INADDR_ANY); // Listen on any interface
serv_addr.sin_port = htons(port_to_listen);

if (bind(listen_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
    fprintf(stderr, "Client %d: Failed to bind to port %d\n", client_id, port_to_listen);
    error_exit("Client: bind failed");
}

if (listen(listen_fd, 5) < 0) {
    error_exit("Client: listen failed");
}

printf("Client %d: Listening on port %d...\n", client_id, port_to_listen);

// Accept connection from parent/server
conn_fd = accept(listen_fd, (struct sockaddr*)&cli_addr, &cli_len);
if (conn_fd < 0) {
    error_exit("Client: accept failed");
}
close(listen_fd); // Stop listening after accepting one connection

// Record time_before
struct timeval time_before, time_after;
gettimeofday(&time_before, NULL);
char cl_ip[INET_ADDRSTRLEN];
inet_ntop(AF_INET, &cli_addr.sin_addr, cl_ip, sizeof(cl_ip));
printf("Client %d: Accepted connection from %s:%d at %ld.%06ld\n",
       client_id, cl_ip, ntohs(cli_addr.sin_port), time_before.tv_sec, time_before.tv_usec);

// Receive matrix part
uint32_t received_rows, received_cols;
int *my_matrix_part = recv_matrix(conn_fd, &received_rows, &received_cols);
if (!my_matrix_part) {
    fprintf(stderr, "Client %d: Failed to receive matrix.\n", client_id);
    close(conn_fd);
    free_config(config);
    exit(EXIT_FAILURE);
}
printf("Client %d: Received initial matrix part (%u x %u)\n", client_id, received_rows, received_cols);


// Determine children
int *children_ids = find_children(client_id, config->total_clients);
if (!children_ids) {
     fprintf(stderr, "Client %d: Failed to determine children.\n", client_id);
     free(my_matrix_part);
     close(conn_fd);
     free_config(config);
     exit(EXIT_FAILURE);
}

int num_children = 0;
while (children_ids[num_children] != 0) {
    num_children++;
}

// Check if leaf node
if (num_children == 0) {
    printf("Client %d: Is a leaf node. Processing complete.\n", client_id);
    // Leaf node processing: just ACK parent
} else {
    printf("Client %d: Is an internal node with %d children: ", client_id, num_children);
    for(int i=0; i<num_children; ++i) printf("%d ", children_ids[i]);
    printf("\n");

    // Internal node processing: distribute matrix parts
    pthread_t *child_threads = malloc(sizeof(pthread_t) * num_children);
    ClientSendThreadArgs *child_args = malloc(sizeof(ClientSendThreadArgs) * num_children);
    int *ack_flags = malloc(sizeof(int) * num_children);
    pthread_mutex_t ack_mutex = PTHREAD_MUTEX_INITIALIZER;

    if (!child_threads || !child_args || !ack_flags) {
         perror("Client: Failed to allocate memory for child thread management");
         free(children_ids);
         free(my_matrix_part);
         close(conn_fd);
         free_config(config);
         exit(EXIT_FAILURE);
    }
    for(int i=0; i<num_children; ++i) ack_flags[i] = 0; // Initialize ACK flags


    uint32_t current_rows = received_rows;
    int *current_matrix_ptr = my_matrix_part; // Pointer to the start of the current relevant block

    // Send to children in reverse order of ID (highest ID first)
    for (int i = num_children - 1; i >= 0; --i) {
        int child_id = children_ids[i];
        uint32_t rows_to_send = current_rows / 2;
        uint32_t rows_to_keep = current_rows - rows_to_send;
        int *matrix_to_send_ptr = current_matrix_ptr + (size_t)rows_to_keep * received_cols; // Send lower half

         printf("Client %d: Preparing to send %u rows (from %u total) to child %d.\n",
               client_id, rows_to_send, current_rows, child_id);


        if (rows_to_send > 0) { // Only send if there are rows to send
            child_args[i].child_id = child_id;
            child_args[i].matrix_part = matrix_to_send_ptr;
            child_args[i].rows = rows_to_send;
            child_args[i].cols = received_cols;
            child_args[i].config = config;
            child_args[i].ack_received_flag = &ack_flags[i];
            child_args[i].ack_mutex = &ack_mutex;

            if (pthread_create(&child_threads[i], NULL, client_send_child_thread, &child_args[i]) != 0) {
                perror("Client: Failed to create thread for child");
                // Attempt cleanup
                // Need robust cancellation/joining here if some threads started
                free(ack_flags);
                free(child_args);
                free(child_threads);
                free(children_ids);
                free(my_matrix_part);
                close(conn_fd);
                free_config(config);
                exit(EXIT_FAILURE);
            }
        } else {
             printf("Client %d: No rows left to send to child %d, marking ACK as received.\n", client_id, child_id);
             ack_flags[i] = 1; // Mark as done if no data sent
        }


        // Update remaining matrix for next iteration (or for self)
        current_rows = rows_to_keep;
        // current_matrix_ptr remains the same (points to the start of the upper block)
    }

     printf("Client %d: Keeping final %u rows for self.\n", client_id, current_rows);


    // Wait for all child threads to complete (receive ACKs)
    printf("Client %d: Waiting for ACKs from all %d children...\n", client_id, num_children);
    bool all_acks_received = true;
    for (int i = 0; i < num_children; ++i) {
         if (child_args[i].rows > 0) { // Only join threads that were actually created
            void *t_ret;
            pthread_join(child_threads[i], &t_ret);
            if (t_ret != (void*)0) {
                fprintf(stderr, "Client %d: Thread for child %d failed.\n", client_id, children_ids[i]);
                all_acks_received = false;
                // Maybe continue waiting for others? Or exit? For now, note failure.
            }
             // Double check the flag (set by the thread)
             pthread_mutex_lock(&ack_mutex);
             if (!ack_flags[i]) {
                  fprintf(stderr, "Client %d: ACK flag not set for child %d even after join.\n", client_id, children_ids[i]);
                  all_acks_received = false;
             }
             pthread_mutex_unlock(&ack_mutex);

         } else {
              // If no rows were sent, the ack_flag should be 1.
             if (!ack_flags[i]) {
                  fprintf(stderr, "Client %d: ACK flag not set for child %d (no data sent case).\n", client_id, children_ids[i]);
                 all_acks_received = false;
             }
         }
    }

    pthread_mutex_destroy(&ack_mutex);
    free(ack_flags);
    free(child_args);
    free(child_threads);

    if (!all_acks_received) {
        fprintf(stderr, "Client %d: Failed to receive ACKs from all children. Cannot proceed.\n", client_id);
         free(children_ids);
         free(my_matrix_part);
         close(conn_fd);
         free_config(config);
         exit(EXIT_FAILURE);
    }
    printf("Client %d: All child ACKs received.\n", client_id);

} // End internal node processing

// Send ACK back to parent/server
printf("Client %d: Sending ACK to parent/server (socket %d).\n", client_id, conn_fd);
if (send_all(conn_fd, ACK_MSG, ACK_LEN) != 0) {
    fprintf(stderr, "Client %d: Failed to send ACK to parent/server.\n", client_id);
    // Error is critical, but try to record time anyway before exiting
} else {
     printf("Client %d: ACK sent successfully.\n", client_id);
}


// Record time_after
gettimeofday(&time_after, NULL);
printf("Client %d: Finished processing at %ld.%06ld\n", client_id, time_after.tv_sec, time_after.tv_usec);


// Calculate and print elapsed time for this client
double elapsed = time_diff(&time_before, &time_after);
printf("Client %d: Total time elapsed (accept to ack sent): %.6f seconds\n", client_id, elapsed);


// Cleanup
free(children_ids);
free(my_matrix_part);
close(conn_fd);
free_config(config);
printf("--- Client %d finished ---\n", client_id);
}

// --- Main ---
int main(int argc, char *argv[]) {
if (argc != 3) {
fprintf(stderr, "Usage:\n");
fprintf(stderr, " Server: %s <n> 0\n", argv[0]);
fprintf(stderr, " Client: %s <port> <client_id>\n", argv[0]);
return EXIT_FAILURE;
}

long arg1 = strtol(argv[1], NULL, 10);
long arg2 = strtol(argv[2], NULL, 10);

if (arg1 <= 0) {
    fprintf(stderr, "Error: First argument must be positive.\n");
    return EXIT_FAILURE;
}
 if (arg2 < 0) {
    fprintf(stderr, "Error: Second argument must be non-negative.\n");
    return EXIT_FAILURE;
}


if (arg2 == 0) {
    // Server mode
    int n = (int)arg1;
    if (n <= 0) {
         fprintf(stderr, "Error: Matrix size n must be positive.\n");
        return EXIT_FAILURE;
    }
    run_server(n);
} else {
    // Client mode
    int port = (int)arg1;
    int client_id = (int)arg2;
     if (port <= 0 || port > 65535) {
         fprintf(stderr, "Error: Invalid port number %d.\n", port);
        return EXIT_FAILURE;
    }
     if (client_id <= 0) {
         fprintf(stderr, "Error: Client ID must be positive.\n");
        return EXIT_FAILURE;
    }
    run_client(port, client_id);
}

return EXIT_SUCCESS;
}
