#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h> // for fork, close, gethostname
#include <sys/socket.h> // for sockets
#include <netinet/in.h> // for sockaddr_in
#include <arpa/inet.h>  // for inet_pton
#include <sys/wait.h>   // for waitpid
#include <time.h>       // for time, srand
#include <errno.h>      // for errno

#define CONFIG_FILE "config.txt"
#define MAX_NODES 10
#define BUFFER_SIZE 4096
#define MATRIX_MAX_VAL 100 // Max random value (inclusive)
#define MAX_ADDR_LEN 16    // Max IP address string length (xxx.xxx.xxx.xxx\0)
#define INT_STR_SIZE 12    // Enough space for string representation of an int

// Structure to hold node information
typedef struct {
    char ip[MAX_ADDR_LEN];
    int port;
} Node;

// --- Error Handling ---
void error_exit(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

// --- Configuration Reading ---
int read_config(Node nodes[], int max_nodes) {
    FILE *fp = fopen(CONFIG_FILE, "r");
    if (fp == NULL) {
        error_exit("Error opening config file");
    }

    int count = 0;
    char line[100]; // Buffer for reading lines

    while (fgets(line, sizeof(line), fp) != NULL && count < max_nodes) {
        // Remove trailing newline if present
        line[strcspn(line, "\n")] = 0;

        // Skip empty lines or comments
        if (strlen(line) == 0 || line[0] == '#') {
            continue;
        }

        // Parse IP and port
        if (sscanf(line, "%15s %d", nodes[count].ip, &nodes[count].port) == 2) {
             // Basic validation (could be more robust)
            if (nodes[count].port <= 0 || nodes[count].port > 65535) {
                 fprintf(stderr, "Warning: Invalid port number %d in config.txt\n", nodes[count].port);
                 continue; // Skip this invalid entry
            }
            count++;
        } else {
            fprintf(stderr, "Warning: Could not parse line in config.txt: %s\n", line);
        }
    }

    fclose(fp);

    if (count == 0) {
        fprintf(stderr, "Error: No valid nodes found in config file.\n");
        exit(EXIT_FAILURE);
    }
    return count;
}

// --- Helper: Send all data reliably ---
ssize_t send_all(int sockfd, const void *buf, size_t len) {
    size_t total_sent = 0;
    const char *ptr = (const char *)buf;

    while (total_sent < len) {
        ssize_t sent = send(sockfd, ptr + total_sent, len - total_sent, 0);
        if (sent == -1) {
            if (errno == EINTR) continue; // Interrupted by signal, try again
            perror("send failed");
            return -1; // Indicate error
        }
        if (sent == 0) {
             fprintf(stderr, "send returned 0 (connection closed?)\n");
             return -1; // Indicate connection closed or error
        }
        total_sent += sent;
    }
    return total_sent; // Return total bytes sent on success
}


// --- Helper: Receive specific amount of data reliably ---
ssize_t recv_all(int sockfd, void *buf, size_t len) {
    size_t total_recv = 0;
    char *ptr = (char *)buf;

    while (total_recv < len) {
        ssize_t received = recv(sockfd, ptr + total_recv, len - total_recv, 0);
        if (received == -1) {
            if (errno == EINTR) continue; // Interrupted by signal, try again
            perror("recv failed");
            return -1; // Indicate error
        }
        if (received == 0) {
            // Connection closed prematurely by peer
            fprintf(stderr, "recv error: Connection closed before receiving all data. Expected %zu, got %zu\n", len, total_recv);
            return -1; // Indicate error (or incomplete data)
        }
        total_recv += received;
    }
    return total_recv; // Return total bytes received on success
}


// --- Master's Child Process Logic ---
void send_matrix_chunk(int n, int rows, Node target_node) {
    if (rows <= 0) {
        printf("Node %s:%d assigned 0 rows, skipping.\n", target_node.ip, target_node.port);
        return; // Nothing to send
    }

    // 1. Generate Matrix Chunk
    size_t matrix_size_bytes = (size_t)rows * n * sizeof(int);
    int *matrix = (int *)malloc(matrix_size_bytes);
    if (matrix == NULL) {
        error_exit("malloc failed for matrix chunk");
    }

    printf("[Child %d] Generating %d x %d matrix chunk (%zu bytes) for %s:%d\n",
           getpid(), rows, n, matrix_size_bytes, target_node.ip, target_node.port);
    for (int i = 0; i < rows * n; ++i) {
        matrix[i] = (rand() % MATRIX_MAX_VAL) + 1;
    }

    // 2. Connect to Slave
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        error_exit("socket creation failed");
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(target_node.port);

    if (inet_pton(AF_INET, target_node.ip, &serv_addr.sin_addr) <= 0) {
        fprintf(stderr, "Invalid address/ Address not supported: %s\n", target_node.ip);
        close(sock);
        free(matrix);
        exit(EXIT_FAILURE);
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        fprintf(stderr, "Connection Failed to %s:%d ", target_node.ip, target_node.port);
        perror(""); // Print specific connect error
        close(sock);
        free(matrix);
        exit(EXIT_FAILURE); // Child exits on failure
    }

    printf("[Child %d] Connected to %s:%d\n", getpid(), target_node.ip, target_node.port);

    // 3. Send n (dimension) as a string
    char n_str[INT_STR_SIZE];
    snprintf(n_str, sizeof(n_str), "%d", n);
    if (send_all(sock, n_str, strlen(n_str)) < 0) { // Send null terminator? No, recv expects it. Let's add 1.
        fprintf(stderr, "[Child %d] Failed to send n\n", getpid());
        close(sock);
        free(matrix);
        exit(EXIT_FAILURE);
    }
    // Send a delimiter after n (like a newline) to make receiving easier
    if (send_all(sock, "\n", 1) < 0) {
        fprintf(stderr, "[Child %d] Failed to send n delimiter\n", getpid());
        close(sock);
        free(matrix);
        exit(EXIT_FAILURE);
    }
    printf("[Child %d] Sent n=%d\n", getpid(), n);


    // 4. Send rows assigned to this node as a string
    char rows_str[INT_STR_SIZE];
    snprintf(rows_str, sizeof(rows_str), "%d", rows);
     if (send_all(sock, rows_str, strlen(rows_str)) < 0) {
        fprintf(stderr, "[Child %d] Failed to send rows\n", getpid());
        close(sock);
        free(matrix);
        exit(EXIT_FAILURE);
    }
     // Send a delimiter after rows
    if (send_all(sock, "\n", 1) < 0) {
        fprintf(stderr, "[Child %d] Failed to send rows delimiter\n", getpid());
        close(sock);
        free(matrix);
        exit(EXIT_FAILURE);
    }
    printf("[Child %d] Sent rows=%d\n", getpid(), rows);


    // 5. Send Matrix Data
    printf("[Child %d] Sending matrix data (%zu bytes)...\n", getpid(), matrix_size_bytes);
    if (send_all(sock, matrix, matrix_size_bytes) < 0) {
        fprintf(stderr, "[Child %d] Failed to send matrix data\n", getpid());
        // Error already printed by send_all
        close(sock);
        free(matrix);
        exit(EXIT_FAILURE);
    }
    printf("[Child %d] Matrix data sent.\n", getpid());

    // 6. Receive Acknowledgment
    char ack_buffer[BUFFER_SIZE];
    memset(ack_buffer, 0, sizeof(ack_buffer));
    ssize_t bytes_received = recv(sock, ack_buffer, sizeof(ack_buffer) - 1, 0);
    if (bytes_received < 0) {
        perror("recv ack failed");
    } else if (bytes_received == 0) {
         fprintf(stderr, "[Child %d] Connection closed by slave before sending ACK.\n", getpid());
    } else {
        ack_buffer[bytes_received] = '\0'; // Null-terminate
        printf("[Child %d] Received ACK: %s\n", getpid(), ack_buffer);
    }

    // 7. Cleanup
    close(sock);
    free(matrix);
    printf("[Child %d] Connection closed, exiting.\n", getpid());
}

// --- Master Process Logic ---
void master(int n) {
    printf("Running as MASTER\n");
    Node nodes[MAX_NODES];
    int num_nodes = read_config(nodes, MAX_NODES);

    printf("Read %d slave nodes from %s\n", num_nodes, CONFIG_FILE);

    if (n <= 0) {
        fprintf(stderr, "Error: Matrix dimension n must be positive.\n");
        exit(EXIT_FAILURE);
    }
    if (n < num_nodes) {
        printf("Warning: Matrix dimension n (%d) is less than the number of nodes (%d). Some nodes will receive 0 rows.\n", n, num_nodes);
    }


    int rows_per_node = n / num_nodes;
    int remainder = n % num_nodes;
    int current_row = 0; // Keep track if needed, not strictly necessary here

    pid_t *child_pids = malloc(num_nodes * sizeof(pid_t));
    if (!child_pids) error_exit("malloc failed for child PIDs");
    int children_started = 0;


    printf("Distributing %d rows across %d nodes (%d per node, %d remainder)\n", n, num_nodes, rows_per_node, remainder);

    for (int i = 0; i < num_nodes; ++i) {
        int node_rows = rows_per_node + (i < remainder ? 1 : 0);

        pid_t pid = fork();

        if (pid < 0) {
            perror("fork failed");
            // Attempt to kill already started children before exiting? Complex.
            // For simplicity, just exit. Resources might leak.
             free(child_pids);
            exit(EXIT_FAILURE);
        } else if (pid == 0) {
            // --- Child Process ---
            // Seed random generator differently in each child
            srand(time(NULL) ^ getpid());
            send_matrix_chunk(n, node_rows, nodes[i]);
            exit(EXIT_SUCCESS); // Child exits normally
        } else {
            // --- Parent Process ---
            printf("[Master] Forked child %d (PID: %d) for node %s:%d (%d rows)\n", i, pid, nodes[i].ip, nodes[i].port, node_rows);
            child_pids[children_started++] = pid;
            current_row += node_rows; // Update row counter if needed
        }
    }

    // Wait for all children to complete
    printf("[Master] Waiting for %d child processes...\n", children_started);
    int status;
    pid_t wpid;
    int children_finished = 0;
    while (children_finished < children_started) {
         wpid = wait(&status); // Wait for any child
         if (wpid == -1) {
             perror("wait failed");
             break; // Exit loop on wait error
         }

        children_finished++;
        if (WIFEXITED(status)) {
            printf("[Master] Child PID %d exited with status %d\n", wpid, WEXITSTATUS(status));
        } else if (WIFSIGNALED(status)) {
            printf("[Master] Child PID %d killed by signal %d\n", wpid, WTERMSIG(status));
        } else {
             printf("[Master] Child PID %d finished with unexpected status\n", wpid);
        }
    }

    free(child_pids);
    printf("[Master] All child processes finished. Master exiting.\n");
}


// Helper to read until a delimiter or buffer full
ssize_t recv_until_delim(int sockfd, char *buffer, size_t max_len, char delim) {
    size_t current_len = 0;
    ssize_t n;
    while (current_len < max_len - 1) { // Leave space for null terminator
        n = recv(sockfd, buffer + current_len, 1, 0);
        if (n == 1) {
            if (buffer[current_len] == delim) {
                buffer[current_len] = '\0'; // Replace delim with null terminator
                return current_len; // Return length excluding delimiter
            }
            current_len++;
        } else if (n == 0) {
            // Connection closed
            buffer[current_len] = '\0';
            return -2; // Special code for connection closed
        } else {
            // Error
            if (errno == EINTR) continue;
            perror("recv_until_delim failed");
            return -1; // Indicate error
        }
    }
    // Buffer full without finding delimiter
    buffer[current_len] = '\0';
    return -3; // Special code for buffer full
}


// --- Slave Process Logic ---
void slave(int listen_port, int slave_id) {
    printf("Running as SLAVE node (ID: %d) on port %d\n", slave_id, listen_port);

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd == -1) {
        error_exit("slave socket creation failed");
    }

    // Allow reuse of address
    int opt = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt(SO_REUSEADDR) failed");
        // Non-fatal, continue
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY); // Bind to all interfaces
    serv_addr.sin_port = htons(listen_port);

    if (bind(listen_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        error_exit("slave bind failed");
    }

    if (listen(listen_fd, 1) < 0) { // Listen for only one connection at a time
        error_exit("slave listen failed");
    }

    printf("[Slave %d] Waiting for connection on port %d...\n", slave_id, listen_port);

    struct sockaddr_in cli_addr;
    socklen_t clilen = sizeof(cli_addr);
    int conn_fd = accept(listen_fd, (struct sockaddr *)&cli_addr, &clilen);
    if (conn_fd < 0) {
        error_exit("slave accept failed");
    }

    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &cli_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
    printf("[Slave %d] Connection accepted from %s:%d\n", slave_id, client_ip, ntohs(cli_addr.sin_port));

    // 1. Receive n (dimension) - read until newline
    char n_str[INT_STR_SIZE];
    ssize_t n_len = recv_until_delim(conn_fd, n_str, sizeof(n_str), '\n');
    if (n_len < 0) {
         fprintf(stderr, "[Slave %d] Failed to receive n (error code: %zd)\n", slave_id, n_len);
         close(conn_fd);
         close(listen_fd);
         exit(EXIT_FAILURE);
    }
    int n = atoi(n_str);
    if (n <= 0) {
         fprintf(stderr, "[Slave %d] Received invalid n value: %s (%d)\n", slave_id, n_str, n);
         close(conn_fd);
         close(listen_fd);
         exit(EXIT_FAILURE);
    }
     printf("[Slave %d] Received n = %d\n", slave_id, n);


    // 2. Receive rows for this slave - read until newline
    char rows_str[INT_STR_SIZE];
    ssize_t rows_len = recv_until_delim(conn_fd, rows_str, sizeof(rows_str), '\n');
    if (rows_len < 0) {
         fprintf(stderr, "[Slave %d] Failed to receive rows (error code: %zd)\n", slave_id, rows_len);
         close(conn_fd);
         close(listen_fd);
         exit(EXIT_FAILURE);
    }
    int rows = atoi(rows_str);
     if (rows < 0) { // Allow rows == 0
         fprintf(stderr, "[Slave %d] Received invalid rows value: %s (%d)\n", slave_id, rows_str, rows);
         close(conn_fd);
         close(listen_fd);
         exit(EXIT_FAILURE);
     }
    printf("[Slave %d] Received rows = %d\n", slave_id, rows);


    // 3. Receive Matrix Data
    int *matrix_buffer = NULL;
    size_t expected_bytes = 0;
    if (rows > 0) {
        expected_bytes = (size_t)rows * n * sizeof(int);
        matrix_buffer = (int *)malloc(expected_bytes);
        if (matrix_buffer == NULL) {
            fprintf(stderr, "[Slave %d] Failed to allocate memory for matrix buffer (%zu bytes)\n", slave_id, expected_bytes);
            close(conn_fd);
            close(listen_fd);
            exit(EXIT_FAILURE);
        }

        printf("[Slave %d] Receiving matrix data (%zu bytes)...\n", slave_id, expected_bytes);
        ssize_t total_received = recv_all(conn_fd, matrix_buffer, expected_bytes);

        if (total_received < 0) {
             fprintf(stderr, "[Slave %d] Error receiving matrix data.\n", slave_id);
             free(matrix_buffer);
             close(conn_fd);
             close(listen_fd);
             exit(EXIT_FAILURE);
        } else if ((size_t)total_received != expected_bytes) {
            // Should not happen with recv_all unless connection closed prematurely
             fprintf(stderr, "[Slave %d] Incomplete matrix data received. Expected %zu, got %zd\n",
                     slave_id, expected_bytes, total_received);
             free(matrix_buffer);
             close(conn_fd);
             close(listen_fd);
             exit(EXIT_FAILURE);
        }
         printf("[Slave %d] Received matrix shape: (%d, %d)\n", slave_id, rows, n);

        // Optional: Print first few elements to verify
        // printf("[Slave %d] First few elements: ", slave_id);
        // int limit = (rows * n < 10) ? rows * n : 10;
        // for (int i = 0; i < limit; ++i) printf("%d ", matrix_buffer[i]);
        // printf("\n");

    } else {
         printf("[Slave %d] Received 0 rows, no matrix data expected.\n", slave_id);
    }


    // 4. Send Acknowledgment
    char ack_msg[100];
    char hostname[64];
    gethostname(hostname, sizeof(hostname) -1);
    hostname[sizeof(hostname)-1] = '\0'; // Ensure null termination
    snprintf(ack_msg, sizeof(ack_msg), "ack from %s, port %d", hostname, listen_port);

    printf("[Slave %d] Sending ACK: %s\n", slave_id, ack_msg);
    if (send_all(conn_fd, ack_msg, strlen(ack_msg)) < 0) {
         fprintf(stderr, "[Slave %d] Failed to send ACK.\n", slave_id);
         // Continue to cleanup even if ACK fails
    }

    // 5. Cleanup
    printf("[Slave %d] Closing connection.\n", slave_id);
    close(conn_fd);
    close(listen_fd); // Close listening socket after handling one connection
    if (matrix_buffer) {
        free(matrix_buffer);
    }
    printf("[Slave %d] Finished.\n", slave_id);
}

// --- Main Function ---
int main(int argc, char *argv[]) {
    // Seed random number generator once
    srand(time(NULL));

    if (argc == 2) {
        // Master Mode
        int n = atoi(argv[1]);
        if (n <= 0) {
            fprintf(stderr, "Usage: %s <n> (for master)\n", argv[0]);
            fprintf(stderr, "       n must be a positive integer.\n");
            return EXIT_FAILURE;
        }
        master(n);
    } else if (argc == 3) {
        // Slave Mode
        int p = atoi(argv[1]); // Port
        int s = atoi(argv[2]); // Slave ID (for logging)
         if (p <= 0 || p > 65535) {
            fprintf(stderr, "Usage: %s <port> <slave_id> (for slave)\n", argv[0]);
            fprintf(stderr, "       port must be between 1 and 65535.\n");
            return EXIT_FAILURE;
         }
         if (s < 0) {
             fprintf(stderr, "Usage: %s <port> <slave_id> (for slave)\n", argv[0]);
             fprintf(stderr, "       slave_id should be non-negative.\n");
             return EXIT_FAILURE;
         }
        slave(p, s);
    } else {
        fprintf(stderr, "Usage:\n");
        fprintf(stderr, "  Master: %s <n>\n", argv[0]);
        fprintf(stderr, "  Slave:  %s <port> <slave_id>\n", argv[0]);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}