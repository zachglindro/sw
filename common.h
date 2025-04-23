#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <errno.h>
#include <time.h> // For srand

#define MAX_SLAVES 64          // Maximum number of slaves supported
#define CONFIG_FILE "config.txt"
#define MAX_LINE_LEN 256
#define ACK_MSG 'K'            // Acknowledgment character
#define PRINT_LIMIT 5          // Limit number of matrix elements to print for verification

// Structure to hold slave information
typedef struct {
    char ip_addr[INET_ADDRSTRLEN];
    int port;
} SlaveInfo;

// Structure to pass arguments to master's sender threads
typedef struct {
    int thread_id;              // Identifier for the thread/slave
    int **matrix;               // Pointer to the full matrix M
    int start_row;            // Starting row index for this slave's submatrix
    int num_rows;             // Number of rows in this slave's submatrix
    int n;                      // Dimension of the square matrix (total columns)
    SlaveInfo slave_details;    // IP and port of the target slave
    int *ack_received;          // Pointer to an array element to signal ack receipt
} MasterThreadArgs;

// --- Utility Function Prototypes (implemented in utils.c) ---

// Error handling
void die(const char *message);

// Timing
double time_diff(struct timeval *start, struct timeval *end);

// Network helpers (handle partial send/recv)
ssize_t send_full(int sockfd, const void *buf, size_t len, int flags);
ssize_t recv_full(int sockfd, void *buf, size_t len, int flags);

#endif // COMMON_H