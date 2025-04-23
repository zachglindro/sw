#include "common.h"

// --- Utility Function Implementations ---

// Print error message and exit
void die(const char *message) {
    perror(message);
    exit(EXIT_FAILURE);
}

// Calculate time difference in seconds
double time_diff(struct timeval *start, struct timeval *end) {
    return (end->tv_sec - start->tv_sec) + (end->tv_usec - start->tv_usec) / 1e6;
}

// Send exactly 'len' bytes or return an error
ssize_t send_full(int sockfd, const void *buf, size_t len, int flags) {
    size_t total_sent = 0;
    ssize_t n_sent;
    const char *ptr = (const char *)buf;

    while (total_sent < len) {
        n_sent = send(sockfd, ptr + total_sent, len - total_sent, flags);
        if (n_sent < 0) {
            if (errno == EINTR) continue; // Interrupted by signal, try again
            perror("send");
            return -1; // Return error
        }
        if (n_sent == 0) {
            // Connection closed by peer (should not happen here ideally)
            fprintf(stderr, "send_full: Connection closed by peer\n");
            return -1; // Indicate connection closed issue
        }
        total_sent += n_sent;
    }
    // Ensure we didn't somehow send more (shouldn't happen with send)
    if (total_sent != len) {
         fprintf(stderr, "send_full: Logic error, sent %zu != %zu\n", total_sent, len);
         return -1;
    }
    return total_sent; // Success, return total bytes sent (which is len)
}


// Receive exactly 'len' bytes or return an error / EOF
ssize_t recv_full(int sockfd, void *buf, size_t len, int flags) {
    size_t total_recv = 0;
    ssize_t n_recv;
    char *ptr = (char *)buf;

    while (total_recv < len) {
        n_recv = recv(sockfd, ptr + total_recv, len - total_recv, flags);
        if (n_recv < 0) {
            if (errno == EINTR) continue; // Interrupted by signal, try again
            perror("recv");
            return -1; // Return error
        }
        if (n_recv == 0) {
            // Connection closed by peer (EOF)
            // Return the number of bytes successfully received before EOF
            return total_recv;
        }
        total_recv += n_recv;
    }
     // Ensure we didn't somehow receive more (shouldn't happen with recv)
    if (total_recv != len) {
         fprintf(stderr, "recv_full: Logic error, received %zu != %zu\n", total_recv, len);
         return -1;
    }
    return total_recv; // Success, return total bytes received (which is len)
}