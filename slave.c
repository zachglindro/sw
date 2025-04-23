#include "common.h"

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <n> <port>\n", argv[0]);
        fprintf(stderr, " <n>: Expected size of the square matrix (for verification)\n");
        fprintf(stderr, " <port>: Port number to listen on\n");
        exit(EXIT_FAILURE);
    }

    int n_expected = atoi(argv[1]); // Expected matrix width from master
    int port = atoi(argv[2]);

    if (n_expected <= 0) {
        fprintf(stderr, "Error: Matrix size n must be a positive integer.\n");
        exit(EXIT_FAILURE);
    }
     if (port <= 0 || port > 65535) {
        fprintf(stderr, "Error: Invalid port number %d.\n", port);
        exit(EXIT_FAILURE);
    }

    printf("SLAVE Instance: Listening on port %d, expecting matrix width n = %d\n", port, n_expected);

    int listen_sockfd, conn_sockfd;
    struct sockaddr_in serv_addr, cli_addr;
    socklen_t clilen;

    // 1. Create listening socket
    listen_sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sockfd < 0) die("Slave socket creation failed");

    // Set SO_REUSEADDR to allow immediate reuse of the port
    int optval = 1;
    if (setsockopt(listen_sockfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0) {
        perror("setsockopt(SO_REUSEADDR) failed");
        // Continue, but might have issues restarting quickly
    }


    // 2. Prepare server address structure
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY; // Listen on any interface
    serv_addr.sin_port = htons(port);

    // 3. Bind the socket to the address and port
    if (bind(listen_sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        fprintf(stderr, "Slave bind failed on port %d", port);
        die(""); // perror message included in die()
    }


    // 4. Listen for incoming connections
    if (listen(listen_sockfd, 5) < 0) { // Allow backlog of 5 pending connections
        die("Slave listen failed");
    }
    printf("Slave listening on port %d. Waiting for master connection...\n", port);


    // 5. Accept a connection
    clilen = sizeof(cli_addr);
    conn_sockfd = accept(listen_sockfd, (struct sockaddr *) &cli_addr, &clilen);
    if (conn_sockfd < 0) die("Slave accept failed");

     // Get client IP address for logging
     char cli_ip[INET_ADDRSTRLEN];
     inet_ntop(AF_INET, &(cli_addr.sin_addr), cli_ip, INET_ADDRSTRLEN);
     printf("Master connected from %s:%d\n", cli_ip, ntohs(cli_addr.sin_port));

     // Close the listening socket now that we have a connection
     // (If expecting multiple connections sequentially, keep it open until done)
     close(listen_sockfd);


    // --- Receive Submatrix ---
    struct timeval time_before, time_after;
    gettimeofday(&time_before, NULL); // Start timing *after* connection accepted

    // 6. Receive submatrix dimensions
    int sub_rows_net, sub_cols_net; // Network byte order
    int sub_rows, sub_cols;       // Host byte order
    ssize_t bytes_received;

    printf("Receiving submatrix dimensions...\n");
    bytes_received = recv_full(conn_sockfd, &sub_rows_net, sizeof(sub_rows_net), 0);
     if (bytes_received != sizeof(sub_rows_net)) {
        fprintf(stderr, "Slave failed to receive sub_rows completely (received %zd bytes). Master likely disconnected.\n", bytes_received);
        close(conn_sockfd);
        exit(EXIT_FAILURE);
    }
    sub_rows = ntohl(sub_rows_net); // Convert from network to host byte order


    bytes_received = recv_full(conn_sockfd, &sub_cols_net, sizeof(sub_cols_net), 0);
    if (bytes_received != sizeof(sub_cols_net)) {
        fprintf(stderr, "Slave failed to receive sub_cols completely (received %zd bytes). Master likely disconnected.\n", bytes_received);
        close(conn_sockfd);
        exit(EXIT_FAILURE);
    }
    sub_cols = ntohl(sub_cols_net); // Convert from network to host byte order


    printf("Received dimensions: %d rows, %d columns.\n", sub_rows, sub_cols);

    if (sub_cols != n_expected) {
        fprintf(stderr, "Error: Received matrix width %d does not match expected width %d.\n", sub_cols, n_expected);
        close(conn_sockfd);
        exit(EXIT_FAILURE);
    }
     if (sub_rows < 0 ) { // Check for invalid row count
         fprintf(stderr, "Error: Received invalid number of rows: %d\n", sub_rows);
         close(conn_sockfd);
         exit(EXIT_FAILURE);
     }
     if (sub_rows == 0) {
          printf("Info: Received 0 rows. Nothing more to receive.\n");
          // Still need to send ACK even if 0 rows received
     }


    // 7. Allocate memory for submatrix (as a 2D array for easier access/printing)
    int **submatrix = NULL;
    if (sub_rows > 0) {
         submatrix = (int **)malloc(sub_rows * sizeof(int *));
         if (!submatrix) die("Slave failed to allocate memory for submatrix rows");

         for (int i = 0; i < sub_rows; ++i) {
             submatrix[i] = (int *)malloc(sub_cols * sizeof(int));
             if (!submatrix[i]) {
                 fprintf(stderr, "Slave failed to allocate memory for submatrix row %d\n", i);
                  // Free previously allocated rows
                  for(int k=0; k < i; ++k) free(submatrix[k]);
                  free(submatrix);
                 close(conn_sockfd);
                 die("Submatrix row allocation failed");
             }
         }
         printf("Memory allocated for %d x %d submatrix.\n", sub_rows, sub_cols);
    } else {
         printf("No memory allocated as 0 rows were received.\n");
    }


    // 8. Receive submatrix data row by row
    if (sub_rows > 0) {
        printf("Receiving submatrix data...\n");
        for (int i = 0; i < sub_rows; ++i) {
            size_t row_size_bytes = sub_cols * sizeof(int);
            // Receive directly into a temporary buffer for network ordered data
            int* row_data_net = malloc(row_size_bytes);
             if (!row_data_net) {
                 fprintf(stderr, "Slave: Failed to allocate memory for receive row buffer\n");
                 // Cleanup allocated matrix rows
                 for(int k=0; k < sub_rows; ++k) if(submatrix[k]) free(submatrix[k]);
                 free(submatrix);
                 close(conn_sockfd);
                 die("Receive buffer allocation failed");
             }

            bytes_received = recv_full(conn_sockfd, row_data_net, row_size_bytes, 0);

            if (bytes_received != row_size_bytes) {
                fprintf(stderr, "Slave failed to receive row %d data completely (received %zd bytes). Master likely disconnected.\n", i, bytes_received);
                 free(row_data_net);
                  // Cleanup allocated matrix rows
                 for(int k=0; k < sub_rows; ++k) if(submatrix[k]) free(submatrix[k]);
                 free(submatrix);
                close(conn_sockfd);
                exit(EXIT_FAILURE);
            }

            // Convert the received row from network to host byte order and store it
            for (int j = 0; j < sub_cols; ++j) {
                submatrix[i][j] = ntohl(row_data_net[j]);
            }
            free(row_data_net); // Free the temporary buffer
        }
        printf("Submatrix data received successfully.\n");
    }

    // 9. Send acknowledgment
    printf("Sending ACK to master...\n");
    char ack_msg = ACK_MSG;
    ssize_t bytes_sent = send_full(conn_sockfd, &ack_msg, sizeof(ack_msg), 0);
    if (bytes_sent != sizeof(ack_msg)) {
        fprintf(stderr, "Slave failed to send ACK completely.\n");
        // Don't necessarily exit, maybe master got partial ACK? But log the error.
    } else {
         printf("ACK sent.\n");
    }

    gettimeofday(&time_after, NULL); // Stop timing after sending ACK

    // Close connection with master
    close(conn_sockfd);
    printf("Connection with master closed.\n");

    // --- Calculate and Print Elapsed Time ---
    double time_elapsed = time_diff(&time_before, &time_after);
    printf("\n----------------------------------------\n");
    printf("SLAVE: Reception process finished.\n");
    printf("SLAVE: Elapsed time: %.6f seconds\n", time_elapsed);
    printf("----------------------------------------\n");


    // --- Verification (Print first few elements) ---
    if (sub_rows > 0 && submatrix != NULL) {
        printf("Verification: Received Submatrix (first %d x %d elements):\n", PRINT_LIMIT, PRINT_LIMIT);
        int print_rows = sub_rows < PRINT_LIMIT ? sub_rows : PRINT_LIMIT;
        int print_cols = sub_cols < PRINT_LIMIT ? sub_cols : PRINT_LIMIT;
        for (int i = 0; i < print_rows; ++i) {
            for (int j = 0; j < print_cols; ++j) {
                printf("%4d ", submatrix[i][j]);
            }
            printf("\n");
        }

         // --- Cleanup ---
         printf("Slave cleaning up received matrix...\n");
         for (int i = 0; i < sub_rows; ++i) {
             free(submatrix[i]);
         }
         free(submatrix);

    } else {
        printf("Verification: No matrix data received (0 rows).\n");
    }


    printf("Slave finished.\n");
    return 0;
}