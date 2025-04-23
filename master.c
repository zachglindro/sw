#include "common.h"

// Function executed by each master thread to send data to a slave
void* send_submatrix_to_slave(void* arg) {
    MasterThreadArgs *args = (MasterThreadArgs*)arg;
    int sockfd;
    struct sockaddr_in serv_addr;
    int i, row;
    ssize_t bytes_sent, bytes_received;

    printf("Thread %d: Connecting to slave %s:%d\n", args->thread_id, args->slave_details.ip_addr, args->slave_details.port);

    // 1. Create socket
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Master thread socket creation failed");
        args->ack_received[args->thread_id] = -1; // Indicate error
        pthread_exit(NULL);
    }

    // 2. Prepare server address structure
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(args->slave_details.port);
    if (inet_pton(AF_INET, args->slave_details.ip_addr, &serv_addr.sin_addr) <= 0) {
        fprintf(stderr, "Master thread invalid address/ Address not supported: %s\n", args->slave_details.ip_addr);
        close(sockfd);
        args->ack_received[args->thread_id] = -1; // Indicate error
        pthread_exit(NULL);
    }

    // 3. Connect to slave
    if (connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("Master thread connection failed");
        fprintf(stderr, " >> Failed to connect to %s:%d\n", args->slave_details.ip_addr, args->slave_details.port);
        close(sockfd);
        args->ack_received[args->thread_id] = -1; // Indicate error
        pthread_exit(NULL);
    }
    printf("Thread %d: Connected to %s:%d\n", args->thread_id, args->slave_details.ip_addr, args->slave_details.port);


    // 4. Send submatrix dimensions (number of rows, number of columns)
    int rows_net = htonl(args->num_rows); // Convert to network byte order
    int n_net = htonl(args->n);         // Convert to network byte order

    printf("Thread %d: Sending dimensions (%d rows, %d cols) to %s:%d\n", args->thread_id, args->num_rows, args->n, args->slave_details.ip_addr, args->slave_details.port);

    bytes_sent = send_full(sockfd, &rows_net, sizeof(rows_net), 0);
    if (bytes_sent != sizeof(rows_net)) {
         fprintf(stderr, "Thread %d: Failed to send num_rows completely to %s:%d\n", args->thread_id, args->slave_details.ip_addr, args->slave_details.port);
         close(sockfd);
         args->ack_received[args->thread_id] = -1;
         pthread_exit(NULL);
    }


    bytes_sent = send_full(sockfd, &n_net, sizeof(n_net), 0);
     if (bytes_sent != sizeof(n_net)) {
         fprintf(stderr, "Thread %d: Failed to send n (cols) completely to %s:%d\n", args->thread_id, args->slave_details.ip_addr, args->slave_details.port);
         close(sockfd);
         args->ack_received[args->thread_id] = -1;
         pthread_exit(NULL);
    }


    // 5. Send submatrix data
    printf("Thread %d: Sending %d rows starting from row %d to %s:%d...\n", args->thread_id, args->num_rows, args->start_row, args->slave_details.ip_addr, args->slave_details.port);
    for (i = 0; i < args->num_rows; ++i) {
        row = args->start_row + i;
        // Important: Send each element individually after converting to network byte order
        // Or, more efficiently, send the whole row at once if data is contiguous
        // Assuming int is 4 bytes here. For portability, check sizeof(int).
        // Sending row by row:
        size_t row_size_bytes = args->n * sizeof(int);
        int* row_data_net = malloc(row_size_bytes); // Temporary buffer for network order data
        if (!row_data_net) {
             fprintf(stderr, "Thread %d: Failed to allocate memory for row buffer\n", args->thread_id);
             close(sockfd);
             args->ack_received[args->thread_id] = -1;
             pthread_exit(NULL);
        }
        // Convert the row to network byte order
        for(int j=0; j < args->n; ++j) {
            row_data_net[j] = htonl(args->matrix[row][j]);
        }

        bytes_sent = send_full(sockfd, row_data_net, row_size_bytes, 0);
        free(row_data_net); // Free the temporary buffer

        if (bytes_sent != row_size_bytes) {
            fprintf(stderr, "Thread %d: Failed to send row %d data completely to %s:%d\n", args->thread_id, row, args->slave_details.ip_addr, args->slave_details.port);
            close(sockfd);
            args->ack_received[args->thread_id] = -1;
            pthread_exit(NULL);
        }
    }
    printf("Thread %d: Submatrix data sent to %s:%d.\n", args->thread_id, args->slave_details.ip_addr, args->slave_details.port);


    // 6. Receive acknowledgment
    char ack_buffer;
    printf("Thread %d: Waiting for ACK from %s:%d...\n", args->thread_id, args->slave_details.ip_addr, args->slave_details.port);
    bytes_received = recv_full(sockfd, &ack_buffer, sizeof(ack_buffer), 0);

    if (bytes_received == sizeof(ack_buffer) && ack_buffer == ACK_MSG) {
        printf("Thread %d: ACK received from %s:%d.\n", args->thread_id, args->slave_details.ip_addr, args->slave_details.port);
        args->ack_received[args->thread_id] = 1; // Mark ACK as received
    } else if (bytes_received == 0) {
        fprintf(stderr, "Thread %d: Connection closed by slave %s:%d before ACK received.\n", args->thread_id, args->slave_details.ip_addr, args->slave_details.port);
        args->ack_received[args->thread_id] = -1; // Error
    } else if (bytes_received < 0) {
        perror("Master thread recv ACK failed");
        args->ack_received[args->thread_id] = -1; // Error
    }
     else {
        fprintf(stderr, "Thread %d: Invalid ACK received from %s:%d (received %d bytes, char '%c'). Expected '%c'\n", args->thread_id, args->slave_details.ip_addr, args->slave_details.port, (int)bytes_received, ack_buffer, ACK_MSG);
        args->ack_received[args->thread_id] = -1; // Error
    }


    // 7. Close socket
    close(sockfd);
    printf("Thread %d: Connection closed with %s:%d.\n", args->thread_id, args->slave_details.ip_addr, args->slave_details.port);
    pthread_exit(NULL); // Use NULL for success, non-NULL can indicate error but we use ack_received array
}


int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <n>\n", argv[0]);
        fprintf(stderr, " <n>: Size of the square matrix\n");
        exit(EXIT_FAILURE);
    }

    int n = atoi(argv[1]);
    if (n <= 0) {
        fprintf(stderr, "Error: Matrix size n must be a positive integer.\n");
        exit(EXIT_FAILURE);
    }

    printf("MASTER Instance: Matrix size n = %d\n", n);

    // --- Read Configuration ---
    SlaveInfo slaves[MAX_SLAVES];
    int t = 0; // Number of slaves
    FILE *fp = fopen(CONFIG_FILE, "r");
    if (!fp) {
        die("Error opening config file");
    }
    char line[MAX_LINE_LEN];
    while (t < MAX_SLAVES && fgets(line, sizeof(line), fp)) {
         // Skip empty lines or comments (optional)
        if (line[0] == '\n' || line[0] == '#') continue;

        if (sscanf(line, "%s %d", slaves[t].ip_addr, &slaves[t].port) == 2) {
             // Basic validation (could add more robust IP/port checks)
             if (slaves[t].port <= 0 || slaves[t].port > 65535) {
                 fprintf(stderr, "Warning: Invalid port number %d for slave %d in config file. Skipping.\n", slaves[t].port, t);
                 continue;
             }
             printf("Read Slave %d: IP=%s, Port=%d\n", t, slaves[t].ip_addr, slaves[t].port);
             t++;
        } else {
             fprintf(stderr, "Warning: Malformed line in config file: %s. Skipping.\n", line);
        }
    }
    fclose(fp);

    if (t == 0) {
        fprintf(stderr, "Error: No valid slave configurations found in %s\n", CONFIG_FILE);
        exit(EXIT_FAILURE);
    }
    printf("Total slaves found: t = %d\n", t);

    if (n < t) {
        fprintf(stderr,"Warning: Matrix size n (%d) is smaller than the number of slaves t (%d). Some slaves will receive 0 rows.\n", n, t);
    }
     if (n % t != 0) {
        printf("Info: Matrix size n (%d) is not perfectly divisible by the number of slaves t (%d). Rows will be distributed unevenly.\n", n, t);
    }


    // --- Create and Initialize Matrix M ---
    printf("Creating %d x %d matrix M...\n", n, n);
    int **matrix = (int **)malloc(n * sizeof(int *));
    if (!matrix) die("Failed to allocate memory for matrix rows");
    srand(time(NULL)); // Seed random number generator
    for (int i = 0; i < n; ++i) {
        matrix[i] = (int *)malloc(n * sizeof(int));
        if (!matrix[i]) {
            fprintf(stderr, "Failed to allocate memory for matrix row %d\n", i);
            // Free previously allocated rows before exiting
            for(int k=0; k < i; ++k) free(matrix[k]);
            free(matrix);
            die("Matrix row allocation failed");
        }
        for (int j = 0; j < n; ++j) {
            matrix[i][j] = (rand() % 1000) + 1; // Random non-zero positive integer (1-1000)
        }
    }
    printf("Matrix M created and filled with random positive integers.\n");

     // Optional: Print first few elements of the original matrix
    printf("Original Matrix M (first %d x %d elements):\n", PRINT_LIMIT, PRINT_LIMIT);
    for(int i=0; i < n && i < PRINT_LIMIT; ++i) {
        for (int j=0; j < n && j < PRINT_LIMIT; ++j) {
            printf("%4d ", matrix[i][j]);
        }
        printf("\n");
    }


    // --- Distribute Submatrices using Threads ---
    pthread_t threads[t];
    MasterThreadArgs thread_args[t];
    int ack_status[t]; // 0 = pending, 1 = success, -1 = error
    memset(ack_status, 0, sizeof(ack_status)); // Initialize all to pending

    int base_rows_per_slave = n / t;
    int remainder_rows = n % t;
    int current_row = 0;

    printf("Starting distribution to %d slaves...\n", t);
    struct timeval time_before, time_after;
    gettimeofday(&time_before, NULL); // Start timing

    for (int i = 0; i < t; ++i) {
        int rows_for_this_slave = base_rows_per_slave + (i < remainder_rows ? 1 : 0);

        thread_args[i].thread_id = i;
        thread_args[i].matrix = matrix;
        thread_args[i].start_row = current_row;
        thread_args[i].num_rows = rows_for_this_slave;
        thread_args[i].n = n;
        thread_args[i].slave_details = slaves[i]; // Copy slave info
        thread_args[i].ack_received = ack_status; // Pass pointer to the status array

        if (pthread_create(&threads[i], NULL, send_submatrix_to_slave, &thread_args[i]) != 0) {
            perror("Failed to create master thread");
            // Mark as error and continue trying to launch others? Or abort? Abort is safer.
             fprintf(stderr, "Aborting due to thread creation failure for slave %d\n", i);
             // Attempt cleanup (difficult to cancel running threads safely)
              for(int k=0; k < n; ++k) free(matrix[k]);
              free(matrix);
             exit(EXIT_FAILURE);
        }

        current_row += rows_for_this_slave;
    }

    // --- Wait for all threads to complete and check ACKs ---
    printf("Waiting for all slaves to acknowledge...\n");
    int all_acks_received = 1;
    for (int i = 0; i < t; ++i) {
        pthread_join(threads[i], NULL); // Wait for thread i to finish
        if (ack_status[i] == 1) {
            printf("Master confirmed ACK from slave %d (%s:%d)\n", i, slaves[i].ip_addr, slaves[i].port);
        } else {
             fprintf(stderr, "Master failed to get ACK or encountered error with slave %d (%s:%d). Status: %d\n", i, slaves[i].ip_addr, slaves[i].port, ack_status[i]);
            all_acks_received = 0;
            // Continue joining other threads, but note the failure
        }
    }

     gettimeofday(&time_after, NULL); // Stop timing after all threads joined

     if (all_acks_received) {
        printf("All %d slaves acknowledged successfully.\n", t);
     } else {
         fprintf(stderr, "One or more slaves failed to acknowledge or encountered an error during communication.\n");
         // Consider if the program should exit differently on failure
     }


    // --- Calculate and Print Elapsed Time ---
    double time_elapsed = time_diff(&time_before, &time_after);
    printf("\n----------------------------------------\n");
    printf("MASTER: Distribution process finished.\n");
    printf("MASTER: Elapsed time: %.6f seconds\n", time_elapsed);
    printf("----------------------------------------\n");


    // --- Cleanup ---
    printf("Master cleaning up...\n");
    for (int i = 0; i < n; ++i) {
        free(matrix[i]);
    }
    free(matrix);
    printf("Master finished.\n");

    return 0;
}