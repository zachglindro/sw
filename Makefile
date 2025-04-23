CC = gcc
CFLAGS = -Wall -Wextra -pthread -O2  # Include pthread link flag, Optimization O2
LDFLAGS = -pthread # Sometimes needed separately depending on linker/gcc version

# Source files
UTILS_SRC = utils.c
MASTER_SRC = master.c $(UTILS_SRC)
SLAVE_SRC = slave.c $(UTILS_SRC)

# Object files (optional, can directly compile sources to executables)

# Executables
MASTER_EXEC = master
SLAVE_EXEC = slave

# Targets
all: $(MASTER_EXEC) $(SLAVE_EXEC)

$(MASTER_EXEC): $(MASTER_SRC) common.h
	$(CC) $(CFLAGS) $(MASTER_SRC) -o $(MASTER_EXEC) $(LDFLAGS) -lm # Added -lm for time_diff if needed

$(SLAVE_EXEC): $(SLAVE_SRC) common.h
	$(CC) $(CFLAGS) $(SLAVE_SRC) -o $(SLAVE_EXEC) $(LDFLAGS) -lm # Added -lm for time_diff if needed

clean:
	rm -f $(MASTER_EXEC) $(SLAVE_EXEC) *.o # Remove executables and object files

.PHONY: all clean