
#include "helper_funcs.h"
#include "connection.h"
#include "debug.h"
#include "response.h"
#include "request.h"
#include "queue.h"

#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/file.h>

queue_t *queue = NULL;
pthread_mutex_t mutex;

void handle_connection(int);

void handle_get(conn_t *);
void handle_put(conn_t *);
void handle_unsupported(conn_t *);
void do_work(void);
void auditlog(conn_t *conn, const Response_t *res);
typedef void *(*worker_function)(void *);
int main(int argc, char **argv) {
    // use optind, accquired from https://stackoverflow.com/questions/46636641/how-does-optind-get-assigned-in-c
    int opt;
    int init_threads = 4;
    pthread_mutex_init(&mutex, NULL);
    while ((opt = getopt(argc, argv, "t:")) != -1) {
        switch (opt) {
        case 't': init_threads = atoi(optarg); break;
        }
    }

    // if (argc < 2) {
    //     warnx("wrong arguments: %s port_num", argv[0]);
    //     fprintf(stderr, "usage: %s <port>\n", argv[0]);
    //     return EXIT_FAILURE;
    // }
    if (optind >= argc) {
        fprintf(stderr, "No argument after option\n");
        return EXIT_FAILURE;
    }

    char *endptr = NULL;
    size_t port = (size_t) strtoull(argv[optind], &endptr, 10);
    if (endptr && *endptr != '\0') {
        warnx("invalid port number: %s", argv[optind]);
        return EXIT_FAILURE;
    }

    signal(SIGPIPE, SIG_IGN);
    Listener_Socket sock;
    listener_init(&sock, port);

    pthread_t worker_threads[init_threads];

    queue = queue_new(init_threads);
    for (int i = 0; i < init_threads; i++) {
        pthread_create(&worker_threads[i], NULL, (worker_function) do_work, NULL);
    }

    while (1) {
        uintptr_t connfd = listener_accept(&sock);
        queue_push(queue, (void *) connfd);
    }
    pthread_mutex_destroy(&mutex);
    queue_delete(&queue);

    return EXIT_SUCCESS;
}

void do_work(void) {
    while (1) {
        int sd;
        queue_pop(queue, (void **) &sd);
        handle_connection(sd);
        close(sd);
    }
}

void handle_connection(int connfd) {

    conn_t *conn = conn_new(connfd);

    const Response_t *res = conn_parse(conn);

    if (res != NULL) {
        auditlog(conn, res);
        conn_send_response(conn, res);
    } else {
        //debug("%s", conn_str(conn));
        const Request_t *req = conn_get_request(conn);
        if (req == &REQUEST_GET) {
            handle_get(conn);
        } else if (req == &REQUEST_PUT) {
            handle_put(conn);
        } else {
            handle_unsupported(conn);
        }
    }

    conn_delete(&conn);
}

void handle_get(conn_t *conn) {

    char *uri = conn_get_uri(conn);
    //debug("GET request not implemented. But, we want to get %s", uri);
    // What are the steps in here?

    // 1. Open the file.
    // If  open it returns < 0, then use the result appropriately
    //   a. Cannot access -- use RESPONSE_FORBIDDEN
    //   b. Cannot find the file -- use RESPONSE_NOT_FOUND
    //   c. other error? -- use RESPONSE_INTERNAL_SERVER_ERROR
    // (hint: check errno for these cases)!
    int fd = open(uri, O_RDONLY);
    if (fd < 0) {

        // Check if the file is a directory, because directories *will*
        // open, but are not valid.
        // (hint: checkout the macro "S_IFDIR", which you can use after you call fstat!)

        if ((errno == EACCES) || errno == EISDIR) {
            auditlog(conn, &RESPONSE_FORBIDDEN);
            conn_send_response(conn, &RESPONSE_FORBIDDEN);

        } else if (errno == ENOENT) {
            auditlog(conn, &RESPONSE_NOT_FOUND);
            conn_send_response(conn, &RESPONSE_NOT_FOUND);
        } else {
            auditlog(conn, &RESPONSE_INTERNAL_SERVER_ERROR);
            conn_send_response(conn, &RESPONSE_INTERNAL_SERVER_ERROR);
        }
        close(fd);
        return;
    }
    // add thread

    pthread_mutex_lock(&mutex);
    flock(fd, LOCK_SH);

    // 2. Get the size of the file.
    // (hint: checkout the function fstat)!
    struct stat st;
    if (fstat(fd, &st) < 0) {
        conn_send_response(conn, &RESPONSE_INTERNAL_SERVER_ERROR);
        close(fd);
        return;
    }
    pthread_mutex_unlock(&mutex);
    auditlog(conn, &RESPONSE_OK);
    // Get the size of the file.

    // 4. Send the file
    // (hint: checkout the conn_send_file function!)
    conn_send_file(conn, fd, st.st_size);
    flock(fd, LOCK_UN);

    close(fd);
}

void handle_unsupported(conn_t *conn) {
    //debug("handling unsupported request");
    // send responses
    auditlog(conn, &RESPONSE_NOT_IMPLEMENTED);
    conn_send_response(conn, &RESPONSE_NOT_IMPLEMENTED);
}

void handle_put(conn_t *conn) {
    pthread_mutex_lock(&mutex);
    char *uri = conn_get_uri(conn);
    const Response_t *res = NULL;
    int fd;
    // Check if file already exists before opening it.
    bool existed = access(uri, F_OK) == 0;
    // Open the file..
    if (!existed) {
        fd = open(uri, O_CREAT | O_WRONLY);
    } else {
        fd = open(uri, O_WRONLY);
    }

    if (fd < 0) {
        res = (errno == EACCES || errno == EISDIR) ? &RESPONSE_FORBIDDEN
                                                   : &RESPONSE_INTERNAL_SERVER_ERROR;
        goto out;
    }
    flock(fd, LOCK_EX);
    pthread_mutex_unlock(&mutex);
    ftruncate(fd, 0);

    res = conn_recv_file(conn, fd);
    if (res == NULL) {
        res = existed ? &RESPONSE_OK : &RESPONSE_CREATED;
    }

    conn_send_response(conn, res);
    auditlog(conn, res);

    flock(fd, LOCK_UN);
    close(fd);
    return;

out:
    pthread_mutex_unlock(&mutex);
    auditlog(conn, res);
    conn_send_response(conn, res);
    close(fd);
}

void auditlog(conn_t *conn, const Response_t *res) {
    const Request_t *req = conn_get_request(conn);
    char *header = conn_get_header(conn, "Request-Id");
    int id = header ? atoi(header) : 0;
    const char *method = request_get_str(req);

    fprintf(stderr, "%s,%s,%d,%d\n", method, conn_get_uri(conn), response_get_code(res), id);
}
